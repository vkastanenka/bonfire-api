package relation

import (
	"bonfire-api/internal/channel"
	"bonfire-api/internal/gateway"
	"bonfire-api/internal/pkg/errs"
	"bonfire-api/internal/user"
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

const broadcastTimeout = 2 * time.Second

type Service struct {
	cache          *Cache
	repo           *Repository
	broadcaster    Broadcaster
	gatewayCache   GatewayCache
	channelRepo    ChannelRepository
	memberRepo     MemberRepository
	userRepo       UserRepository
	cachedUserRepo CachedUserRepository
	tx             TX
}

func NewService(
	cache *Cache,
	repo *Repository,
	broadcaster Broadcaster,
	gatewayCache GatewayCache,
	channelRepo ChannelRepository,
	memberRepo MemberRepository,
	userRepo UserRepository,
	cachedUserRepo CachedUserRepository,
	tx TX,
) *Service {
	return &Service{
		cache:          cache,
		repo:           repo,
		broadcaster:    broadcaster,
		gatewayCache:   gatewayCache,
		channelRepo:    channelRepo,
		memberRepo:     memberRepo,
		userRepo:       userRepo,
		cachedUserRepo: cachedUserRepo,
		tx:             tx,
	}
}

type TransitionResult struct {
	Channel      *channel.Channel
	Membership   *channel.Member
	PeerUser     *user.User
	PeerPresence *gateway.Presence
}

func (s *Service) TransitionPending(ctx context.Context, actorID uuid.UUID, username string) (*TransitionResult, error) {
	chID, err := uuid.NewV7()
	if err != nil {
		return nil, ErrIDGenerateFailed()
	}

	outgoingPendingsCount, err := s.repo.CountOutgoingPendingsByUserID(ctx, actorID)
	if err != nil {
		return nil, err
	}
	if countAtLimit(outgoingPendingsCount) {
		return nil, ErrMaxOutgoingPendingRequestsReached()
	}

	peer, err := s.userRepo.GetByUsername(ctx, username)
	if err != nil {
		if errs.IsNotFound(err) {
			return nil, ErrUsernameNotFound()
		}
		return nil, err
	}
	if peer.IsDisabled() {
		return nil, ErrUsernameNotFound()
	}

	peerID := peer.ID
	if actorID == peerID {
		return nil, ErrCannotRequestSelf()
	}

	u1, u2 := sortIDPair(actorID, peerID)
	var savedRel *Relation
	var newCh *channel.Channel
	var actorMem *channel.Member
	var peerMem *channel.Member

	now := time.Now()

	err = s.tx.ExecTx(ctx, func(txCtx context.Context) error {
		rel, err := s.repo.GetForUpdate(txCtx, u1, u2)
		if err != nil && !errs.IsNotFound(err) {
			return err
		}

		if rel != nil {
			switch rel.Type {
			case TypeFriends:
				return ErrAlreadyFriends()
			case TypeBlocked:
				if rel.IsActor(actorID) {
					return ErrAlreadyOutgoingBlock()
				}
				return ErrUsernameNotFound()
			case TypePending:
				if rel.IsActor(actorID) {
					return ErrAlreadyPending()
				}

				actorFriendsCount, err := s.repo.CountFriendsByUserID(txCtx, actorID)
				if err != nil {
					return err
				}
				if countAtLimit(actorFriendsCount) {
					return ErrMaxFriendsReached()
				}

				peerFriendsCount, err := s.repo.CountFriendsByUserID(txCtx, peerID)
				if err != nil {
					return err
				}
				if countAtLimit(peerFriendsCount) {
					return ErrPeerMaxFriendsReached()
				}

				savedRel, newCh, actorMem, peerMem, err = s.acceptPendingRelation(txCtx, rel, chID, actorID, peerID, now)
				return err
			}
			return ErrUnknownRelationFound()
		}

		peerIncomingPendingsCount, err := s.repo.CountIncomingPendingsByUserID(txCtx, peerID)
		if err != nil {
			return err
		}
		if countAtLimit(peerIncomingPendingsCount) {
			return ErrPeerMaxIncomingFriendRequests()
		}

		savedRel, err = s.repo.Save(txCtx, NewPending(u1, u2, actorID, now))
		return err
	})
	if err != nil {
		return nil, err
	}

	actor, err := s.cachedUserRepo.Get(ctx, actorID)
	if err != nil {
		slog.WarnContext(ctx, "failed to load actor user in transition pending request",
			slog.String("actor_id", actorID.String()),
			slog.Any("error", err),
		)
	}

	result := &TransitionResult{PeerUser: peer}

	if savedRel.IsFriends() {
		peerPresence := s.onFriendAdded(ctx, actor, peer, newCh, actorMem, peerMem)
		result.Channel = newCh
		result.Membership = actorMem
		result.PeerPresence = &peerPresence
		return result, nil
	}

	s.invalidateCache(ctx, "new_pending", actorID, peerID, s.cache.DeletePendingPair)

	s.broadcastToUser(
		ctx,
		actorID,
		MessageFriendRequestSent,
		newFriendRequestSentPayload(peer),
		"failed to broadcast new pending relation",
		actorID,
		peerID,
	)

	if actor != nil {
		s.broadcastToUser(
			ctx,
			peerID,
			MessageFriendRequestSent,
			newFriendRequestSentPayload(actor),
			"failed to broadcast new pending relation",
			actorID,
			peerID,
		)
	}

	return result, nil
}

func (s *Service) TransitionFriends(ctx context.Context, actorID, peerID uuid.UUID) (*TransitionResult, error) {
	if actorID == peerID {
		return nil, ErrCannotRequestSelf()
	}

	actorFriendsCount, err := s.repo.CountFriendsByUserID(ctx, actorID)
	if err != nil {
		return nil, err
	}
	if countAtLimit(actorFriendsCount) {
		return nil, ErrMaxFriendsReached()
	}

	peer, err := s.cachedUserRepo.Get(ctx, peerID)
	if err != nil {
		if errs.IsNotFound(err) {
			return nil, ErrUserNotFound()
		}
		return nil, err
	}
	if peer.IsDisabled() {
		return nil, ErrUserNotFound()
	}

	peerFriendsCount, err := s.repo.CountFriendsByUserID(ctx, peerID)
	if err != nil {
		return nil, err
	}
	if countAtLimit(peerFriendsCount) {
		return nil, ErrPeerMaxFriendsReached()
	}

	chID, err := uuid.NewV7()
	if err != nil {
		return nil, ErrIDGenerateFailed()
	}

	u1, u2 := sortIDPair(actorID, peerID)
	var newCh *channel.Channel
	var actorMem *channel.Member
	var peerMem *channel.Member

	now := time.Now()

	err = s.tx.ExecTx(ctx, func(txCtx context.Context) error {
		rel, err := s.repo.GetForUpdate(txCtx, u1, u2)
		if err != nil {
			return err
		}

		if !rel.IsPending() {
			return ErrNotPending()
		}

		if rel.IsActor(actorID) {
			return ErrPendingActor()
		}

		_, newCh, actorMem, peerMem, err = s.acceptPendingRelation(txCtx, rel, chID, actorID, peerID, now)
		return err
	})
	if err != nil {
		return nil, err
	}

	actor, err := s.cachedUserRepo.Get(ctx, actorID)
	if err != nil {
		slog.WarnContext(ctx, "failed to load actor user in transition friends request",
			slog.String("actor_id", actorID.String()),
			slog.Any("error", err),
		)
	}

	peerPresence := s.onFriendAdded(ctx, actor, peer, newCh, actorMem, peerMem)

	return &TransitionResult{
		Channel:      newCh,
		Membership:   actorMem,
		PeerUser:     peer,
		PeerPresence: &peerPresence,
	}, nil
}

func (s *Service) TransitionBlocked(ctx context.Context, actorID, peerID uuid.UUID) error {
	if actorID == peerID {
		return ErrCannotRequestSelf()
	}

	outgoingBlocksCount, err := s.repo.CountOutgoingBlocksByUserID(ctx, actorID)
	if err != nil {
		return err
	}
	if countAtLimit(outgoingBlocksCount) {
		return ErrMaxBlocksReached()
	}

	peer, err := s.cachedUserRepo.Get(ctx, peerID)
	if err != nil {
		if errs.IsNotFound(err) {
			return ErrUserNotFound()
		}
		return err
	}
	if peer.IsDisabled() {
		return ErrUserNotFound()
	}

	u1, u2 := sortIDPair(actorID, peerID)
	prevType := TypeUnknown
	var prevActorID uuid.UUID

	now := time.Now()

	err = s.tx.ExecTx(ctx, func(txCtx context.Context) error {
		rel, err := s.repo.GetForUpdate(txCtx, u1, u2)
		if err != nil && !errs.IsNotFound(err) {
			return err
		}

		if rel != nil {
			if rel.Type.IsBlocked() {
				if rel.IsActor(actorID) {
					return ErrAlreadyOutgoingBlock()
				}
				return ErrUserNotFound()
			}

			prevType = rel.Type
			prevActorID = rel.ActorID
			rel.Block(actorID, now)
		} else {
			rel = NewBlocked(u1, u2, actorID, now)
		}

		_, err = s.repo.Save(txCtx, rel)
		return err
	})
	if err != nil {
		return err
	}

	if prevType == TypeUnknown {
		s.invalidateCache(ctx, "blocks", actorID, peerID, s.cache.DeleteBlocksPair)
	} else {
		switch prevType {
		case TypePending:
			senderID, recipientID := resolvePendingPair(prevActorID, actorID, peerID)
			s.invalidateCache(ctx, "pending_blocks", senderID, recipientID, s.cache.DeletePendingBlocksPair)
		case TypeFriends:
			s.invalidateCache(ctx, "friend_blocks", actorID, peerID, s.cache.DeleteFriendBlocksPair)
		}
	}

	s.broadcastToUser(
		ctx,
		actorID,
		MessageUserBlocked,
		newPeerIDPayload(peerID),
		"failed to broadcast user blocked to actor",
		actorID,
		peerID,
	)

	switch prevType {
	case TypePending:
		s.broadcastToUser(
			ctx,
			peerID,
			MessageFriendRequestDeleted,
			newPeerIDPayload(actorID),
			"failed to broadcast friend request deleted to peer",
			actorID,
			peerID,
		)
	case TypeFriends:
		s.broadcastToUser(
			ctx,
			peerID,
			MessageFriendDeleted,
			newPeerIDPayload(actorID),
			"failed to broadcast friend deleted to peer",
			actorID,
			peerID,
		)
	}

	return nil
}

func (s *Service) DeleteByUserID(ctx context.Context, actorID, peerID uuid.UUID) error {
	if actorID == peerID {
		return ErrCannotRequestSelf()
	}

	u1, u2 := sortIDPair(actorID, peerID)
	var deletedRel *Relation

	err := s.tx.ExecTx(ctx, func(txCtx context.Context) error {
		rel, err := s.repo.GetForUpdate(txCtx, u1, u2)
		if err != nil {
			return err
		}

		if rel.IsBlockedByPeer(actorID) {
			return ErrUserNotFound()
		}

		if err := s.repo.DeleteByUserID(txCtx, u1, u2, actorID); err != nil {
			return err
		}

		deletedRel = rel
		return nil
	})
	if err != nil {
		return err
	}

	switch deletedRel.Type {
	case TypePending:
		senderID, recipientID := resolvePendingPair(deletedRel.ActorID, actorID, peerID)
		s.invalidateCache(ctx, "delete_pending", senderID, recipientID, s.cache.DeletePendingPair)
		s.broadcastToUser(
			ctx,
			actorID,
			MessageFriendRequestDeleted,
			newPeerIDPayload(peerID),
			"failed to broadcast friend request deleted to actor",
			actorID,
			peerID,
		)
		s.broadcastToUser(
			ctx,
			peerID,
			MessageFriendRequestDeleted,
			newPeerIDPayload(actorID),
			"failed to broadcast friend request deleted to peer",
			actorID,
			peerID,
		)
	case TypeFriends:
		s.invalidateCache(ctx, "delete_friends", actorID, peerID, s.cache.DeleteFriendsPair)
		s.broadcastToUser(
			ctx,
			actorID,
			MessageFriendDeleted,
			newPeerIDPayload(peerID),
			"failed to broadcast friend deleted to actor",
			actorID,
			peerID,
		)
		s.broadcastToUser(
			ctx,
			peerID,
			MessageFriendDeleted,
			newPeerIDPayload(actorID),
			"failed to broadcast friend deleted to peer",
			actorID,
			peerID,
		)
	case TypeBlocked:
		s.invalidateCache(ctx, "delete_blocks", actorID, peerID, s.cache.DeleteBlocksPair)
		s.broadcastToUser(
			ctx,
			actorID,
			MessageBlockDeleted,
			newPeerIDPayload(peerID),
			"failed to broadcast block deleted to actor",
			actorID,
			peerID,
		)
	}

	return nil
}

func (s *Service) acceptPendingRelation(
	ctx context.Context,
	rel *Relation,
	chID, actorID, peerID uuid.UUID,
	now time.Time,
) (*Relation, *channel.Channel, *channel.Member, *channel.Member, error) {
	if rel == nil {
		return nil, nil, nil, nil, ErrSendingFriendRequest()
	}

	var err error
	ch := channel.NewDirect(chID, now)
	actorMem := channel.NewCreatorMember(chID, actorID, now)
	peerMem := channel.NewPeerMember(chID, peerID, now)

	if ch, err = s.channelRepo.Create(ctx, ch); err != nil {
		return nil, nil, nil, nil, err
	}

	if _, err := s.memberRepo.CreateBatch(ctx, []*channel.Member{actorMem, peerMem}); err != nil {
		return nil, nil, nil, nil, err
	}

	rel.Accept(actorID, ch.ID, now)

	rel, err = s.repo.Save(ctx, rel)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return rel, ch, actorMem, peerMem, nil
}

func (s *Service) onFriendAdded(
	ctx context.Context,
	actor, peer *user.User,
	ch *channel.Channel,
	actorMem, peerMem *channel.Member,
) gateway.Presence {
	if peer == nil || ch == nil || actorMem == nil || peerMem == nil {
		return gateway.PresenceOffline
	}

	if s.cache != nil {
		cacheCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), broadcastTimeout)
		defer cancel()

		if err := s.cache.HandleNewFriends(cacheCtx, actorMem.UserID, peer.ID, ch); err != nil {
			slog.WarnContext(cacheCtx, "failed to handle relation cache invalidation",
				slog.String("cache_type", "new_friends"),
				slog.String("actor_id", actorMem.UserID.String()),
				slog.String("peer_id", peer.ID.String()),
				slog.String("channel_id", ch.ID.String()),
				slog.Any("error", err),
			)
		}
	}

	var presences map[uuid.UUID]gateway.Presence
	if s.gatewayCache != nil {
		presenceCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), broadcastTimeout)
		defer cancel()

		var err error
		presences, err = s.gatewayCache.GetBatchPresence(presenceCtx, []uuid.UUID{actorMem.UserID, peer.ID})
		if err != nil {
			slog.WarnContext(presenceCtx, "failed to load presences for new friends broadcast",
				slog.String("actor_id", actorMem.UserID.String()),
				slog.String("peer_id", peer.ID.String()),
				slog.Any("error", err),
			)
		}
	}

	peerPresence := resolvePresence(presences, peer.ID)
	s.broadcastToUser(
		ctx,
		actorMem.UserID,
		MessageFriendAdded,
		newFriendAddedPayload(ch, actorMem, peer, peerPresence),
		"failed to broadcast new friends relation to actor",
		actorMem.UserID,
		peer.ID,
	)

	if actor != nil {
		actorPresence := resolvePresence(presences, actor.ID)
		s.broadcastToUser(
			ctx,
			peer.ID,
			MessageFriendAdded,
			newFriendAddedPayload(ch, peerMem, actor, actorPresence),
			"failed to broadcast new friends relation to peer",
			actor.ID,
			peer.ID,
		)
	}

	return peerPresence
}

type pairDeleteFunc func(ctx context.Context, actorID, peerID uuid.UUID) error

func (s *Service) invalidateCache(
	ctx context.Context,
	kind string,
	actorID, peerID uuid.UUID,
	deleteFn pairDeleteFunc,
) {
	if deleteFn == nil || s.cache == nil {
		return
	}

	cacheCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), broadcastTimeout)
	defer cancel()

	if err := deleteFn(cacheCtx, actorID, peerID); err != nil {
		slog.WarnContext(cacheCtx, "failed to invalidate relation cache",
			slog.String("cache_type", kind),
			slog.String("actor_id", actorID.String()),
			slog.String("peer_id", peerID.String()),
			slog.Any("error", err),
		)
	}
}

func (s *Service) broadcastToUser(
	ctx context.Context,
	recipientID uuid.UUID,
	event string,
	payload any,
	warnMsg string,
	logUserID, logPeerID uuid.UUID,
) {
	if s.broadcaster == nil {
		return
	}

	bCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), broadcastTimeout)
	defer cancel()

	if err := s.broadcaster.ToUser(bCtx, recipientID, event, payload); err != nil {
		slog.WarnContext(bCtx, warnMsg,
			slog.String("actor_id", logUserID.String()),
			slog.String("peer_id", logPeerID.String()),
			slog.Any("error", err),
		)
	}
}

func resolvePresence(presences map[uuid.UUID]gateway.Presence, id uuid.UUID) gateway.Presence {
	if p, ok := presences[id]; ok && p != gateway.PresenceUnknown {
		return p
	}
	return gateway.PresenceOffline
}

func resolvePendingPair(prevActorID, actorID, peerID uuid.UUID) (senderID, recipientID uuid.UUID) {
	if prevActorID == peerID {
		return peerID, actorID
	}
	return actorID, peerID
}
