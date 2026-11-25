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

type TransitionFriendsResult struct {
	Channel      *channel.Channel
	Member       *channel.Member
	PeerUser     *user.User
	PeerPresence gateway.Presence
}

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

func (s *Service) TransitionPending(ctx context.Context, userID uuid.UUID, username string) (*TransitionFriendsResult, error) {
	chID, err := uuid.NewV7()
	if err != nil {
		return nil, ErrIDGenerateFailed()
	}

	outgoingPendingsCount, err := s.repo.CountOutgoingPendingsByUserID(ctx, userID)
	if err != nil {
		return nil, err
	}
	if outgoingPendingsCount >= MaxTypeLimit {
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
	if userID == peerID {
		return nil, ErrCannotRequestSelf()
	}

	u1, u2 := sortIDPair(userID, peerID)
	var savedRel *Relation
	var createdCh *createdDirectChannel

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
				if rel.IsActor(userID) {
					return ErrAlreadyOutgoingBlock()
				}
				return ErrUsernameNotFound()
			case TypePending:
				if rel.IsActor(userID) {
					return ErrAlreadyPending()
				}

				userFriendsCount, err := s.repo.CountFriendsByUserID(txCtx, userID)
				if err != nil {
					return err
				}
				if userFriendsCount >= MaxTypeLimit {
					return ErrMaxFriendsReached()
				}

				peerFriends, err := s.repo.CountFriendsByUserID(txCtx, peerID)
				if err != nil {
					return err
				}
				if peerFriends >= MaxTypeLimit {
					return ErrPeerMaxFriendsReached()
				}

				acceptedRel, created, err := s.acceptPendingRelation(txCtx, rel, chID, userID, peerID, now)
				if err != nil {
					return err
				}

				savedRel = acceptedRel
				createdCh = created
				return nil
			}
			return nil
		}

		peerIncomingPendingsCount, err := s.repo.CountIncomingPendingsByUserID(txCtx, peerID)
		if err != nil {
			return err
		}
		if peerIncomingPendingsCount >= MaxTypeLimit {
			return ErrPeerMaxIncomingFriendRequests()
		}

		pendingRel := NewPending(u1, u2, userID, now)
		savedRel, err = s.repo.Save(txCtx, pendingRel)
		return err
	})
	if err != nil {
		return nil, err
	}

	if savedRel == nil {
		return nil, ErrSendingFriendRequest()
	}

	switch savedRel.Type {
	case TypePending:
		s.invalidateCache(ctx, "new_pending", userID, peerID, s.cache.DeletePendingPair)

		actor, err := s.cachedUserRepo.Get(ctx, userID)
		if err != nil {
			slog.WarnContext(ctx, "failed to load actor for pending relation broadcast",
				slog.String("user_id", userID.String()),
				slog.String("peer_id", peerID.String()),
				slog.Any("error", err),
			)
			return &TransitionFriendsResult{PeerUser: peer}, nil
		}

		s.broadcastToUser(
			ctx,
			userID,
			MessageFriendRequestSent,
			newFriendRequestSentPayload(peer),
			"failed to broadcast new pending relation",
			userID,
			peerID,
		)
		s.broadcastToUser(
			ctx,
			peerID,
			MessageFriendRequestSent,
			newFriendRequestSentPayload(actor),
			"failed to broadcast new pending relation",
			userID,
			peerID,
		)
		return &TransitionFriendsResult{PeerUser: peer}, nil

	case TypeFriends:
		var ch *channel.Channel
		var actorMember, peerMember *channel.Member
		if createdCh != nil {
			ch = createdCh.channel
			actorMember = createdCh.actorMember
			peerMember = createdCh.peerMember
		}

		peerUser, peerPresence := s.onFriendAdded(ctx, userID, peerID, ch, actorMember, peerMember)

		return &TransitionFriendsResult{
			Channel:      ch,
			Member:       actorMember,
			PeerUser:     peerUser,
			PeerPresence: peerPresence,
		}, nil
	}

	return nil, nil
}

func (s *Service) TransitionFriends(ctx context.Context, userID, peerID uuid.UUID) (*TransitionFriendsResult, error) {
	if userID == peerID {
		return nil, ErrCannotRequestSelf()
	}

	u1, u2 := sortIDPair(userID, peerID)
	var createdCh *createdDirectChannel

	chID, err := uuid.NewV7()
	if err != nil {
		return nil, ErrIDGenerateFailed()
	}

	now := time.Now()

	err = s.tx.ExecTx(ctx, func(txCtx context.Context) error {
		userFriendsCount, err := s.repo.CountFriendsByUserID(txCtx, userID)
		if err != nil {
			return err
		}
		if userFriendsCount >= MaxTypeLimit {
			return ErrMaxFriendsReached()
		}

		rel, err := s.repo.GetForUpdate(txCtx, u1, u2)
		if err != nil {
			return err
		}
		if rel == nil {
			return ErrNotPending()
		}

		if !rel.IsPending() {
			return ErrNotPending()
		}

		if rel.IsActor(userID) {
			return ErrPendingActor()
		}

		ok, err := s.canAddFriendWithPeer(txCtx, peerID)
		if err != nil {
			return err
		}
		if !ok {
			return ErrCannotAcceptFriendRequest()
		}

		_, created, err := s.acceptPendingRelation(txCtx, rel, chID, userID, peerID, now)
		if err != nil {
			return err
		}

		createdCh = created
		return nil
	})
	if err != nil {
		return nil, err
	}

	var ch *channel.Channel
	var actorMember, peerMember *channel.Member
	if createdCh != nil {
		ch = createdCh.channel
		actorMember = createdCh.actorMember
		peerMember = createdCh.peerMember
	}

	peerUser, peerPresence := s.onFriendAdded(ctx, userID, peerID, ch, actorMember, peerMember)

	return &TransitionFriendsResult{
		Channel:      ch,
		Member:       actorMember,
		PeerUser:     peerUser,
		PeerPresence: peerPresence,
	}, nil
}

func (s *Service) TransitionBlocked(ctx context.Context, userID, peerID uuid.UUID) error {
	if userID == peerID {
		return ErrCannotRequestSelf()
	}

	u1, u2 := sortIDPair(userID, peerID)
	now := time.Now()

	var prevType Type
	var hadPrevRel bool
	var prevActorID uuid.UUID

	err := s.tx.ExecTx(ctx, func(txCtx context.Context) error {
		userBlockedCount, err := s.repo.CountOutgoingBlocksByUserID(txCtx, userID)
		if err != nil {
			return err
		}
		if userBlockedCount >= MaxTypeLimit {
			return ErrMaxBlocksReached()
		}

		rel, err := s.repo.GetForUpdate(txCtx, u1, u2)
		if err != nil && !errs.IsNotFound(err) {
			return err
		}

		if rel != nil {
			if rel.Type.IsBlocked() {
				if rel.IsActor(userID) {
					return ErrAlreadyOutgoingBlock()
				}
				return nil
			}

			prevType = rel.Type
			prevActorID = rel.ActorID
			hadPrevRel = true
			rel.Block(userID, now)
		} else {
			rel = NewBlocked(u1, u2, userID, now)
		}

		_, err = s.repo.Save(txCtx, rel)
		return err
	})
	if err != nil {
		return err
	}

	if !hadPrevRel {
		s.invalidateCache(ctx, "blocks", userID, peerID, s.cache.DeleteBlocksPair)
	} else {
		switch prevType {
		case TypePending:
			senderID, recipientID := resolvePendingPair(prevActorID, userID, peerID)
			s.invalidateCache(ctx, "pending_blocks", senderID, recipientID, s.cache.DeletePendingBlocksPair)
		case TypeFriends:
			s.invalidateCache(ctx, "friend_blocks", userID, peerID, s.cache.DeleteFriendBlocksPair)
		default:
			s.invalidateCache(ctx, "blocks", userID, peerID, s.cache.DeleteBlocksPair)
		}
	}

	s.broadcastToUser(
		ctx,
		userID,
		MessageUserBlocked,
		newUserIDPayload(peerID),
		"failed to broadcast user blocked to actor",
		userID,
		peerID,
	)

	if hadPrevRel {
		switch prevType {
		case TypePending:
			if prevActorID == userID {
				s.broadcastToUser(
					ctx,
					peerID,
					MessageFriendRequestDeleted,
					newUserIDPayload(userID),
					"failed to broadcast friend request deleted to peer",
					userID,
					peerID,
				)
			}
		case TypeFriends:
			s.broadcastToUser(
				ctx,
				peerID,
				MessageFriendDeleted,
				newUserIDPayload(userID),
				"failed to broadcast friend deleted to peer",
				userID,
				peerID,
			)
		}
	}

	return nil
}

func (s *Service) DeleteByUserID(ctx context.Context, userID, peerID uuid.UUID) error {
	if userID == peerID {
		return ErrCannotRequestSelf()
	}

	u1, u2 := sortIDPair(userID, peerID)
	var deletedRel *Relation

	err := s.tx.ExecTx(ctx, func(txCtx context.Context) error {
		rel, err := s.repo.GetForUpdate(txCtx, u1, u2)
		if err != nil {
			if errs.IsNotFound(err) {
				return nil
			}
			return err
		}

		if rel.IsBlockedByPeer(userID) {
			return nil
		}

		if err := s.repo.DeleteByUserID(txCtx, u1, u2, userID); err != nil {
			return err
		}

		deletedRel = rel
		return nil
	})
	if err != nil {
		return err
	}

	if deletedRel == nil {
		return nil
	}

	switch deletedRel.Type {
	case TypePending:
		senderID, recipientID := resolvePendingPair(deletedRel.ActorID, userID, peerID)
		s.invalidateCache(ctx, "delete_pending", senderID, recipientID, s.cache.DeletePendingPair)
	case TypeFriends:
		s.invalidateCache(ctx, "delete_friends", userID, peerID, s.cache.DeleteFriendsPair)
	case TypeBlocked:
		s.invalidateCache(ctx, "delete_blocks", userID, peerID, s.cache.DeleteBlocksPair)
	}

	switch deletedRel.Type {
	case TypePending:
		s.broadcastToUser(
			ctx,
			peerID,
			MessageFriendRequestDeleted,
			newUserIDPayload(userID),
			"failed to broadcast friend request deleted to peer",
			userID,
			peerID,
		)
		s.broadcastToUser(
			ctx,
			userID,
			MessageFriendRequestDeleted,
			newUserIDPayload(peerID),
			"failed to broadcast friend request deleted to actor",
			userID,
			peerID,
		)
	case TypeFriends:
		s.broadcastToUser(
			ctx,
			peerID,
			MessageFriendDeleted,
			newUserIDPayload(userID),
			"failed to broadcast friend deleted to peer",
			userID,
			peerID,
		)
		s.broadcastToUser(
			ctx,
			userID,
			MessageFriendDeleted,
			newUserIDPayload(peerID),
			"failed to broadcast friend deleted to actor",
			userID,
			peerID,
		)
	}

	return nil
}

func (s *Service) acceptPendingRelation(
	txCtx context.Context,
	rel *Relation,
	chID, actorID, peerID uuid.UUID,
	now time.Time,
) (*Relation, *createdDirectChannel, error) {
	created, err := s.createDirectChannelWithMembers(txCtx, chID, actorID, peerID, now)
	if err != nil {
		return nil, nil, err
	}

	rel.Accept(actorID, created.channel.ID, now)
	savedRel, err := s.repo.Save(txCtx, rel)
	if err != nil {
		return nil, nil, err
	}

	return savedRel, created, nil
}

func (s *Service) createDirectChannelWithMembers(
	txCtx context.Context,
	chID, actorID, peerID uuid.UUID,
	now time.Time,
) (*createdDirectChannel, error) {
	ch := channel.NewDirect(chID, now)
	actorMember := channel.NewCreatorMember(chID, actorID, now)
	peerMember := channel.NewPeerMember(chID, peerID, now)

	if _, err := s.channelRepo.Create(txCtx, ch); err != nil {
		return nil, err
	}

	if _, err := s.memberRepo.CreateBatch(txCtx, []*channel.Member{actorMember, peerMember}); err != nil {
		return nil, err
	}

	return &createdDirectChannel{
		channel:     ch,
		actorMember: actorMember,
		peerMember:  peerMember,
	}, nil
}

func (s *Service) onFriendAdded(
	ctx context.Context,
	userID, peerID uuid.UUID,
	ch *channel.Channel,
	actorMember, peerMember *channel.Member,
) (*user.User, gateway.Presence) {
	s.handleNewFriendsCache(ctx, userID, peerID, ch)

	ids := []uuid.UUID{userID, peerID}
	users, presences := s.loadUsersAndPresences(ctx, ids, userID, peerID)

	actorUser := users[userID]
	actorPresence := resolvePresence(presences, userID)
	s.broadcastFriendAdded(ctx, peerID, actorUser, actorPresence, ch, peerMember, userID, peerID, "peer")

	peerUser := users[peerID]
	peerPresence := resolvePresence(presences, peerID)
	s.broadcastFriendAdded(ctx, userID, peerUser, peerPresence, ch, actorMember, userID, peerID, "actor")

	return peerUser, peerPresence
}

func (s *Service) canAddFriendWithPeer(txCtx context.Context, peerID uuid.UUID) (bool, error) {
	peer, err := s.userRepo.Get(txCtx, peerID)
	if err != nil {
		if errs.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if peer.IsDisabled() {
		return false, nil
	}

	peerFriends, err := s.repo.CountFriendsByUserID(txCtx, peerID)
	if err != nil {
		return false, err
	}
	if peerFriends >= MaxTypeLimit {
		return false, nil
	}

	return true, nil
}

func (s *Service) loadUsersAndPresences(
	ctx context.Context,
	ids []uuid.UUID,
	logUserID, logPeerID uuid.UUID,
) (map[uuid.UUID]*user.User, map[uuid.UUID]gateway.Presence) {
	users, usersErr := s.cachedUserRepo.GetBatch(ctx, ids)
	if usersErr != nil {
		slog.WarnContext(ctx, "failed to load users for new friends broadcast",
			slog.String("user_id", logUserID.String()),
			slog.String("peer_id", logPeerID.String()),
			slog.Any("error", usersErr),
		)
	}

	presences, presencesErr := s.gatewayCache.GetBatchPresence(ctx, ids)
	if presencesErr != nil {
		slog.WarnContext(ctx, "failed to load presences for new friends broadcast",
			slog.String("user_id", logUserID.String()),
			slog.String("peer_id", logPeerID.String()),
			slog.Any("error", presencesErr),
		)
	}

	return users, presences
}

func (s *Service) broadcastFriendAdded(
	ctx context.Context,
	recipientID uuid.UUID,
	friendUser *user.User,
	friendPresence gateway.Presence,
	ch *channel.Channel,
	member *channel.Member,
	logUserID, logPeerID uuid.UUID,
	targetLabel string,
) {
	if friendUser == nil {
		return
	}

	s.broadcastToUser(
		ctx,
		recipientID,
		MessageFriendAdded,
		newFriendAddedPayload(friendUser, friendPresence, ch, member),
		"failed to broadcast new friends relation to "+targetLabel,
		logUserID,
		logPeerID,
	)
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
			slog.String("user_id", logUserID.String()),
			slog.String("peer_id", logPeerID.String()),
			slog.Any("error", err),
		)
	}
}

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
		slog.WarnContext(cacheCtx, "failed to invalidate cache pair",
			slog.String("cache_type", kind),
			slog.String("actor_id", actorID.String()),
			slog.String("peer_id", peerID.String()),
			slog.Any("error", err),
		)
	}
}

func (s *Service) handleNewFriendsCache(
	ctx context.Context,
	userID, peerID uuid.UUID,
	ch *channel.Channel,
) {
	if s.cache == nil {
		return
	}

	cacheCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), broadcastTimeout)
	defer cancel()

	if err := s.cache.HandleNewFriends(cacheCtx, userID, peerID, ch); err != nil {
		slog.WarnContext(cacheCtx, "failed to update cache for new friends",
			slog.String("user_id", userID.String()),
			slog.String("peer_id", peerID.String()),
			slog.Any("error", err),
		)
	}
}

type createdDirectChannel struct {
	channel     *channel.Channel
	actorMember *channel.Member
	peerMember  *channel.Member
}

type pairDeleteFunc func(ctx context.Context, actorID, peerID uuid.UUID) error

func resolvePendingPair(relActorID, userID, peerID uuid.UUID) (senderID, recipientID uuid.UUID) {
	if relActorID == peerID {
		return peerID, userID
	}
	return userID, peerID
}

func resolvePresence(presences map[uuid.UUID]gateway.Presence, id uuid.UUID) gateway.Presence {
	if p, ok := presences[id]; ok && p != gateway.PresenceUnknown {
		return p
	}
	return gateway.PresenceOffline
}
