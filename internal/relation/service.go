package relation

import (
	"bonfire-api/internal/gateway"
	"bonfire-api/internal/pkg/errs"
	"bonfire-api/internal/user"
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

type Service struct {
	cache *Cache
	repo  *Repository
	// cachedRepo        CachedRepository
	broadcaster  Broadcaster
	gatewayCache GatewayCache
	// channelRepo       ChannelRepository
	// cachedChannelRepo CachedChannelRepository
	// memberRepo        MemberRepository
	// cachedMemberRepo  CachedMemberRepository
	// presenceCache     PresenceCache
	// userCache         UserCache
	userRepo       UserRepository
	cachedUserRepo CachedUserRepository
	tx             TX
}

func NewService(
	cache *Cache,
	repo *Repository,
	// cachedRepo CachedRepository,
	broadcaster Broadcaster,
	gatewayCache GatewayCache,
	// channelRepo ChannelRepository,
	// cachedChannelRepo CachedChannelRepository,
	// memberRepo MemberRepository,
	// cachedMemberRepo CachedMemberRepository,
	userRepo UserRepository,
	cachedUserRepo CachedUserRepository,
	tx TX,
) *Service {
	return &Service{
		cache:        cache,
		repo:         repo,
		broadcaster:  broadcaster,
		gatewayCache: gatewayCache,
		// cachedRepo:        cachedRepo,
		// channelRepo:       channelRepo,
		// cachedChannelRepo: cachedChannelRepo,
		// memberRepo:        memberRepo,
		// cachedMemberRepo:  cachedMemberRepo,
		// presenceCache:     presenceCache,
		// userCache:         userCache,
		userRepo:       userRepo,
		cachedUserRepo: cachedUserRepo,
		tx:             tx,
	}
}

func (s *Service) TransitionPending(ctx context.Context, userID uuid.UUID, peerID uuid.UUID) error {
	if userID == peerID {
		return ErrCannotRequestSelf()
	}

	u1, u2 := sortIDPair(userID, peerID)
	var savedRel *Relation

	now := time.Now()

	err := s.tx.ExecTx(ctx, func(txCtx context.Context) error {
		userFriendsCount, err := s.repo.CountFriendsByUserID(txCtx, userID)
		if err != nil {
			return err
		}
		if userFriendsCount >= MaxTypeLimit {
			return ErrMaxFriendsReached()
		}

		outgoingPendingsCount, err := s.repo.CountOutgoingPendingsByUserID(txCtx, userID)
		if err != nil {
			return err
		}
		if outgoingPendingsCount >= MaxTypeLimit {
			return ErrMaxPendingRequestsReached()
		}

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

				return nil
			case TypePending:
				if rel.IsActor(userID) {
					return ErrAlreadyPending()
				}

				ok, err := s.canAddFriendWithPeer(txCtx, peerID)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}

				// TODO: Accept pending -> transition to TypeFriends + assign channelID, then Save
				return nil
			}

			return nil
		}

		ok, err := s.canAddFriendWithPeer(txCtx, peerID)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}

		peerIncomingPendingsCount, err := s.repo.CountIncomingPendingsByUserID(txCtx, peerID)
		if err != nil {
			return err
		}
		if peerIncomingPendingsCount >= MaxTypeLimit {
			return nil
		}

		pendingRel := NewPending(u1, u2, userID, now)

		savedRel, err = s.repo.Save(txCtx, pendingRel)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	if savedRel != nil {
		switch savedRel.Type {
		case TypePending:
			s.invalidateCache(ctx, "new_pending", userID, peerID, s.cache.DeletePendingPair)
		case TypeFriends:
			s.invalidateCache(ctx, "new_friends", peerID, userID, s.cache.DeleteNewFriendsPair)
		}

		actor, err := s.cachedUserRepo.Get(ctx, userID)
		if err != nil {
			slog.WarnContext(ctx, "failed to load actor for pending relation broadcast",
				slog.String("user_id", userID.String()),
				slog.String("peer_id", peerID.String()),
				slog.Any("error", err),
			)
			return nil
		}

		bCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()

		switch savedRel.Type {
		case TypePending:
			if err := s.broadcaster.ToUser(
				bCtx,
				peerID,
				MessageFriendRequestSent,
				newFriendRequestSentPayload(user.NewDTO(actor)),
			); err != nil {
				slog.WarnContext(bCtx, "failed to broadcast new pending relation",
					slog.String("user_id", userID.String()),
					slog.String("peer_id", peerID.String()),
					slog.Any("error", err),
				)
			}
		case TypeFriends:
			// TODO: Broadcast MessageFriendAdded when pending acceptance is wired up
		}
	}

	return nil
}

// Channel      *channel.Channel
// ActorMember  *channel.Member

type TransitionFriendsResult struct {
	PeerUser     *user.User
	PeerPresence gateway.Presence
}

func (s *Service) TransitionFriends(ctx context.Context, userID, peerID uuid.UUID) (*TransitionFriendsResult, error) {
	if userID == peerID {
		return nil, ErrCannotRequestSelf()
	}

	u1, u2 := sortIDPair(userID, peerID)
	now := time.Now()

	err := s.tx.ExecTx(ctx, func(txCtx context.Context) error {
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

		// TODO: Create new channel and channel members, then pass channelID to rel.Accept.
		rel.Accept(userID, uuid.Nil, now)

		_, err = s.repo.Save(txCtx, rel)
		return err
	})
	if err != nil {
		return nil, err
	}

	s.invalidateCache(ctx, "new_friends", peerID, userID, s.cache.DeleteNewFriendsPair)

	ids := []uuid.UUID{userID, peerID}

	users, usersErr := s.cachedUserRepo.GetBatch(ctx, ids)
	if usersErr != nil {
		slog.WarnContext(ctx, "failed to load users for new friends broadcast",
			slog.String("user_id", userID.String()),
			slog.String("peer_id", peerID.String()),
			slog.Any("error", usersErr),
		)
	}

	presences, presencesErr := s.gatewayCache.GetBatchPresence(ctx, ids)
	if presencesErr != nil {
		slog.WarnContext(ctx, "failed to load presences for new friends broadcast",
			slog.String("user_id", userID.String()),
			slog.String("peer_id", peerID.String()),
			slog.Any("error", presencesErr),
		)
	}

	bCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()

	actorUser := users[userID]
	actorPresence := resolvePresence(presences, userID)
	if actorUser != nil {
		if err := s.broadcaster.ToUser(
			bCtx,
			peerID,
			MessageFriendAdded,
			newFriendAddedPayload(user.NewDTO(actorUser), actorPresence),
		); err != nil {
			slog.WarnContext(bCtx, "failed to broadcast new friends relation to peer",
				slog.String("user_id", userID.String()),
				slog.String("peer_id", peerID.String()),
				slog.Any("error", err),
			)
		}
	}

	peerUser := users[peerID]
	peerPresence := resolvePresence(presences, peerID)
	if peerUser != nil {
		if err := s.broadcaster.ToUser(
			bCtx,
			userID,
			MessageFriendAdded,
			newFriendAddedPayload(user.NewDTO(peerUser), peerPresence),
		); err != nil {
			slog.WarnContext(bCtx, "failed to broadcast new friends relation to actor",
				slog.String("user_id", userID.String()),
				slog.String("peer_id", peerID.String()),
				slog.Any("error", err),
			)
		}
	}

	return &TransitionFriendsResult{
		PeerUser:     peerUser,
		PeerPresence: peerPresence,
	}, nil
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

func resolvePresence(presences map[uuid.UUID]gateway.Presence, id uuid.UUID) gateway.Presence {
	if p, ok := presences[id]; ok && p != gateway.PresenceUnknown {
		return p
	}
	return gateway.PresenceOffline
}

// func (s *Service) TransitionBlocked(ctx context.Context, peerID uuid.UUID) error {
// 	claims, err := appctx.GetClaims(ctx)
// 	if err != nil {
// 		return err
// 	}

// 	actorID := claims.UserID
// 	if actorID == peerID {
// 		return ErrCannotRequestSelf()
// 	}

// 	if s.checkBlockedCache(ctx, actorID, peerID) {
// 		return nil
// 	}

// 	u1, u2 := sortIDPair(actorID, peerID)
// 	var prevRel *Relation
// 	now := time.Now()

// 	err = s.tx.ExecTx(ctx, func(txCtx context.Context) error {
// 		relLock, getErr := s.repo.GetForUpdate(txCtx, u1, u2)
// 		if getErr != nil && !errs.IsNotFound(getErr) {
// 			return getErr
// 		}

// 		if err := validateNonBlockedActor(actorID, relLock); err != nil {
// 			return nil
// 		}

// 		if errs.IsNotFound(getErr) {
// 			relLock = NewBlocked(u1, u2, actorID, now)
// 		} else {
// 			if relLock.Type.IsBlocked() {
// 				return nil
// 			}
// 			prevRel = relLock
// 			relLock.Block(actorID, now)
// 		}

// 		if _, err := s.repo.Save(txCtx, relLock); err != nil {
// 			return err
// 		}

// 		if prevRel != nil {
// 			if prevRel.IsFriends() {
// 				payload := EventFriendDeletedPayload{
// 					ActorID: actorID,
// 					PeerID:  peerID,
// 				}

// 				event, err := outbox.New(
// 					actorID,
// 					claims.SessionID,
// 					appctx.GetTraceID(ctx),
// 					[]uuid.UUID{actorID, peerID},
// 					EventFriendDeleted,
// 					payload,
// 					now,
// 				)
// 				if err != nil {
// 					return err
// 				}
// 				if err := s.outboxRepo.Create(txCtx, event); err != nil {
// 					return err
// 				}
// 			} else if prevRel.IsPending() {
// 				payload := EventFriendRequestDeletedPayload{
// 					ActorID: actorID,
// 					PeerID:  peerID,
// 				}

// 				event, err := outbox.New(
// 					actorID,
// 					claims.SessionID,
// 					appctx.GetTraceID(ctx),
// 					[]uuid.UUID{peerID},
// 					EventFriendRequestDeleted,
// 					payload,
// 					now,
// 				)
// 				if err != nil {
// 					return err
// 				}
// 				if err := s.outboxRepo.Create(txCtx, event); err != nil {
// 					return err
// 				}
// 			}
// 		}

// 		return nil
// 	})
// 	if err != nil {
// 		return err
// 	}

// 	s.invalidateCache(ctx, actorID, peerID, &Relation{Type: TypeBlocked})
// 	s.invalidateCache(ctx, actorID, peerID, prevRel)

// 	return nil
// }

// func (s *Service) DeleteByUserID(ctx context.Context, peerID uuid.UUID) error {
// 	claims, err := appctx.GetClaims(ctx)
// 	if err != nil {
// 		return err
// 	}

// 	actorID := claims.UserID
// 	if actorID == peerID {
// 		return ErrCannotRequestSelf()
// 	}

// 	if s.checkBlockedCache(ctx, actorID, peerID) {
// 		return nil
// 	}

// 	u1, u2 := sortIDPair(actorID, peerID)
// 	var deletedRel *Relation

// 	err = s.tx.ExecTx(ctx, func(txCtx context.Context) error {
// 		rel, err := s.repo.GetForUpdate(txCtx, u1, u2)
// 		if err != nil {
// 			if errs.IsNotFound(err) {
// 				return nil
// 			}
// 			return err
// 		}

// 		if err := validateNonBlockedActor(actorID, rel); err != nil {
// 			return nil
// 		}

// 		if err := s.repo.DeleteByUserID(txCtx, u1, u2, actorID); err != nil {
// 			return err
// 		}

// 		deletedRel = rel
// 		now := time.Now()

// 		if rel.IsFriends() {
// 			payload := EventFriendDeletedPayload{
// 				ActorID: actorID,
// 				PeerID:  rel.PeerID(actorID),
// 			}

// 			event, err := outbox.New(
// 				actorID,
// 				claims.SessionID,
// 				appctx.GetTraceID(ctx),
// 				[]uuid.UUID{actorID, peerID},
// 				EventFriendDeleted,
// 				payload,
// 				now,
// 			)
// 			if err != nil {
// 				return err
// 			}
// 			if err := s.outboxRepo.Create(txCtx, event); err != nil {
// 				return err
// 			}
// 		} else if rel.IsPending() {
// 			payload := EventFriendRequestDeletedPayload{
// 				ActorID: actorID,
// 				PeerID:  peerID,
// 			}

// 			event, err := outbox.New(
// 				actorID,
// 				claims.SessionID,
// 				appctx.GetTraceID(ctx),
// 				[]uuid.UUID{peerID},
// 				EventFriendRequestDeleted,
// 				payload,
// 				now,
// 			)
// 			if err != nil {
// 				return err
// 			}
// 			if err := s.outboxRepo.Create(txCtx, event); err != nil {
// 				return err
// 			}
// 		}

// 		return nil
// 	})
// 	if err != nil {
// 		return err
// 	}

// 	s.invalidateCache(ctx, actorID, peerID, deletedRel)

// 	return nil
// }

// func (s *Service) checkBlockedCache(ctx context.Context, actorID, peerID uuid.UUID) bool {
// 	if incomingBlocks, err := s.cache.GetIncomingBlockIDs(ctx, actorID); err == nil {
// 		if helpers.ContainsID(incomingBlocks, peerID) {
// 			return true
// 		}
// 	} else {
// 		slog.WarnContext(ctx, "failed to get incoming blocks from cache",
// 			slog.String("user_id", actorID.String()),
// 			slog.String("peer_id", peerID.String()),
// 			slog.Any("error", err),
// 		)
// 	}

// 	if outgoingBlocks, err := s.cache.GetOutgoingBlockIDs(ctx, actorID); err == nil {
// 		if helpers.ContainsID(outgoingBlocks, peerID) {
// 			return true
// 		}
// 	} else {
// 		slog.WarnContext(ctx, "failed to get outgoing blocks from cache",
// 			slog.String("user_id", actorID.String()),
// 			slog.String("peer_id", peerID.String()),
// 			slog.Any("error", err),
// 		)
// 	}

// 	return false
// }

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

	cacheCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
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

// func (s *Service) TransitionFriends(ctx context.Context, userID, peerID uuid.UUID) (*TransitionFriendsResult, error) {
// 	if userID == peerID {
// 		return nil, ErrCannotRequestSelf()
// 	}

// 	if s.checkBlockedCache(ctx, actorID, peerID) {
// 		return nil, ErrPendingRequestNotFound()
// 	}

// 	if incomingPendings, err := s.cache.GetIncomingPendingIDs(ctx, actorID); err == nil {
// 		if !helpers.ContainsID(incomingPendings, peerID) {
// 			return nil, ErrPendingRequestNotFound()
// 		}
// 	} else {
// 		slog.WarnContext(ctx, "failed to get incoming pendings from cache",
// 			slog.String("user_id", actorID.String()),
// 			slog.String("peer_id", peerID.String()),
// 			slog.Any("error", err),
// 		)
// 	}

// 	actorFriends, err := s.cachedRepo.GetFriendIDs(ctx, actorID)
// 	if err == nil {
// 		if helpers.ContainsID(actorFriends, peerID) {
// 			return nil, ErrAlreadyFriends()
// 		}
// 		if len(actorFriends) >= MaxPeerTypeLimit {
// 			return nil, ErrMaxFriendsReached()
// 		}
// 	}

// 	peerFriends, err := s.cachedRepo.GetFriendIDs(ctx, peerID)
// 	if err == nil && len(peerFriends) >= MaxPeerTypeLimit {
// 		return nil, ErrPendingRequestNotFound()
// 	}

// 	actorUser, err := s.cachedUserRepo.Get(ctx, actorID)
// 	if err != nil {
// 		return nil, err
// 	}

// 	peerUser, err := s.cachedUserRepo.Get(ctx, peerID)
// 	if err != nil {
// 		return nil, err
// 	}

// 	presences, _ := s.presenceCache.GetBatch(ctx, []uuid.UUID{actorID, peerID})
// 	actorPresence := presences[actorID]
// 	peerPresence := presences[peerID]

// 	var createdChannel *channel.Channel
// 	var actorMember *channel.Member
// 	var updatedRel *Relation

// 	u1, u2 := sortIDPair(actorID, peerID)

// 	err = s.tx.ExecTx(ctx, func(txCtx context.Context) error {
// 		rel, err := s.repo.GetForUpdate(txCtx, u1, u2)
// 		if err != nil {
// 			return err
// 		}

// 		if err := validateNonBlockedActor(actorID, rel); err != nil {
// 			return ErrPendingRequestNotFound()
// 		}

// 		if err := validateAccept(actorID, rel); err != nil {
// 			return err
// 		}

// 		now := time.Now()

// 		ch, err := channel.NewDirectChannel(now)
// 		if err != nil {
// 			return err
// 		}

// 		newCh, err := s.channelRepo.Create(txCtx, ch)
// 		if err != nil {
// 			return err
// 		}
// 		createdChannel = newCh

// 		newMembers := channel.NewMembers(newCh.ID, actorID, rel.PeerIDs(actorID), now)
// 		createdMembers, err := s.memberRepo.CreateBatch(txCtx, newMembers)
// 		if err != nil {
// 			return err
// 		}

// 		var peerMember *channel.Member
// 		for _, m := range createdMembers {
// 			if m.UserID == actorID {
// 				actorMember = m
// 			} else if m.UserID == peerID {
// 				peerMember = m
// 			}
// 		}

// 		rel.Accept(actorID, newCh.ID, now)

// 		if _, err := s.repo.Save(txCtx, rel); err != nil {
// 			return err
// 		}
// 		updatedRel = rel

// 		actorPayload := EventFriendAddedPayload{
// 			Friend:         user.ParseView(peerUser),
// 			FriendPresence: peerPresence,
// 			Channel:        newCh,
// 			Member:         actorMember,
// 			CreatedAt:      now,
// 		}

// 		actorEvent, err := outbox.New(
// 			actorID,
// 			claims.SessionID,
// 			appctx.GetTraceID(ctx),
// 			[]uuid.UUID{actorID, peerID},
// 			EventFriendAdded,
// 			actorPayload,
// 			now,
// 		)
// 		if err != nil {
// 			return err
// 		}
// 		if err := s.outboxRepo.Create(txCtx, actorEvent); err != nil {
// 			return err
// 		}

// 		peerPayload := EventFriendAddedPayload{
// 			Friend:         user.ParseView(actorUser),
// 			FriendPresence: actorPresence,
// 			Channel:        newCh,
// 			Member:         peerMember,
// 			CreatedAt:      now,
// 		}

// 		peerEvent, err := outbox.New(
// 			actorID,
// 			claims.SessionID,
// 			appctx.GetTraceID(ctx),
// 			[]uuid.UUID{actorID, peerID},
// 			EventFriendAdded,
// 			peerPayload,
// 			now,
// 		)
// 		if err != nil {
// 			return err
// 		}

// 		return s.outboxRepo.Create(txCtx, peerEvent)
// 	})
// 	if err != nil {
// 		return nil, err
// 	}

// 	s.invalidateCache(ctx, actorID, peerID, &Relation{Type: TypePending})
// 	s.invalidateCache(ctx, actorID, peerID, updatedRel)

// 	return &TransitionFriendsResult{
// 		Channel:      createdChannel,
// 		ActorMember:  actorMember,
// 		PeerUser:     peerUser,
// 		PeerPresence: peerPresence,
// 	}, nil
// }
