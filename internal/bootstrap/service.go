package bootstrap

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"time"

	"bonfire-api/internal/channel"
	"bonfire-api/internal/gateway"
	"bonfire-api/internal/relation"
	"bonfire-api/internal/user"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

type Service struct {
	cache        *Cache
	channelRepo  ChannelRepository
	memberRepo   ChannelMemberRepository
	relationRepo RelationRepository
	userRepo     UserRepository
}

func NewService(
	cache *Cache,
	channelRepo ChannelRepository,
	memberRepo ChannelMemberRepository,
	relationRepo RelationRepository,
	userRepo UserRepository,
) *Service {
	return &Service{
		cache:        cache,
		channelRepo:  channelRepo,
		memberRepo:   memberRepo,
		relationRepo: relationRepo,
		userRepo:     userRepo,
	}
}

type Params struct {
	UserID uuid.UUID
}

type Result struct {
	User               *user.User
	OutgoingPendingIDs []uuid.UUID
	IncomingPendingIDs []uuid.UUID
	FriendIDs          []uuid.UUID
	OutgoingBlockIDs   []uuid.UUID
	UserMembers        map[uuid.UUID]*channel.Member
	Channels           map[uuid.UUID]*channel.Channel
	ChannelMemberIDs   map[uuid.UUID][]uuid.UUID
	SidebarIDs         []uuid.UUID
	Peers              map[uuid.UUID]*user.User
	PeerPresences      map[uuid.UUID]gateway.Presence
}

func (s *Service) Bootstrap(ctx context.Context, p Params) (*Result, error) {
	backfill := BackfillParams{UserID: p.UserID}

	s1, err := s.hydrateUserData(ctx, p.UserID, &backfill)
	if err != nil {
		return nil, err
	}

	s2, err := s.hydrateChannelsAndMembers(ctx, s1.userMembers, &backfill)
	if err != nil {
		return nil, err
	}

	s3, err := s.hydrateUsersAndPresences(
		ctx,
		p.UserID,
		s1.outgoingPendingIDs,
		s1.incomingPendingIDs,
		s1.friendIDs,
		s1.outgoingBlockIDs,
		s2.channelMemberIDs,
		&backfill,
	)
	if err != nil {
		return nil, err
	}

	if backfill.hasWrites() {
		s.backfillCache(ctx, backfill)
	}

	return s.buildResult(
		p.UserID,
		s1.outgoingPendingIDs,
		s1.incomingPendingIDs,
		s1.friendIDs,
		s1.outgoingBlockIDs,
		s1.userMembers,
		s2.channels,
		s2.channelMemberIDs,
		s3.users,
		s3.presences,
	), nil
}

type step1Data struct {
	outgoingPendingIDs []uuid.UUID
	incomingPendingIDs []uuid.UUID
	friendIDs          []uuid.UUID
	outgoingBlockIDs   []uuid.UUID
	userMembers        map[uuid.UUID]*channel.Member
}

func (s *Service) hydrateUserData(
	ctx context.Context,
	userID uuid.UUID,
	backfill *BackfillParams,
) (*step1Data, error) {
	userData, err := s.cache.GetUserData(ctx, userID)
	if err != nil {
		return nil, err
	}

	outgoingPendingIDs := userData.OutgoingPendingIDs
	incomingPendingIDs := userData.IncomingPendingIDs
	friendIDs := userData.FriendIDs
	outgoingBlockIDs := userData.OutgoingBlockIDs
	userMembers := userData.ChannelMembers
	if userMembers == nil {
		userMembers = make(map[uuid.UUID]*channel.Member)
	}

	needPendingOrFriend := userData.OutgoingPendingMiss || userData.IncomingPendingMiss || userData.FriendMiss
	hasMiss := needPendingOrFriend || userData.OutgoingBlockMiss || userData.ChannelMemberMiss

	if !hasMiss {
		return &step1Data{
			outgoingPendingIDs: outgoingPendingIDs,
			incomingPendingIDs: incomingPendingIDs,
			friendIDs:          friendIDs,
			outgoingBlockIDs:   outgoingBlockIDs,
			userMembers:        userMembers,
		}, nil
	}

	var (
		dbOutgoing []uuid.UUID
		dbIncoming []uuid.UUID
		dbFriends  []uuid.UUID
		dbBlocks   []uuid.UUID
		dbUserMems map[uuid.UUID]*channel.Member
	)

	g, gCtx := errgroup.WithContext(ctx)

	if needPendingOrFriend {
		g.Go(func() error {
			rels, qErr := s.relationRepo.ListPendingsAndFriendsByUserID(gCtx, userID, relation.MaxTypeLimit*2)
			if qErr != nil {
				return qErr
			}
			dbOutgoing, dbIncoming, dbFriends = partitionPendingAndFriendRelations(userID, rels)
			return nil
		})
	}

	if userData.OutgoingBlockMiss {
		g.Go(func() error {
			rels, qErr := s.relationRepo.ListOutgoingBlocksByUserID(gCtx, userID, relation.MaxTypeLimit)
			if qErr != nil {
				return qErr
			}
			dbBlocks = relation.ExtractPeerIDs(userID, rels)
			return nil
		})
	}

	if userData.ChannelMemberMiss {
		g.Go(func() error {
			members, qErr := s.memberRepo.ListVisibleByUserID(gCtx, userID, channel.ChannelMaxSidebarItems)
			if qErr != nil {
				return qErr
			}
			mapped := make(map[uuid.UUID]*channel.Member, len(members))
			for _, m := range members {
				if m == nil {
					continue
				}
				mapped[m.ChannelID] = m
			}
			dbUserMems = mapped
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	if userData.OutgoingPendingMiss {
		outgoingPendingIDs = dbOutgoing
		backfill.SetOutgoingPendings = true
		backfill.OutgoingPendingIDs = outgoingPendingIDs
	}
	if userData.IncomingPendingMiss {
		incomingPendingIDs = dbIncoming
		backfill.SetIncomingPendings = true
		backfill.IncomingPendingIDs = incomingPendingIDs
	}
	if userData.FriendMiss {
		friendIDs = dbFriends
		backfill.SetFriends = true
		backfill.FriendIDs = friendIDs
	}
	if userData.OutgoingBlockMiss {
		outgoingBlockIDs = dbBlocks
		backfill.SetOutgoingBlocks = true
		backfill.OutgoingBlockIDs = outgoingBlockIDs
	}
	if userData.ChannelMemberMiss {
		userMembers = dbUserMems
		backfill.SetUserMembers = true
		backfill.UserMembers = userMembers
	}

	return &step1Data{
		outgoingPendingIDs: outgoingPendingIDs,
		incomingPendingIDs: incomingPendingIDs,
		friendIDs:          friendIDs,
		outgoingBlockIDs:   outgoingBlockIDs,
		userMembers:        userMembers,
	}, nil
}

func partitionPendingAndFriendRelations(
	userID uuid.UUID,
	rels []*relation.Relation,
) (outgoingPending []uuid.UUID, incomingPending []uuid.UUID, friends []uuid.UUID) {
	for _, rel := range rels {
		if rel == nil {
			continue
		}

		var peerID uuid.UUID
		switch userID {
		case rel.User1ID:
			peerID = rel.User2ID
		case rel.User2ID:
			peerID = rel.User1ID
		default:
			continue
		}

		switch rel.Type {
		case relation.TypePending:
			if rel.ActorID == userID {
				outgoingPending = append(outgoingPending, peerID)
			} else {
				incomingPending = append(incomingPending, peerID)
			}
		case relation.TypeFriends:
			friends = append(friends, peerID)
		}
	}

	return outgoingPending, incomingPending, friends
}

type step2Data struct {
	channels         map[uuid.UUID]*channel.Channel
	channelMemberIDs map[uuid.UUID][]uuid.UUID
}

func (s *Service) hydrateChannelsAndMembers(
	ctx context.Context,
	userMembers map[uuid.UUID]*channel.Member,
	backfill *BackfillParams,
) (*step2Data, error) {
	channelIDs := make([]uuid.UUID, 0, len(userMembers))
	for chID := range userMembers {
		channelIDs = append(channelIDs, chID)
	}

	chAndMemData, err := s.cache.GetChannelsAndMembers(ctx, channelIDs, channel.ChannelMembersKey)
	if err != nil {
		return nil, err
	}

	channels := chAndMemData.Channels
	if channels == nil {
		channels = make(map[uuid.UUID]*channel.Channel)
	}
	channelMemberIDs := chAndMemData.ChannelMembers
	if channelMemberIDs == nil {
		channelMemberIDs = make(map[uuid.UUID][]uuid.UUID)
	}

	if len(chAndMemData.MissingChIDs) == 0 && len(chAndMemData.MissingMemChIDs) == 0 {
		return &step2Data{
			channels:         channels,
			channelMemberIDs: channelMemberIDs,
		}, nil
	}

	var (
		fetchedChannels  []*channel.Channel
		fetchedMemberIDs map[uuid.UUID][]uuid.UUID
		backfillChs      = make(map[uuid.UUID]*channel.Channel, len(chAndMemData.MissingChIDs))
		backfillMems     = make(map[uuid.UUID][]uuid.UUID, len(chAndMemData.MissingMemChIDs))
	)

	g, gCtx := errgroup.WithContext(ctx)

	if len(chAndMemData.MissingChIDs) > 0 {
		missingChIDs := chAndMemData.MissingChIDs
		g.Go(func() error {
			fetched, qErr := s.channelRepo.GetBatch(gCtx, missingChIDs)
			if qErr != nil {
				return qErr
			}
			fetchedChannels = fetched
			for _, ch := range fetched {
				if ch == nil {
					continue
				}
				backfillChs[ch.ID] = ch
			}
			return nil
		})
	}

	if len(chAndMemData.MissingMemChIDs) > 0 {
		missingMemIDs := chAndMemData.MissingMemChIDs
		g.Go(func() error {
			fetchedMap, qErr := s.memberRepo.GetBatchByChannelIDs(gCtx, missingMemIDs)
			if qErr != nil {
				return qErr
			}
			mems := make(map[uuid.UUID][]uuid.UUID, len(fetchedMap))
			for chID, members := range fetchedMap {
				ids := make([]uuid.UUID, 0, len(members))
				for _, m := range members {
					if m == nil {
						continue
					}
					ids = append(ids, m.UserID)
				}
				mems[chID] = ids
				backfillMems[chID] = ids
			}
			fetchedMemberIDs = mems
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	for _, ch := range fetchedChannels {
		if ch == nil {
			continue
		}
		channels[ch.ID] = ch
	}
	for chID, ids := range fetchedMemberIDs {
		channelMemberIDs[chID] = ids
	}

	if len(backfillChs) > 0 {
		backfill.Channels = backfillChs
	}
	if len(backfillMems) > 0 {
		backfill.ChannelMemberIDs = backfillMems
	}

	return &step2Data{
		channels:         channels,
		channelMemberIDs: channelMemberIDs,
	}, nil
}

type step3Data struct {
	users     map[uuid.UUID]*user.User
	presences map[uuid.UUID]gateway.Presence
}

func (s *Service) hydrateUsersAndPresences(
	ctx context.Context,
	userID uuid.UUID,
	outgoingPendingIDs []uuid.UUID,
	incomingPendingIDs []uuid.UUID,
	friendIDs []uuid.UUID,
	outgoingBlockIDs []uuid.UUID,
	channelMemberIDs map[uuid.UUID][]uuid.UUID,
	backfill *BackfillParams,
) (*step3Data, error) {
	allUserIDs := collectUniqueUserIDs(
		userID,
		outgoingPendingIDs,
		incomingPendingIDs,
		friendIDs,
		outgoingBlockIDs,
		channelMemberIDs,
	)

	usersAndPresences, err := s.cache.GetUsersAndPresences(ctx, allUserIDs)
	if err != nil {
		return nil, err
	}

	users := usersAndPresences.Users
	if users == nil {
		users = make(map[uuid.UUID]*user.User)
	}

	if len(usersAndPresences.MissingUserIDs) > 0 {
		fetchedUsers, qErr := s.userRepo.GetBatch(ctx, usersAndPresences.MissingUserIDs)
		if qErr != nil {
			return nil, qErr
		}

		backfillUsers := make(map[uuid.UUID]*user.User, len(fetchedUsers))
		for _, u := range fetchedUsers {
			if u == nil {
				continue
			}
			users[u.ID] = u
			backfillUsers[u.ID] = u
		}

		if len(backfillUsers) > 0 {
			backfill.Users = backfillUsers
		}
	}

	return &step3Data{
		users:     users,
		presences: usersAndPresences.Presences,
	}, nil
}

func (s *Service) buildResult(
	userID uuid.UUID,
	outgoingPendingIDs []uuid.UUID,
	incomingPendingIDs []uuid.UUID,
	friendIDs []uuid.UUID,
	outgoingBlockIDs []uuid.UUID,
	userMembers map[uuid.UUID]*channel.Member,
	channels map[uuid.UUID]*channel.Channel,
	channelMemberIDs map[uuid.UUID][]uuid.UUID,
	users map[uuid.UUID]*user.User,
	presences map[uuid.UUID]gateway.Presence,
) *Result {
	outPending := nonNilUUIDs(outgoingPendingIDs)
	inPending := nonNilUUIDs(incomingPendingIDs)
	friends := nonNilUUIDs(friendIDs)
	outBlocks := nonNilUUIDs(outgoingBlockIDs)

	relation.SortIDs(outPending, users)
	relation.SortIDs(inPending, users)
	relation.SortIDs(friends, users)
	relation.SortIDs(outBlocks, users)

	sortedChannels := make([]*channel.Channel, 0, len(channels))
	for _, ch := range channels {
		if ch != nil {
			sortedChannels = append(sortedChannels, ch)
		}
	}

	sortSidebar(sortedChannels, userMembers)

	if len(sortedChannels) > channel.ChannelMaxSidebarItems {
		sortedChannels = sortedChannels[:channel.ChannelMaxSidebarItems]
	}

	sidebarIDs := make([]uuid.UUID, 0, len(sortedChannels))
	for _, ch := range sortedChannels {
		sidebarIDs = append(sidebarIDs, ch.ID)
	}

	peers := make(map[uuid.UUID]*user.User)
	for uid, u := range users {
		if uid == userID || u == nil {
			continue
		}
		peers[uid] = u
	}

	peerPresences := make(map[uuid.UUID]gateway.Presence)
	for uid, pres := range presences {
		if uid == userID {
			continue
		}
		peerPresences[uid] = pres
	}

	if userMembers == nil {
		userMembers = make(map[uuid.UUID]*channel.Member)
	}
	if channels == nil {
		channels = make(map[uuid.UUID]*channel.Channel)
	}
	if channelMemberIDs == nil {
		channelMemberIDs = make(map[uuid.UUID][]uuid.UUID)
	}

	return &Result{
		User:               users[userID],
		OutgoingPendingIDs: outPending,
		IncomingPendingIDs: inPending,
		FriendIDs:          friends,
		OutgoingBlockIDs:   outBlocks,
		UserMembers:        userMembers,
		Channels:           channels,
		ChannelMemberIDs:   channelMemberIDs,
		SidebarIDs:         sidebarIDs,
		Peers:              peers,
		PeerPresences:      peerPresences,
	}
}

func nonNilUUIDs(ids []uuid.UUID) []uuid.UUID {
	if ids == nil {
		return []uuid.UUID{}
	}
	return ids
}

func (s *Service) backfillCache(ctx context.Context, params BackfillParams) {
	cacheCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), backfillTimeout)
	defer cancel()

	if err := s.cache.Backfill(cacheCtx, params); err != nil {
		slog.WarnContext(cacheCtx, "failed to backfill bootstrap cache",
			slog.String("user_id", params.UserID.String()),
			slog.Any("error", err),
		)
	}
}

func collectUniqueUserIDs(
	userID uuid.UUID,
	outgoingPendingIDs []uuid.UUID,
	incomingPendingIDs []uuid.UUID,
	friendIDs []uuid.UUID,
	outgoingBlockIDs []uuid.UUID,
	channelMemberIDs map[uuid.UUID][]uuid.UUID,
) []uuid.UUID {
	capacity := 1 + len(outgoingPendingIDs) + len(incomingPendingIDs) + len(friendIDs) + len(outgoingBlockIDs) + len(channelMemberIDs)*4
	seen := make(map[uuid.UUID]struct{}, capacity)
	add := func(id uuid.UUID) {
		if id == uuid.Nil {
			return
		}
		seen[id] = struct{}{}
	}

	add(userID)
	for _, id := range outgoingPendingIDs {
		add(id)
	}
	for _, id := range incomingPendingIDs {
		add(id)
	}
	for _, id := range friendIDs {
		add(id)
	}
	for _, id := range outgoingBlockIDs {
		add(id)
	}
	for _, memberIDs := range channelMemberIDs {
		for _, uid := range memberIDs {
			add(uid)
		}
	}

	ids := make([]uuid.UUID, 0, len(seen))
	for uid := range seen {
		ids = append(ids, uid)
	}
	return ids
}

func sortSidebar(channels []*channel.Channel, userMembersMap map[uuid.UUID]*channel.Member) {
	pinnedAt := func(c *channel.Channel) time.Time {
		if c == nil {
			return time.Time{}
		}
		if m := userMembersMap[c.ID]; m != nil && m.PinnedAt != nil {
			return *m.PinnedAt
		}
		return time.Time{}
	}

	slices.SortFunc(channels, func(a, b *channel.Channel) int {
		pinA, pinB := pinnedAt(a), pinnedAt(b)
		isPinnedA, isPinnedB := !pinA.IsZero(), !pinB.IsZero()

		if isPinnedA != isPinnedB {
			if isPinnedA {
				return -1
			}
			return 1
		}
		if isPinnedA && isPinnedB {
			if cmp := pinB.Compare(pinA); cmp != 0 {
				return cmp
			}
		}

		var tA, tB time.Time
		if a != nil {
			tA = a.LastMessageAt
		}
		if b != nil {
			tB = b.LastMessageAt
		}
		if cmp := tB.Compare(tA); cmp != 0 {
			return cmp
		}

		var cA, cB time.Time
		if a != nil {
			cA = a.CreatedAt
		}
		if b != nil {
			cB = b.CreatedAt
		}
		if cmp := cB.Compare(cA); cmp != 0 {
			return cmp
		}

		var idA, idB uuid.UUID
		if a != nil {
			idA = a.ID
		}
		if b != nil {
			idB = b.ID
		}
		return bytes.Compare(idA[:], idB[:])
	})
}

// type Result struct {
// 	User *user.User
// 	// Friends               map[uuid.UUID]uuid.UUID         `json:"friends"`
// 	// UserMembers           map[uuid.UUID]*channel.Member   `json:"userMembers"`
// 	// Channels              map[uuid.UUID]*channel.Channel  `json:"channels"`
// 	// ChannelPeerIDs        map[uuid.UUID][]uuid.UUID       `json:"channelPeerIDs"`
// 	// ChannelMemberIDs      map[uuid.UUID][]uuid.UUID       `json:"channelMemberIDs"`
// 	// FriendIDs             []uuid.UUID                     `json:"friendIDs"`
// 	// PendingIDs            []uuid.UUID                     `json:"pendingIDs"`
// 	// SidebarIDs            []uuid.UUID                     `json:"sidebarIDs"`
// 	// Peers                 map[uuid.UUID]*user.User        `json:"peers"`
// 	// PeerPresences         map[uuid.UUID]presence.Presence `json:"peerPresences"`
// 	// Messages              []*channel.Message              `json:"messages"`
// 	// HasMoreMessagesBefore bool                            `json:"hasMoreMessagesBefore"`
// 	// HasMoreMessagesAfter  bool                            `json:"hasMoreMessagesAfter"`
// }

// type Service struct {
// 	// channelRepo        ChannelRepository
// 	// cachedChannelRepo  CachedChannelRepository
// 	// memberRepo         MemberRepository
// 	// cachedMemberRepo   CachedMemberRepository
// 	// messageRepo        MessageRepository
// 	// cachedMessageRepo  CachedMessageRepository
// 	// reactionRepo       ReactionRepository
// 	// outboxRepo         OutboxRepository
// 	// presenceCache      PresenceCache
// 	// relationRepo       RelationRepository
// 	// cachedRelationRepo CachedRelationRepository
// 	userRepo       UserRepository
// 	cachedUserRepo CachedUserRepository
// 	// tx                 TX
// }

// func NewService(
// 	// channelRepo ChannelRepository,
// 	// cachedChannelRepo CachedChannelRepository,
// 	// memberRepo MemberRepository,
// 	// cachedMemberRepo CachedMemberRepository,
// 	// messageRepo MessageRepository,
// 	// cachedMessageRepo CachedMessageRepository,
// 	// reactionRepo ReactionRepository,
// 	// outboxRepo OutboxRepository,
// 	// presenceCache PresenceCache,
// 	// relationRepo RelationRepository,
// 	// cachedRelationRepo CachedRelationRepository,
// 	userRepo UserRepository,
// 	cachedUserRepo CachedUserRepository,
// 	// tx TX,
// ) *Service {
// 	return &Service{
// 		// channelRepo:        channelRepo,
// 		// cachedChannelRepo:  cachedChannelRepo,
// 		// memberRepo:         memberRepo,
// 		// cachedMemberRepo:   cachedMemberRepo,
// 		// messageRepo:        messageRepo,
// 		// cachedMessageRepo:  cachedMessageRepo,
// 		// reactionRepo:       reactionRepo,
// 		// outboxRepo:         outboxRepo,
// 		// presenceCache:      presenceCache,
// 		// relationRepo:       relationRepo,
// 		// cachedRelationRepo: cachedRelationRepo,
// 		userRepo:       userRepo,
// 		cachedUserRepo: cachedUserRepo,
// 		// tx:                 tx,
// 	}
// }

// func getMessagesCursor(
// 	actorLastReadID *uuid.UUID,
// 	fallbackMessageID *uuid.UUID,
// ) (*uuid.UUID, int, int) {
// 	if fallbackMessageID != nil {
// 		return fallbackMessageID, channel.MessageListBeforeLimit, channel.MessageListAfterLimit
// 	}

// 	if actorLastReadID != nil {
// 		return actorLastReadID, channel.MessageListBeforeLimit, channel.MessageListAfterLimit
// 	}

// 	return fallbackMessageID, channel.MessageListLimit, 0
// }

// func getMemberChannelIDs(members []*channel.Member) []uuid.UUID {
// 	channelIDs := make([]uuid.UUID, 0, len(members))

// 	for _, m := range members {
// 		if m == nil {
// 			continue
// 		}
// 		channelIDs = append(channelIDs, m.ChannelID)
// 	}

// 	return channelIDs
// }

// // Bootstrap fetches all channel data needed to load a channel, including details, members, and messages.
// func (s *Service) Bootstrap(ctx context.Context, userID uuid.UUID, channelID, messageID *uuid.UUID) (*Result, error) {
// 	user, err := s.cachedUserRepo.Get(ctx, userID)
// 	if err != nil {
// 		return nil, err
// 	}

// 	friends, err := s.cachedRelationRepo.GetFriends(ctx, userID)
// 	if err != nil {
// 		return nil, err
// 	}

// 	friendIDs := make([]uuid.UUID, 0, len(friends))
// 	seenPeers := make(map[uuid.UUID]struct{}, len(friends))

// 	for friendID := range friends {
// 		friendIDs = append(friendIDs, friendID)
// 		seenPeers[friendID] = struct{}{}
// 	}

// 	var (
// 		pendingIDs []uuid.UUID
// 		pendingErr error
// 	)

// 	pendingIDs, pendingErr = s.cachedRelationRepo.GetPendingIDs(ctx, userID)
// 	if pendingErr != nil {
// 		return nil, fmt.Errorf("failed to fetch pending IDs: %w", pendingErr)
// 	}

// 	userMembers, err := s.cachedMemberRepo.ListVisibleByUserID(ctx, userID, 100)
// 	if err != nil {
// 		return nil, err
// 	}

// 	memberChannelIDs := getMemberChannelIDs(userMembers)
// 	channels, err := s.cachedChannelRepo.GetBatch(ctx, memberChannelIDs)
// 	if err != nil {
// 		return nil, err
// 	}

// 	channelMembersMap, err := s.cachedMemberRepo.GetBatchByChannelIDs(ctx, memberChannelIDs)
// 	if err != nil {
// 		return nil, err
// 	}

// 	channelPeerIDsMap := make(map[uuid.UUID][]uuid.UUID, len(channelMembersMap))
// 	channelMemberIDsMap := make(map[uuid.UUID][]uuid.UUID, len(channelMembersMap))

// 	for channelID, members := range channelMembersMap {
// 		peerIDs := make([]uuid.UUID, 0, len(members))
// 		memberIDs := make([]uuid.UUID, 0, len(members))

// 		for _, member := range members {
// 			mID := member.UserID
// 			memberIDs = append(memberIDs, mID)

// 			if mID == userID {
// 				continue
// 			}

// 			peerIDs = append(peerIDs, mID)
// 			seenPeers[mID] = struct{}{}
// 		}

// 		slices.SortFunc(peerIDs, func(a, b uuid.UUID) int {
// 			return bytes.Compare(a[:], b[:])
// 		})

// 		channelPeerIDsMap[channelID] = peerIDs
// 		channelMemberIDsMap[channelID] = memberIDs
// 	}

// 	// Extract all unique peer IDs (friends + channel peers)
// 	dedupedPeerIDs := make([]uuid.UUID, 0, len(seenPeers))
// 	for peerID := range seenPeers {
// 		dedupedPeerIDs = append(dedupedPeerIDs, peerID)
// 	}

// 	peers, err := s.cachedUserRepo.GetBatch(ctx, dedupedPeerIDs)
// 	if err != nil {
// 		return nil, err
// 	}

// 	peerPresences, err := s.presenceCache.GetBatchPresence(ctx, dedupedPeerIDs)
// 	if err != nil {
// 		return nil, err
// 	}

// 	getDisplayName := func(id uuid.UUID) string {
// 		if id == userID && user != nil {
// 			return user.DisplayName
// 		}
// 		if u, ok := peers[id]; ok {
// 			return u.DisplayName
// 		}
// 		return ""
// 	}

// 	for _, memberIDs := range channelMemberIDsMap {
// 		slices.SortFunc(memberIDs, func(a, b uuid.UUID) int {
// 			nameA := strings.ToLower(getDisplayName(a))
// 			nameB := strings.ToLower(getDisplayName(b))

// 			if nameA != nameB {
// 				return strings.Compare(nameA, nameB)
// 			}
// 			// Fallback tie-breaker by UUIDv7 ascending
// 			return bytes.Compare(a[:], b[:])
// 		})
// 	}

// 	// 1. Sort friendIDs by display name ascending (case-insensitive)
// 	slices.SortFunc(friendIDs, func(a, b uuid.UUID) int {
// 		nameA := strings.ToLower(getDisplayName(a))
// 		nameB := strings.ToLower(getDisplayName(b))

// 		if nameA != nameB {
// 			return strings.Compare(nameA, nameB)
// 		}
// 		// Fallback tie-breaker by UUIDv7 ascending
// 		return bytes.Compare(a[:], b[:])
// 	})

// 	// 2. Sort pendingIDs by UUIDv7 descending (newest first)
// 	slices.SortFunc(pendingIDs, func(a, b uuid.UUID) int {
// 		return bytes.Compare(b[:], a[:])
// 	})

// 	// 3. Map userMembers by ChannelID for O(1) lookup in sortSidebar
// 	userMembersMap := make(map[uuid.UUID]*channel.Member, len(userMembers))
// 	for _, m := range userMembers {
// 		userMembersMap[m.ChannelID] = m
// 	}

// 	// 4. Convert channels map to a slice for sorting
// 	sidebarChannels := make([]*channel.Channel, 0, len(channels))
// 	for _, c := range channels {
// 		sidebarChannels = append(sidebarChannels, c)
// 	}

// 	// 5. In-place sort channels according to sidebar criteria
// 	sortSidebar(sidebarChannels, userMembersMap)

// 	// 6. Extract sorted channel IDs
// 	sidebarIDs := make([]uuid.UUID, len(sidebarChannels))
// 	for i, c := range sidebarChannels {
// 		sidebarIDs[i] = c.ID
// 	}

// 	var (
// 		messages      []*channel.Message
// 		hasMoreBefore bool
// 		hasMoreAfter  bool
// 		messagesErr   error
// 	)

// 	if channelID != nil {
// 		userMember := userMembersMap[*channelID]
// 		var actorLastReadID uuid.UUID
// 		if userMember != nil {
// 			actorLastReadID = *userMember.LastReadMessageID
// 		}

// 		msgCursor, beforeLimit, afterLimit := getMessagesCursor(&actorLastReadID, messageID)

// 		messages, hasMoreBefore, hasMoreAfter, messagesErr = s.cachedMessageRepo.ListAroundByChannelID(
// 			ctx,
// 			*channelID,
// 			*msgCursor,
// 			beforeLimit,
// 			afterLimit,
// 		)
// 		if messagesErr != nil {
// 			return nil, fmt.Errorf("failed to fetch messages: %w", messagesErr)
// 		}
// 	}

// 	return &Result{
// 		User:                  user,
// 		Friends:               friends,
// 		UserMembers:           userMembersMap,
// 		Channels:              channels,
// 		ChannelPeerIDs:        channelPeerIDsMap,
// 		ChannelMemberIDs:      channelMemberIDsMap,
// 		FriendIDs:             friendIDs,
// 		PendingIDs:            pendingIDs,
// 		SidebarIDs:            sidebarIDs,
// 		Peers:                 peers,
// 		PeerPresences:         peerPresences,
// 		Messages:              messages,
// 		HasMoreMessagesBefore: hasMoreBefore,
// 		HasMoreMessagesAfter:  hasMoreAfter,
// 	}, nil
// }
