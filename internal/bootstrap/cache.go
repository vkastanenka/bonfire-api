package bootstrap

import (
	"context"
	"strconv"
	"time"

	"bonfire-api/internal/channel"
	"bonfire-api/internal/gateway"
	"bonfire-api/internal/redis"
	"bonfire-api/internal/relation"
	"bonfire-api/internal/user"

	"github.com/google/uuid"
	redisdriver "github.com/redis/go-redis/v9"
)

type Cache struct {
	client redisdriver.Cmdable
}

func NewCache(client redisdriver.Cmdable) *Cache {
	return &Cache{client: client}
}

type UserDataResult struct {
	PendingIDs        []uuid.UUID
	PendingMiss       bool
	FriendIDs         []uuid.UUID
	FriendMiss        bool
	OutgoingBlockIDs  []uuid.UUID
	OutgoingBlockMiss bool
	ChannelMembers    map[uuid.UUID]*channel.Member
	ChannelMemberMiss bool
}

type ChannelsAndMembersResult struct {
	Channels        map[uuid.UUID]*channel.Channel
	MissingChIDs    []uuid.UUID
	ChannelMembers  map[uuid.UUID][]uuid.UUID
	MissingMemChIDs []uuid.UUID
}

type UsersAndPresencesResult struct {
	Users          map[uuid.UUID]*user.User
	MissingUserIDs []uuid.UUID
	Presences      map[uuid.UUID]gateway.Presence
}

// Step 1: Fetch pendings, friends, outgoing blocks, and user channel memberships in one pipeline.
func (c *Cache) GetUserData(ctx context.Context, userID uuid.UUID) (*UserDataResult, error) {
	pipe := c.client.Pipeline()

	pendingsCmd := pipe.SMembers(ctx, relation.UserPendingsKey(userID))
	friendsCmd := pipe.SMembers(ctx, relation.UserFriendsKey(userID))
	outgoingBlocksCmd := pipe.SMembers(ctx, relation.UserOutgoingBlocksKey(userID))
	channelMembersCmd := pipe.HGetAll(ctx, channel.UserChannelMembersKey(userID))

	_, err := pipe.Exec(ctx)
	if err != nil && !redis.IsCacheMiss(err) {
		return nil, redis.NewError(err, redis.ResourceRelation)
	}

	out := &UserDataResult{}

	pendingIDs, ok := parseUUIDStrings(pendingsCmd.Val())
	if !ok {
		out.PendingMiss = true
	} else {
		out.PendingIDs = pendingIDs
	}

	friendIDs, ok := parseUUIDStrings(friendsCmd.Val())
	if !ok {
		out.FriendMiss = true
	} else {
		out.FriendIDs = friendIDs
	}

	blockIDs, ok := parseUUIDStrings(outgoingBlocksCmd.Val())
	if !ok {
		out.OutgoingBlockMiss = true
	} else {
		out.OutgoingBlockIDs = blockIDs
	}

	rawMembers, err := channelMembersCmd.Result()
	if err != nil || len(rawMembers) == 0 {
		out.ChannelMemberMiss = true
	} else {
		parsedMembers := make(map[uuid.UUID]*channel.Member, len(rawMembers))
		for chKeyStr, rawVal := range rawMembers {
			chID, parseErr := uuid.Parse(chKeyStr)
			if parseErr != nil || rawVal == "" {
				continue
			}
			memberModel, unmarshalErr := channel.UnmarshalMember([]byte(rawVal))
			if unmarshalErr != nil || memberModel == nil {
				continue
			}
			parsedMembers[chID] = memberModel
		}

		if len(parsedMembers) == 0 {
			out.ChannelMemberMiss = true
		} else {
			out.ChannelMembers = parsedMembers
		}
	}
	return out, nil
}

// Step 2: Fetch channel entities and channel member ID sets in batch in one pipeline.
func (c *Cache) GetChannelsAndMembers(
	ctx context.Context,
	channelIDs []uuid.UUID,
	memberSetKeyFn func(uuid.UUID) string,
) (*ChannelsAndMembersResult, error) {
	out := &ChannelsAndMembersResult{
		Channels:       make(map[uuid.UUID]*channel.Channel, len(channelIDs)),
		ChannelMembers: make(map[uuid.UUID][]uuid.UUID, len(channelIDs)),
	}
	if len(channelIDs) == 0 {
		return out, nil
	}

	pipe := c.client.Pipeline()
	chCmds := make(map[uuid.UUID]*redisdriver.StringCmd, len(channelIDs))
	memCmds := make(map[uuid.UUID]*redisdriver.StringSliceCmd, len(channelIDs))

	for _, chID := range channelIDs {
		chCmds[chID] = pipe.Get(ctx, channel.ChannelKey(chID))
		memCmds[chID] = pipe.SMembers(ctx, memberSetKeyFn(chID))
	}

	_, err := pipe.Exec(ctx)
	if err != nil && !redis.IsCacheMiss(err) {
		return nil, redis.NewError(err, redis.ResourceChannel)
	}

	for _, chID := range channelIDs {
		rawCh, cmdErr := chCmds[chID].Bytes()
		if cmdErr != nil || len(rawCh) == 0 {
			out.MissingChIDs = append(out.MissingChIDs, chID)
		} else {
			chModel, unmarshalErr := channel.UnmarshalChannel(rawCh)
			if unmarshalErr != nil || chModel == nil {
				out.MissingChIDs = append(out.MissingChIDs, chID)
			} else {
				out.Channels[chID] = chModel
			}
		}

		memberIDs, ok := parseUUIDStrings(memCmds[chID].Val())
		if !ok {
			out.MissingMemChIDs = append(out.MissingMemChIDs, chID)
		} else {
			out.ChannelMembers[chID] = memberIDs
		}
	}

	return out, nil
}

// Step 3: Fetch deduplicated user profiles and user presences in batch in one pipeline.
func (c *Cache) GetUsersAndPresences(
	ctx context.Context,
	userIDs []uuid.UUID,
) (*UsersAndPresencesResult, error) {
	out := &UsersAndPresencesResult{
		Users:     make(map[uuid.UUID]*user.User, len(userIDs)),
		Presences: make(map[uuid.UUID]gateway.Presence, len(userIDs)),
	}
	if len(userIDs) == 0 {
		return out, nil
	}

	pipe := c.client.Pipeline()
	userCmds := make(map[uuid.UUID]*redisdriver.StringCmd, len(userIDs))
	presenceCmds := make(map[uuid.UUID]*redisdriver.StringCmd, len(userIDs))

	for _, uid := range userIDs {
		userCmds[uid] = pipe.Get(ctx, user.CacheKey(uid))
		presenceCmds[uid] = pipe.HGet(ctx, gateway.UserCacheKey(uid), "presence")
	}

	_, err := pipe.Exec(ctx)
	if err != nil && !redis.IsCacheMiss(err) {
		return nil, redis.NewError(err, redis.ResourceUser)
	}

	for _, uid := range userIDs {
		rawUser, uErr := userCmds[uid].Bytes()
		if uErr != nil || len(rawUser) == 0 {
			out.MissingUserIDs = append(out.MissingUserIDs, uid)
		} else {
			uModel, unmarshalErr := user.UnmarshalCacheDTO(rawUser)
			if unmarshalErr != nil || uModel == nil {
				out.MissingUserIDs = append(out.MissingUserIDs, uid)
			} else {
				out.Users[uid] = uModel
			}
		}

		out.Presences[uid] = parsePresenceValue(presenceCmds[uid].Val())
	}

	return out, nil
}

func parseUUIDStrings(raw []string) ([]uuid.UUID, bool) {
	if len(raw) == 0 {
		return nil, false
	}

	ids := make([]uuid.UUID, 0, len(raw))
	for _, s := range raw {
		id, err := uuid.Parse(s)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		return nil, false
	}
	return ids, true
}

func parsePresenceValue(val string) gateway.Presence {
	if val == "" {
		return gateway.PresenceOffline
	}
	intVal, err := strconv.Atoi(val)
	if err != nil {
		return gateway.PresenceOffline
	}
	p, err := gateway.ParsePresenceInt(intVal)
	if err != nil || !p.IsValid() {
		return gateway.PresenceOffline
	}
	return p
}

const backfillTimeout = 2 * time.Second

type BackfillParams struct {
	UserID uuid.UUID

	SetPendings       bool
	PendingIDs        []uuid.UUID
	SetFriends        bool
	FriendIDs         []uuid.UUID
	SetOutgoingBlocks bool
	OutgoingBlockIDs  []uuid.UUID

	SetUserMembers bool
	UserMembers    map[uuid.UUID]*channel.Member

	Channels         map[uuid.UUID]*channel.Channel
	ChannelMemberIDs map[uuid.UUID][]uuid.UUID
	Users            map[uuid.UUID]*user.User
}

func (c *Cache) Backfill(ctx context.Context, p BackfillParams) error {
	pipe := c.client.Pipeline()
	var queued int

	queueSetIDs := func(key string, ids []uuid.UUID, ttl time.Duration) {
		if len(ids) == 0 {
			return
		}
		members := make([]any, len(ids))
		for i, id := range ids {
			members[i] = id.String()
		}
		pipe.Del(ctx, key)
		pipe.SAdd(ctx, key, members...)
		if ttl > 0 {
			pipe.Expire(ctx, key, ttl)
		}
		queued++
	}

	if p.SetPendings {
		queueSetIDs(relation.UserPendingsKey(p.UserID), p.PendingIDs, relation.UserPendingsTTL)
	}
	if p.SetFriends {
		queueSetIDs(relation.UserFriendsKey(p.UserID), p.FriendIDs, relation.UserFriendsTTL)
	}
	if p.SetOutgoingBlocks {
		queueSetIDs(relation.UserOutgoingBlocksKey(p.UserID), p.OutgoingBlockIDs, relation.UserBlocksTTL)
	}

	if p.SetUserMembers && len(p.UserMembers) > 0 {
		args := make([]any, 0, len(p.UserMembers)*2)
		for chID, m := range p.UserMembers {
			if m == nil {
				continue
			}
			raw, err := channel.MarshalMember(m)
			if err != nil {
				return redis.ErrResourceMarshalFailed().
					Meta("resource", redis.ResourceMember.String()).
					Meta("key", channel.UserChannelMembersKey(p.UserID)).
					Wrap(err)
			}
			args = append(args, chID.String(), raw)
		}
		if len(args) > 0 {
			key := channel.UserChannelMembersKey(p.UserID)
			pipe.Del(ctx, key)
			pipe.HSet(ctx, key, args...)
			if channel.UserChannelsMembersTTL > 0 {
				pipe.Expire(ctx, key, channel.UserChannelsMembersTTL)
			}
			queued++
		}
	}

	for chID, ch := range p.Channels {
		if ch == nil {
			continue
		}
		raw, err := channel.MarshalChannel(ch)
		if err != nil {
			return redis.ErrResourceMarshalFailed().
				Meta("resource", redis.ResourceChannel.String()).
				Meta("key", channel.ChannelKey(chID)).
				Wrap(err)
		}
		pipe.Set(ctx, channel.ChannelKey(chID), raw, channel.ChannelTTL)
		queued++
	}

	for chID, memberIDs := range p.ChannelMemberIDs {
		queueSetIDs(channel.ChannelMembersKey(chID), memberIDs, channel.ChannelMembersTTL)
	}

	for uid, u := range p.Users {
		if u == nil {
			continue
		}
		raw, err := user.MarshalCacheDTO(u)
		if err != nil {
			return redis.ErrResourceMarshalFailed().
				Meta("resource", redis.ResourceUser.String()).
				Meta("key", user.CacheKey(uid)).
				Wrap(err)
		}
		pipe.Set(ctx, user.CacheKey(uid), raw, user.CacheTTL)
		queued++
	}

	if queued == 0 {
		return nil
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return redis.NewError(err, redis.ResourceRelation)
	}

	return nil
}
