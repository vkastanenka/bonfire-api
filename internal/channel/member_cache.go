package channel

import (
	"bonfire-api/internal/pkg/errs"
	"bonfire-api/internal/redis"
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	redisdriver "github.com/redis/go-redis/v9"
)

const (
	UserChannelsMembersTTL = 24 * time.Hour
)

func UserChannelMembersKey(id uuid.UUID) string {
	return "user:" + "{" + id.String() + "}" + ":channel_members"
}

type MemberCache struct {
	client redisdriver.Cmdable
}

func NewMemberCache(client redisdriver.Cmdable) *MemberCache {
	return &MemberCache{client: client}
}

func (c *MemberCache) GetUserMembers(ctx context.Context, userID uuid.UUID) (map[uuid.UUID]*Member, error) {
	_, _, members, err := redis.GetHashMapKeyVals(
		ctx,
		c.client,
		redis.ResourceMember,
		UserChannelMembersKey(userID),
		func(raw string) (*Member, error) {
			return unmarshalMember([]byte(raw))
		},
	)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}

	return members, nil
}

func (c *MemberCache) SetUserMembers(
	ctx context.Context,
	userID uuid.UUID,
	members map[uuid.UUID]*Member,
) error {
	if len(members) == 0 {
		return nil
	}

	return redis.SetHashMapKeyVals(
		ctx,
		c.client,
		redis.ResourceMember,
		UserChannelMembersKey(userID),
		members,
		UserChannelsMembersTTL,
		uuid.UUID.String,
		marshalMember,
	)
}

func unmarshalMember(data []byte) (*Member, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var m Member
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}

	return &m, nil
}

func marshalMember(m *Member) ([]byte, error) {
	if m == nil {
		return nil, nil
	}

	bytes, err := json.Marshal(m)
	if err != nil {
		return nil, errs.Internal("Failed to marshal channel member json.").Wrap(err)
	}

	return bytes, nil
}

// func (c *MemberCache) Invalidate(ctx context.Context, channelID, userID uuid.UUID) error {
// 	if err := c.client.HDel(ctx, channelMembersKey(channelID), userID.String()).Err(); err != nil {
// 		return redis.NewError(err, redis.ScopeMember)
// 	}
// 	return nil
// }

// func (c *MemberCache) InvalidateBatch(ctx context.Context, channelID uuid.UUID, userIDs []uuid.UUID) error {
// 	if len(userIDs) == 0 {
// 		return nil
// 	}

// 	fields := make([]string, len(userIDs))
// 	for i, id := range userIDs {
// 		fields[i] = id.String()
// 	}

// 	if err := c.client.HDel(ctx, channelMembersKey(channelID), fields...).Err(); err != nil {
// 		return redis.NewError(err, redis.ScopeMember)
// 	}
// 	return nil
// }

// func (c *MemberCache) InvalidateChannel(ctx context.Context, channelID uuid.UUID) error {
// 	if err := c.client.Del(ctx, channelMembersKey(channelID)).Err(); err != nil {
// 		return redis.NewError(err, redis.ScopeMember)
// 	}
// 	return nil
// }

// func (c *MemberCache) SetUserMembers(ctx context.Context, userID uuid.UUID, members []*Member) error {
// 	if userID == uuid.Nil || len(members) == 0 {
// 		return nil
// 	}

// 	pipe := c.client.Pipeline()

// 	zKey := userChannelsKey(userID)
// 	pipe.Del(ctx, zKey)

// 	for _, m := range members {
// 		if m == nil || m.ChannelID == uuid.Nil {
// 			continue
// 		}

// 		var score float64
// 		if m.PinnedAt != nil && !m.PinnedAt.IsZero() {
// 			score = 1e12 + float64(m.PinnedAt.Unix())
// 		} else {
// 			score = float64(m.CreatedAt.Unix())
// 		}

// 		pipe.ZAdd(ctx, zKey, redisdriver.Z{
// 			Score:  score,
// 			Member: m.ChannelID.String(),
// 		})
// 	}

// 	for _, m := range members {
// 		if m == nil || m.ChannelID == uuid.Nil || m.UserID == uuid.Nil {
// 			continue
// 		}

// 		mBytes, err := marshalMember(m)
// 		if err != nil {
// 			return err
// 		}

// 		pipe.HSet(ctx, channelMembersKey(m.ChannelID), m.UserID.String(), mBytes)
// 	}

// 	if _, err := pipe.Exec(ctx); err != nil {
// 		return redis.NewError(err, redis.ScopeMember)
// 	}

// 	return nil
// }

// func (c *MemberCache) Add(ctx context.Context, channelID uuid.UUID, members []*Member) error {
// 	if len(members) == 0 {
// 		return nil
// 	}

// 	memberMap := make(map[string]any, len(members))
// 	for _, m := range members {
// 		if m == nil || m.UserID == uuid.Nil {
// 			continue
// 		}

// 		mBytes, err := marshalMember(m)
// 		if err != nil {
// 			return err
// 		}
// 		memberMap[m.UserID.String()] = mBytes
// 	}

// 	if len(memberMap) == 0 {
// 		return nil
// 	}

// 	pipe := c.client.Pipeline()
// 	pipe.HSet(ctx, channelMembersKey(channelID), memberMap)

// 	channelIDStr := channelID.String()
// 	for _, m := range members {
// 		if m == nil || m.UserID == uuid.Nil {
// 			continue
// 		}

// 		var score float64
// 		if m.PinnedAt != nil && !m.PinnedAt.IsZero() {
// 			score = 1e12 + float64(m.PinnedAt.Unix())
// 		} else {
// 			score = float64(m.CreatedAt.Unix())
// 		}

// 		pipe.ZAdd(ctx, userChannelsKey(m.UserID), redisdriver.Z{
// 			Score:  score,
// 			Member: channelIDStr,
// 		})
// 	}

// 	if _, err := pipe.Exec(ctx); err != nil {
// 		return redis.NewError(err, redis.ScopeMember)
// 	}

// 	return nil
// }

// func (c *MemberCache) Remove(ctx context.Context, channelID, userID uuid.UUID) error {
// 	pipe := c.client.Pipeline()

// 	pipe.HDel(ctx, channelMembersKey(channelID), userID.String())
// 	pipe.ZRem(ctx, userChannelsKey(userID), channelID.String())

// 	if _, err := pipe.Exec(ctx); err != nil {
// 		return redis.NewError(err, redis.ScopeMember)
// 	}
// 	return nil
// }

// func (c *MemberCache) GetBatchByChannelIDs(
// 	ctx context.Context,
// 	channelIDs []uuid.UUID,
// ) (map[uuid.UUID][]*Member, []uuid.UUID, error) {
// 	if len(channelIDs) == 0 {
// 		return make(map[uuid.UUID][]*Member), nil, nil
// 	}

// 	keys := make([]string, 0, len(channelIDs))
// 	validIDs := make([]uuid.UUID, 0, len(channelIDs))

// 	for _, id := range channelIDs {
// 		if id == uuid.Nil {
// 			continue
// 		}
// 		keys = append(keys, channelMembersKey(id))
// 		validIDs = append(validIDs, id)
// 	}

// 	rawMaps, err := getBatchHashMaps(ctx, redis.ScopeMember, c.client, keys)
// 	if err != nil {
// 		return nil, nil, err
// 	}

// 	found := make(map[uuid.UUID][]*Member, len(validIDs))
// 	var missing []uuid.UUID

// 	for i, rawMap := range rawMaps {
// 		id := validIDs[i]
// 		if len(rawMap) == 0 {
// 			missing = append(missing, id)
// 			continue
// 		}

// 		members := make([]*Member, 0, len(rawMap))
// 		corrupted := false

// 		for _, rawJSON := range rawMap {
// 			m, err := unmarshalMember([]byte(rawJSON))
// 			if err != nil || m == nil {
// 				_ = c.InvalidateChannel(ctx, id)
// 				missing = append(missing, id)
// 				corrupted = true
// 				break
// 			}
// 			members = append(members, m)
// 		}

// 		if !corrupted {
// 			found[id] = members
// 		}
// 	}

// 	return found, missing, nil
// }

// func (c *MemberCache) SetBatchByChannelIDs(
// 	ctx context.Context,
// 	channelMembersMap map[uuid.UUID][]*Member,
// ) error {
// 	if len(channelMembersMap) == 0 {
// 		return nil
// 	}

// 	pipe := c.client.Pipeline()
// 	hasOps := false

// 	for channelID, members := range channelMembersMap {
// 		if channelID == uuid.Nil || len(members) == 0 {
// 			continue
// 		}

// 		memberMap := make(map[string]any, len(members))
// 		for _, m := range members {
// 			if m == nil || m.UserID == uuid.Nil {
// 				continue
// 			}

// 			mBytes, err := marshalMember(m)
// 			if err != nil {
// 				return err
// 			}
// 			memberMap[m.UserID.String()] = mBytes
// 		}

// 		if len(memberMap) > 0 {
// 			pipe.HSet(ctx, channelMembersKey(channelID), memberMap)
// 			hasOps = true
// 		}
// 	}

// 	if !hasOps {
// 		return nil
// 	}

// 	if _, err := pipe.Exec(ctx); err != nil {
// 		return redis.NewError(err, redis.ScopeMember)
// 	}

// 	return nil
// }
