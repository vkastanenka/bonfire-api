package channel

import (
	"bonfire-api/internal/redis"
	"context"
	"time"

	"github.com/google/uuid"
	redisdriver "github.com/redis/go-redis/v9"
)

const (
	ChannelTTL = 24 * time.Hour
)

func ChannelKey(channelID uuid.UUID) string {
	return "channel:" + "{" + channelID.String() + "}"
}

type ChannelCache struct {
	client redisdriver.Cmdable
}

func NewChannelCache(client redisdriver.Cmdable) *ChannelCache {
	return &ChannelCache{
		client: client,
	}
}

func (c *ChannelCache) Get(ctx context.Context, id uuid.UUID) (*Channel, error) {
	return redis.GetAndUnmarshal(ctx, c.client, redis.ResourceChannel, ChannelKey(id), UnmarshalChannel)
}

func (c *ChannelCache) Set(ctx context.Context, ch *Channel) error {
	return redis.MarshalAndSet(ctx, c.client, redis.ResourceChannel, ChannelKey(ch.ID), ch, ChannelTTL, MarshalChannel)
}

func (c *ChannelCache) Delete(ctx context.Context, id uuid.UUID) error {
	return redis.Delete(ctx, c.client, redis.ResourceChannel, ChannelKey(id))
}

func (c *ChannelCache) GetBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*Channel, []uuid.UUID, error) {
	return redis.GetAndUnmarshalBatch(ctx, c.client, redis.ResourceChannel, ChannelKey, ids, UnmarshalChannel)
}

func (c *ChannelCache) SetBatch(ctx context.Context, channels map[uuid.UUID]*Channel) error {
	return redis.MarshalAndSetBatch(ctx, c.client, redis.ResourceChannel, ChannelKey, channels, ChannelTTL, MarshalChannel)
}

// func (c *ChannelCache) CreateGroup(ctx context.Context, ch *Channel, members []*Member) error {
// 	chBytes, err := MarshalChannel(ch)
// 	if err != nil {
// 		return err
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

// 	pipe := c.client.Pipeline()

// 	pipe.Set(ctx, ChannelKey(ch.ID), chBytes, ChannelTTL)

// 	if len(memberMap) > 0 {
// 		pipe.HSet(ctx, channelMembersKey(ch.ID), memberMap)
// 	}

// 	channelIDStr := ch.ID.String()
// 	for _, m := range members {
// 		if m == nil || m.UserID == uuid.Nil {
// 			continue
// 		}

// 		var score float64
// 		if m.PinnedAt != nil && !m.PinnedAt.IsZero() {
// 			score = 1e12 + float64(m.PinnedAt.Unix())
// 		} else {
// 			score = float64(ch.CreatedAt.Unix())
// 		}

// 		pipe.ZAdd(ctx, userChannelsKey(m.UserID), redisdriver.Z{
// 			Score:  score,
// 			Member: channelIDStr,
// 		})
// 	}

// 	if _, err := pipe.Exec(ctx); err != nil {
// 		return redis.NewError(err, redis.ResourceChannel)
// 	}

// 	return nil
// }

// func (c *ChannelCache) DeleteGroup(ctx context.Context, channelID uuid.UUID, memberIDs []uuid.UUID) error {
// 	pipe := c.client.Pipeline()

// 	pipe.Del(ctx, ChannelKey(channelID))
// 	pipe.Del(ctx, channelMembersKey(channelID))
// 	pipe.Del(ctx, channelMessagesKey(channelID))

// 	channelIDStr := channelID.String()
// 	for _, userID := range memberIDs {
// 		if userID != uuid.Nil {
// 			pipe.ZRem(ctx, userChannelsKey(userID), channelIDStr)
// 		}
// 	}

// 	if _, err := pipe.Exec(ctx); err != nil {
// 		return redis.NewError(err, redis.ResourceChannel)
// 	}

// 	return nil
// }

// func (c *ChannelCache) GetUserChannelIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, bool, error) {
// 	if userID == uuid.Nil {
// 		return nil, false, nil
// 	}

// 	rawIDs, err := c.client.ZRevRange(ctx, userChannelsKey(userID), 0, -1).Result()
// 	if err != nil {
// 		if redis.IsCacheMiss(err) {
// 			return nil, false, nil
// 		}
// 		return nil, false, redis.NewError(err, redis.ScopeUser)
// 	}

// 	if len(rawIDs) == 0 {
// 		return nil, false, nil
// 	}

// 	ids := make([]uuid.UUID, 0, len(rawIDs))
// 	for _, raw := range rawIDs {
// 		id, err := uuid.Parse(raw)
// 		if err == nil {
// 			ids = append(ids, id)
// 		}
// 	}

// 	return ids, true, nil
// }

// func (c *ChannelCache) SetUserChannelIDs(ctx context.Context, userID uuid.UUID, members []*Member) error {
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

// 	if _, err := pipe.Exec(ctx); err != nil {
// 		return redis.NewError(err, redis.ScopeUser)
// 	}
// 	return nil
// }

// func (c *ChannelCache) RemoveUserChannelID(ctx context.Context, userID, channelID uuid.UUID) error {
// 	if err := c.client.ZRem(ctx, userChannelsKey(userID), channelID.String()).Err(); err != nil {
// 		return redis.NewError(err, redis.ScopeUser)
// 	}

// 	return nil
// }
