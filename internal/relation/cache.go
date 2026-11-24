package relation

import (
	"bonfire-api/internal/channel"
	"bonfire-api/internal/redis"
	"context"
	"time"

	"github.com/google/uuid"
	redisdriver "github.com/redis/go-redis/v9"
)

var (
	UserPendingsTTL = 24 * time.Hour
	UserFriendsTTL  = 24 * time.Hour
	UserBlocksTTL   = 24 * time.Hour
)

func UserIncomingPendingsKey(id uuid.UUID) string {
	return "user:" + "{" + id.String() + "}" + ":incoming_pendings"
}

func UserOutgoingPendingsKey(id uuid.UUID) string {
	return "user:" + "{" + id.String() + "}" + ":outgoing_pendings"
}

func UserFriendsKey(id uuid.UUID) string {
	return "user:" + "{" + id.String() + "}" + ":friends"
}

func UserIncomingBlocksKey(id uuid.UUID) string {
	return "user:" + "{" + id.String() + "}" + ":incoming_blocks"
}

func UserOutgoingBlocksKey(id uuid.UUID) string {
	return "user:" + "{" + id.String() + "}" + ":outgoing_blocks"
}

type Cache struct {
	client redisdriver.Cmdable
}

func NewCache(client redisdriver.Cmdable) *Cache {
	return &Cache{client: client}
}

func (c *Cache) GetIncomingPendingIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	return c.getIDs(ctx, userID, UserIncomingPendingsKey)
}

func (c *Cache) SetIncomingPendingIDs(ctx context.Context, userID uuid.UUID, pendingIDs []uuid.UUID) error {
	return c.setIDs(ctx, userID, pendingIDs, UserIncomingPendingsKey, UserPendingsTTL)
}

func (c *Cache) GetOutgoingPendingIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	return c.getIDs(ctx, userID, UserOutgoingPendingsKey)
}

func (c *Cache) SetOutgoingPendingIDs(ctx context.Context, userID uuid.UUID, pendingIDs []uuid.UUID) error {
	return c.setIDs(ctx, userID, pendingIDs, UserOutgoingPendingsKey, UserPendingsTTL)
}

func (c *Cache) GetFriendIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	return c.getIDs(ctx, userID, UserFriendsKey)
}

func (c *Cache) SetFriendIDs(ctx context.Context, userID uuid.UUID, friendIDs []uuid.UUID) error {
	return c.setIDs(ctx, userID, friendIDs, UserFriendsKey, UserFriendsTTL)
}

func (c *Cache) GetIncomingBlockIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	return c.getIDs(ctx, userID, UserIncomingBlocksKey)
}

func (c *Cache) SetIncomingBlockIDs(ctx context.Context, userID uuid.UUID, blockIDs []uuid.UUID) error {
	return c.setIDs(ctx, userID, blockIDs, UserIncomingBlocksKey, UserBlocksTTL)
}

func (c *Cache) GetOutgoingBlockIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	return c.getIDs(ctx, userID, UserOutgoingBlocksKey)
}

func (c *Cache) SetOutgoingBlockIDs(ctx context.Context, userID uuid.UUID, blockIDs []uuid.UUID) error {
	return c.setIDs(ctx, userID, blockIDs, UserOutgoingBlocksKey, UserBlocksTTL)
}

func (c *Cache) DeletePendingPair(ctx context.Context, actorID, peerID uuid.UUID) error {
	return c.deleteKeys(ctx, UserOutgoingPendingsKey(actorID), UserIncomingPendingsKey(peerID))
}

func (c *Cache) DeleteFriendsPair(ctx context.Context, actorID, peerID uuid.UUID) error {
	return c.deleteKeys(ctx, UserFriendsKey(actorID), UserFriendsKey(peerID))
}

func (c *Cache) HandleNewFriends(
	ctx context.Context,
	actorID, peerID uuid.UUID,
	ch *channel.Channel,
) error {
	keys := []string{
		UserOutgoingPendingsKey(actorID),
		UserIncomingPendingsKey(peerID),
		UserFriendsKey(actorID),
		UserFriendsKey(peerID),
		channel.UserChannelMembersKey(actorID),
		channel.UserChannelMembersKey(peerID),
	}

	var (
		chKey   string
		chBytes []byte
		err     error
	)
	if ch != nil {
		chKey = channel.ChannelKey(ch.ID)
		chBytes, err = channel.MarshalChannel(ch)
		if err != nil {
			return redis.ErrResourceMarshalFailed().
				Meta("resource", redis.ResourceChannel.String()).
				Meta("key", chKey).
				Wrap(err)
		}
	}

	pipe := c.client.Pipeline()
	pipe.Del(ctx, keys...)
	if len(chBytes) > 0 {
		pipe.Set(ctx, chKey, chBytes, channel.ChannelTTL)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return redis.NewError(err, redis.ResourceRelation)
	}

	return nil
}

func (c *Cache) DeleteBlocksPair(ctx context.Context, actorID, peerID uuid.UUID) error {
	return c.deleteKeys(ctx, UserOutgoingBlocksKey(actorID), UserIncomingBlocksKey(peerID))
}

func (c *Cache) DeletePendingBlocksPair(ctx context.Context, actorID, peerID uuid.UUID) error {
	return c.deleteKeys(
		ctx,
		UserOutgoingPendingsKey(actorID),
		UserIncomingPendingsKey(peerID),
		UserOutgoingBlocksKey(actorID),
		UserIncomingBlocksKey(peerID),
	)
}

func (c *Cache) DeleteFriendBlocksPair(ctx context.Context, actorID, peerID uuid.UUID) error {
	return c.deleteKeys(
		ctx,
		UserFriendsKey(actorID),
		UserFriendsKey(peerID),
		UserOutgoingBlocksKey(actorID),
		UserIncomingBlocksKey(peerID),
	)
}

func (c *Cache) getIDs(
	ctx context.Context,
	userID uuid.UUID,
	keyFn func(uuid.UUID) string,
) ([]uuid.UUID, error) {
	return redis.GetSetIDs(ctx, c.client, redis.ResourceRelation, keyFn(userID))
}

func (c *Cache) setIDs(
	ctx context.Context,
	userID uuid.UUID,
	ids []uuid.UUID,
	keyFn func(uuid.UUID) string,
	ttl time.Duration,
) error {
	return redis.SetSetIDs(ctx, c.client, redis.ResourceRelation, keyFn(userID), ids, ttl)
}

func (c *Cache) deleteKeys(ctx context.Context, keys ...string) error {
	return redis.DeleteSetPipelined(ctx, c.client, redis.ResourceRelation, keys)
}
