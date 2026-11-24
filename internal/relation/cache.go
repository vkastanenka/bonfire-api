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
	userPendingsTTL = 24 * time.Hour
	userFriendsTTL  = 24 * time.Hour
	userBlocksTTL   = 24 * time.Hour
)

func userIncomingPendingsKey(id uuid.UUID) string {
	return "user:" + "{" + id.String() + "}" + ":incoming_pendings"
}

func userOutgoingPendingsKey(id uuid.UUID) string {
	return "user:" + "{" + id.String() + "}" + ":outgoing_pendings"
}

func userFriendsKey(id uuid.UUID) string {
	return "user:" + "{" + id.String() + "}" + ":friends"
}

func userIncomingBlocksKey(id uuid.UUID) string {
	return "user:" + "{" + id.String() + "}" + ":incoming_blocks"
}

func userOutgoingBlocksKey(id uuid.UUID) string {
	return "user:" + "{" + id.String() + "}" + ":outgoing_blocks"
}

type Cache struct {
	client redisdriver.Cmdable
}

func NewCache(client redisdriver.Cmdable) *Cache {
	return &Cache{client: client}
}

func (c *Cache) GetIncomingPendingIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	return c.getIDs(ctx, userID, userIncomingPendingsKey)
}

func (c *Cache) SetIncomingPendingIDs(ctx context.Context, userID uuid.UUID, pendingIDs []uuid.UUID) error {
	return c.setIDs(ctx, userID, pendingIDs, userIncomingPendingsKey, userPendingsTTL)
}

func (c *Cache) GetOutgoingPendingIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	return c.getIDs(ctx, userID, userOutgoingPendingsKey)
}

func (c *Cache) SetOutgoingPendingIDs(ctx context.Context, userID uuid.UUID, pendingIDs []uuid.UUID) error {
	return c.setIDs(ctx, userID, pendingIDs, userOutgoingPendingsKey, userPendingsTTL)
}

func (c *Cache) GetFriendIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	return c.getIDs(ctx, userID, userFriendsKey)
}

func (c *Cache) SetFriendIDs(ctx context.Context, userID uuid.UUID, friendIDs []uuid.UUID) error {
	return c.setIDs(ctx, userID, friendIDs, userFriendsKey, userFriendsTTL)
}

func (c *Cache) GetIncomingBlockIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	return c.getIDs(ctx, userID, userIncomingBlocksKey)
}

func (c *Cache) SetIncomingBlockIDs(ctx context.Context, userID uuid.UUID, blockIDs []uuid.UUID) error {
	return c.setIDs(ctx, userID, blockIDs, userIncomingBlocksKey, userBlocksTTL)
}

func (c *Cache) GetOutgoingBlockIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	return c.getIDs(ctx, userID, userOutgoingBlocksKey)
}

func (c *Cache) SetOutgoingBlockIDs(ctx context.Context, userID uuid.UUID, blockIDs []uuid.UUID) error {
	return c.setIDs(ctx, userID, blockIDs, userOutgoingBlocksKey, userBlocksTTL)
}

func (c *Cache) DeletePendingPair(ctx context.Context, actorID, peerID uuid.UUID) error {
	return c.deleteKeys(ctx, userOutgoingPendingsKey(actorID), userIncomingPendingsKey(peerID))
}

func (c *Cache) DeleteFriendsPair(ctx context.Context, actorID, peerID uuid.UUID) error {
	return c.deleteKeys(ctx, userFriendsKey(actorID), userFriendsKey(peerID))
}

func (c *Cache) HandleNewFriends(
	ctx context.Context,
	actorID, peerID uuid.UUID,
	ch *channel.Channel,
) error {
	keys := []string{
		userOutgoingPendingsKey(actorID),
		userIncomingPendingsKey(peerID),
		userFriendsKey(actorID),
		userFriendsKey(peerID),
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
	return c.deleteKeys(ctx, userOutgoingBlocksKey(actorID), userIncomingBlocksKey(peerID))
}

func (c *Cache) DeletePendingBlocksPair(ctx context.Context, actorID, peerID uuid.UUID) error {
	return c.deleteKeys(
		ctx,
		userOutgoingPendingsKey(actorID),
		userIncomingPendingsKey(peerID),
		userOutgoingBlocksKey(actorID),
		userIncomingBlocksKey(peerID),
	)
}

func (c *Cache) DeleteFriendBlocksPair(ctx context.Context, actorID, peerID uuid.UUID) error {
	return c.deleteKeys(
		ctx,
		userFriendsKey(actorID),
		userFriendsKey(peerID),
		userOutgoingBlocksKey(actorID),
		userIncomingBlocksKey(peerID),
	)
}

func (c *Cache) getIDs(
	ctx context.Context,
	userID uuid.UUID,
	keyFn func(uuid.UUID) string,
) ([]uuid.UUID, error) {
	return redis.GetSetIDs(ctx, redis.ResourceRelation, c.client, keyFn(userID))
}

func (c *Cache) setIDs(
	ctx context.Context,
	userID uuid.UUID,
	ids []uuid.UUID,
	keyFn func(uuid.UUID) string,
	ttl time.Duration,
) error {
	return redis.SetSetIDs(ctx, redis.ResourceRelation, c.client, keyFn(userID), ids, ttl)
}

func (c *Cache) deleteKeys(ctx context.Context, keys ...string) error {
	return redis.DeleteSetPipelined(ctx, redis.ResourceRelation, c.client, keys)
}
