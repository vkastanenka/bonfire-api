package relation

import (
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
func userFriendsKey(id uuid.UUID) string { return "user:" + "{" + id.String() + "}" + ":friends" }
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

// // ============================================================================
// // Incoming Pendings (Requests received by userID)
// // ============================================================================

// func (c *Cache) GetIncomingPendingIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
// 	return getSetIDs(ctx, redis.ResourceRelation, c.client, userIncomingPendingsKey(userID))
// }

// func (c *Cache) SetIncomingPendingIDs(ctx context.Context, userID uuid.UUID, pendingIDs []uuid.UUID) error {
// 	return setSetIDs(ctx, redis.ResourceRelation, c.client, userIncomingPendingsKey(userID), pendingIDs, userPendingsTTL)
// }

// func (c *Cache) AddIncomingPendingID(ctx context.Context, userID uuid.UUID, senderID uuid.UUID) error {
// 	return addToSetIDs(ctx, redis.ResourceRelation, c.client, userIncomingPendingsKey(userID), userPendingsTTL, senderID)
// }

// func (c *Cache) RemoveIncomingPendingID(ctx context.Context, userID uuid.UUID, senderID uuid.UUID) error {
// 	return removeFromSetIDs(ctx, redis.ResourceRelation, c.client, userIncomingPendingsKey(userID), senderID)
// }

// func (c *Cache) DeleteIncomingPendingsIndex(ctx context.Context, userID uuid.UUID) error {
// 	return deleteSet(ctx, redis.ResourceRelation, c.client, userIncomingPendingsKey(userID))
// }

// // ============================================================================
// // Outgoing Pendings (Requests sent by userID)
// // ============================================================================

// func (c *Cache) GetOutgoingPendingIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
// 	return getSetIDs(ctx, redis.ScopeRelation, c.client, userOutgoingPendingsKey(userID))
// }

// func (c *Cache) SetOutgoingPendingIDs(ctx context.Context, userID uuid.UUID, pendingIDs []uuid.UUID) error {
// 	return setSetIDs(ctx, redis.ScopeRelation, c.client, userOutgoingPendingsKey(userID), pendingIDs, userPendingsTTL)
// }

// func (c *Cache) AddOutgoingPendingID(ctx context.Context, userID uuid.UUID, targetID uuid.UUID) error {
// 	return addToSetIDs(ctx, redis.ScopeRelation, c.client, userOutgoingPendingsKey(userID), userPendingsTTL, targetID)
// }

// func (c *Cache) RemoveOutgoingPendingID(ctx context.Context, userID uuid.UUID, targetID uuid.UUID) error {
// 	return removeFromSetIDs(ctx, redis.ScopeRelation, c.client, userOutgoingPendingsKey(userID), targetID)
// }

// func (c *Cache) DeleteOutgoingPendingsIndex(ctx context.Context, userID uuid.UUID) error {
// 	return deleteSet(ctx, redis.ScopeRelation, c.client, userOutgoingPendingsKey(userID))
// }

// // ============================================================================
// // Friends
// // ============================================================================

// func (c *Cache) GetFriendIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
// 	return getSetIDs(ctx, redis.ScopeRelation, c.client, userFriendsKey(userID))
// }

// func (c *Cache) SetFriendIDs(ctx context.Context, userID uuid.UUID, friendIDs []uuid.UUID) error {
// 	return setSetIDs(ctx, redis.ScopeRelation, c.client, userFriendsKey(userID), friendIDs, userFriendsTTL)
// }

// func (c *Cache) AddFriendID(ctx context.Context, userID uuid.UUID, friendID uuid.UUID) error {
// 	return addToSetIDs(ctx, redis.ScopeRelation, c.client, userFriendsKey(userID), userFriendsTTL, friendID)
// }

// func (c *Cache) RemoveFriendID(ctx context.Context, userID uuid.UUID, friendID uuid.UUID) error {
// 	return removeFromSetIDs(ctx, redis.ScopeRelation, c.client, userFriendsKey(userID), friendID)
// }

// func (c *Cache) DeleteFriendsIndex(ctx context.Context, userID uuid.UUID) error {
// 	return deleteSet(ctx, redis.ScopeRelation, c.client, userFriendsKey(userID))
// }

// // ============================================================================
// // Incoming Blocks (Users who have blocked userID)
// // ============================================================================

// func (c *Cache) GetIncomingBlockIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
// 	return getSetIDs(ctx, redis.ScopeRelation, c.client, userIncomingBlocksKey(userID))
// }

// func (c *Cache) SetIncomingBlockIDs(ctx context.Context, userID uuid.UUID, blockIDs []uuid.UUID) error {
// 	return setSetIDs(ctx, redis.ScopeRelation, c.client, userIncomingBlocksKey(userID), blockIDs, userBlocksTTL)
// }

// func (c *Cache) AddIncomingBlockID(ctx context.Context, userID uuid.UUID, blockerUserID uuid.UUID) error {
// 	return addToSetIDs(ctx, redis.ScopeRelation, c.client, userIncomingBlocksKey(userID), userBlocksTTL, blockerUserID)
// }

// func (c *Cache) RemoveIncomingBlockID(ctx context.Context, userID uuid.UUID, blockerUserID uuid.UUID) error {
// 	return removeFromSetIDs(ctx, redis.ScopeRelation, c.client, userIncomingBlocksKey(userID), blockerUserID)
// }

// func (c *Cache) DeleteIncomingBlocksIndex(ctx context.Context, userID uuid.UUID) error {
// 	return deleteSet(ctx, redis.ScopeRelation, c.client, userIncomingBlocksKey(userID))
// }

// // ============================================================================
// // Outgoing Blocks (Users blocked by userID)
// // ============================================================================

// func (c *Cache) GetOutgoingBlockIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
// 	return getSetIDs(ctx, redis.ScopeRelation, c.client, userOutgoingBlocksKey(userID))
// }

// func (c *Cache) SetOutgoingBlockIDs(ctx context.Context, userID uuid.UUID, blockIDs []uuid.UUID) error {
// 	return setSetIDs(ctx, redis.ScopeRelation, c.client, userOutgoingBlocksKey(userID), blockIDs, userBlocksTTL)
// }

// func (c *Cache) AddOutgoingBlockID(ctx context.Context, userID uuid.UUID, blockedUserID uuid.UUID) error {
// 	return addToSetIDs(ctx, redis.ScopeRelation, c.client, userOutgoingBlocksKey(userID), userBlocksTTL, blockedUserID)
// }

// func (c *Cache) RemoveOutgoingBlockID(ctx context.Context, userID uuid.UUID, blockedUserID uuid.UUID) error {
// 	return removeFromSetIDs(ctx, redis.ScopeRelation, c.client, userOutgoingBlocksKey(userID), blockedUserID)
// }

// func (c *Cache) DeleteOutgoingBlocksIndex(ctx context.Context, userID uuid.UUID) error {
// 	return deleteSet(ctx, redis.ScopeRelation, c.client, userOutgoingBlocksKey(userID))
// }

// // ============================================================================
// // Multi-Key Composite Operations
// // ============================================================================

func (c *Cache) DeletePendingPair(ctx context.Context, actorID, peerID uuid.UUID) error {
	return c.deleteKeys(ctx, userOutgoingPendingsKey(actorID), userIncomingPendingsKey(peerID))
}

func (c *Cache) DeleteFriendsPair(ctx context.Context, actorID, peerID uuid.UUID) error {
	return c.deleteKeys(ctx, userFriendsKey(actorID), userFriendsKey(peerID))
}

func (c *Cache) DeleteNewFriendsPair(ctx context.Context, actorID, peerID uuid.UUID) error {
	return c.deleteKeys(
		ctx,
		userOutgoingPendingsKey(actorID),
		userIncomingPendingsKey(peerID),
		userFriendsKey(actorID),
		userFriendsKey(peerID),
	)
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

func (c *Cache) deleteKeys(ctx context.Context, keys ...string) error {
	return redis.DeleteSetPipelined(ctx, redis.ResourceRelation, c.client, keys)
}

// func (c *Cache) AddPendingPair(ctx context.Context, actorID, peerID uuid.UUID) error {
// 	additions := map[string][]uuid.UUID{
// 		userOutgoingPendingsKey(actorID): {peerID},
// 		userIncomingPendingsKey(peerID):  {actorID},
// 	}
// 	return addToSetIDsPipelined(ctx, redis.ScopeRelation, c.client, additions, userPendingsTTL)
// }

// func (c *Cache) RemovePendingPair(ctx context.Context, actorID, peerID uuid.UUID) error {
// 	removals := map[string][]uuid.UUID{
// 		userOutgoingPendingsKey(actorID): {peerID},
// 		userIncomingPendingsKey(peerID):  {actorID},
// 	}
// 	return removeFromSetIDsPipelined(ctx, redis.ScopeRelation, c.client, removals)
// }

// func (c *Cache) AddFriendPair(ctx context.Context, userA, userB uuid.UUID) error {
// 	additions := map[string][]uuid.UUID{
// 		userFriendsKey(userA): {userB},
// 		userFriendsKey(userB): {userA},
// 	}
// 	return addToSetIDsPipelined(ctx, redis.ScopeRelation, c.client, additions, userFriendsTTL)
// }

// func (c *Cache) RemoveFriendPair(ctx context.Context, userA, userB uuid.UUID) error {
// 	removals := map[string][]uuid.UUID{
// 		userFriendsKey(userA): {userB},
// 		userFriendsKey(userB): {userA},
// 	}
// 	return removeFromSetIDsPipelined(ctx, redis.ScopeRelation, c.client, removals)
// }

// func (c *Cache) BlockUser(ctx context.Context, blockerID, targetID uuid.UUID) error {
// 	additions := map[string][]uuid.UUID{
// 		userOutgoingBlocksKey(blockerID): {targetID},
// 		userIncomingBlocksKey(targetID):  {blockerID},
// 	}
// 	return addToSetIDsPipelined(ctx, redis.ScopeRelation, c.client, additions, userBlocksTTL)
// }

// func (c *Cache) UnblockUser(ctx context.Context, blockerID, targetID uuid.UUID) error {
// 	removals := map[string][]uuid.UUID{
// 		userOutgoingBlocksKey(blockerID): {targetID},
// 		userIncomingBlocksKey(targetID):  {blockerID},
// 	}
// 	return removeFromSetIDsPipelined(ctx, redis.ScopeRelation, c.client, removals)
// }
