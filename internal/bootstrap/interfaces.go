package bootstrap

import (
	"bonfire-api/internal/channel"
	"bonfire-api/internal/relation"
	"bonfire-api/internal/user"
	"context"

	"github.com/google/uuid"
)

type ChannelRepository interface {
	GetBatch(ctx context.Context, ids []uuid.UUID) ([]*channel.Channel, error)
}

type ChannelMemberRepository interface {
	GetBatchByChannelIDs(ctx context.Context, channelIDs []uuid.UUID) (map[uuid.UUID][]*channel.Member, error)
	ListVisibleByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*channel.Member, error)
}

type RelationRepository interface {
	ListPendingsByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*relation.Relation, error)
	ListFriendsByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*relation.Relation, error)
	ListOutgoingBlocksByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*relation.Relation, error)
}

type UserRepository interface {
	GetBatch(ctx context.Context, ids []uuid.UUID) ([]*user.User, error)
}

// type CachedChannelRepository interface {
// 	GetBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*channel.Channel, error)
// }

// type CachedChannelMemberRepository interface {
// 	GetBatchIDsByChannelIDs(ctx context.Context, channelIDs []uuid.UUID) (map[uuid.UUID][]uuid.UUID, error)
// 	GetVisibleByUserID(ctx context.Context, userID uuid.UUID) (map[uuid.UUID]*channel.Member, error)
// }

// type CachedRelationRepository interface {
// 	GetPendingIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
// 	GetFriendIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
// 	GetOutgoingBlockIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
// }

// type CachedUserRepository interface {
// 	Get(ctx context.Context, id uuid.UUID) (*user.User, error)
// 	GetBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*user.User, error)
// }

// type GatewayCache interface {
// 	GetBatchPresence(ctx context.Context, userIDs []uuid.UUID) (map[uuid.UUID]gateway.Presence, error)
// 	GetPresence(ctx context.Context, userID uuid.UUID) (gateway.Presence, error)
// }

// type TX interface {
// 	ExecTx(ctx context.Context, fn func(txCtx context.Context) error) error
// }
