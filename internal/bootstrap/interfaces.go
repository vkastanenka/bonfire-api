package bootstrap

import (
	"bonfire-api/internal/channel"
	"bonfire-api/internal/gateway"
	"bonfire-api/internal/user"
	"context"

	"github.com/google/uuid"
)

type CachedChannelRepository interface {
	GetBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*channel.Channel, error)
}

type CachedChannelMemberRepository interface {
	GetBatchIDsByChannelIDs(ctx context.Context, channelIDs []uuid.UUID) (map[uuid.UUID][]uuid.UUID, error)
	GetVisibleByUserID(ctx context.Context, userID uuid.UUID) (map[uuid.UUID]*channel.Member, error)
}

type CachedRelationRepository interface {
	GetFriendIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
	GetIncomingPendingIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
}

type CachedUserRepository interface {
	Get(ctx context.Context, id uuid.UUID) (*user.User, error)
	GetBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*user.User, error)
}

type GatewayCache interface {
	GetBatchPresence(ctx context.Context, userIDs []uuid.UUID) (map[uuid.UUID]gateway.Presence, error)
	GetPresence(ctx context.Context, userID uuid.UUID) (gateway.Presence, error)
}

// type TX interface {
// 	ExecTx(ctx context.Context, fn func(txCtx context.Context) error) error
// }
