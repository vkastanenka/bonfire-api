package relation

import (
	"bonfire-api/internal/channel"
	"bonfire-api/internal/gateway"
	"bonfire-api/internal/user"
	"context"

	"github.com/google/uuid"
)

type Broadcaster interface {
	ToUser(ctx context.Context, recipientID uuid.UUID, messageType string, payload any, excludeSessionIDs ...uuid.UUID) error
	ToUserSession(ctx context.Context, userID uuid.UUID, sessionID uuid.UUID, messageType string, payload any) error
	ToUserSessions(ctx context.Context, userID uuid.UUID, sessionIDs []uuid.UUID, messageType string, payload any) error
	ToUsers(ctx context.Context, recipientIDs []uuid.UUID, messageType string, payload any, excludeSessionIDs ...uuid.UUID) error
}

type GatewayCache interface {
	GetBatchPresence(ctx context.Context, userIDs []uuid.UUID) (map[uuid.UUID]gateway.Presence, error)
	GetPresence(ctx context.Context, userID uuid.UUID) (gateway.Presence, error)
}

type ChannelRepository interface {
	Create(ctx context.Context, ch *channel.Channel) (*channel.Channel, error)
}

type MemberRepository interface {
	CreateBatch(ctx context.Context, members []*channel.Member) ([]*channel.Member, error)
}

type UserRepository interface {
	Get(ctx context.Context, id uuid.UUID) (*user.User, error)
	GetBatch(ctx context.Context, ids []uuid.UUID) ([]*user.User, error)
}

type CachedUserRepository interface {
	Get(ctx context.Context, id uuid.UUID) (*user.User, error)
	GetBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*user.User, error)
}

type TX interface {
	ExecTx(ctx context.Context, fn func(txCtx context.Context) error) error
}
