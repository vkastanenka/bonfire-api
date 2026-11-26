package relation

import (
	"bonfire-api/internal/channel"
	"bonfire-api/internal/gateway"
	"bonfire-api/internal/user"
	"context"
	"time"

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
	Get(ctx context.Context, id uuid.UUID) (*channel.Channel, error)
}

type MemberRepository interface {
	CreateBatch(ctx context.Context, members []*channel.Member) ([]*channel.Member, error)
	Get(ctx context.Context, channelID, userID uuid.UUID) (*channel.Member, error)
	GetBatchByChannelID(ctx context.Context, channelID uuid.UUID) ([]*channel.Member, error)
	IncrementPeersMentionCountByChannelID(
		ctx context.Context,
		channelID, actorID uuid.UUID,
		incrementAmount int,
		updatedAt time.Time,
	) ([]*channel.Member, error)
}

type UserRepository interface {
	Get(ctx context.Context, id uuid.UUID) (*user.User, error)
	GetBatch(ctx context.Context, ids []uuid.UUID) ([]*user.User, error)
	GetByUsername(ctx context.Context, username string) (*user.User, error)
}

type CachedUserRepository interface {
	Get(ctx context.Context, id uuid.UUID) (*user.User, error)
	GetBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*user.User, error)
}

type TX interface {
	ExecTx(ctx context.Context, fn func(txCtx context.Context) error) error
}
