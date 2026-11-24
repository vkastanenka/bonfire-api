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
	ListPendingsAndFriendsByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*relation.Relation, error)
	ListOutgoingBlocksByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*relation.Relation, error)
}

type UserRepository interface {
	GetBatch(ctx context.Context, ids []uuid.UUID) ([]*user.User, error)
}
