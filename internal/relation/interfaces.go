package relation

import (
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

// type Cache interface {
// 	AddFriendID(ctx context.Context, userID uuid.UUID, friendID uuid.UUID) error
// 	AddFriendPair(ctx context.Context, userA uuid.UUID, userB uuid.UUID) error
// 	AddIncomingBlockID(ctx context.Context, userID uuid.UUID, blockerUserID uuid.UUID) error
// 	AddIncomingPendingID(ctx context.Context, userID uuid.UUID, senderID uuid.UUID) error
// 	AddOutgoingBlockID(ctx context.Context, userID uuid.UUID, blockedUserID uuid.UUID) error
// 	AddOutgoingPendingID(ctx context.Context, userID uuid.UUID, targetID uuid.UUID) error
// 	AddPendingPair(ctx context.Context, actorID uuid.UUID, peerID uuid.UUID) error
// 	BlockUser(ctx context.Context, blockerID uuid.UUID, targetID uuid.UUID) error
// 	DeleteBlocksPair(ctx context.Context, blockerID uuid.UUID, targetID uuid.UUID) error
// 	DeleteFriendsIndex(ctx context.Context, userID uuid.UUID) error
// 	DeleteFriendsPair(ctx context.Context, userA uuid.UUID, userB uuid.UUID) error
// 	DeleteIncomingBlocksIndex(ctx context.Context, userID uuid.UUID) error
// 	DeleteIncomingPendingsIndex(ctx context.Context, userID uuid.UUID) error
// 	DeleteOutgoingBlocksIndex(ctx context.Context, userID uuid.UUID) error
// 	DeleteOutgoingPendingsIndex(ctx context.Context, userID uuid.UUID) error
// 	DeletePendingPair(ctx context.Context, actorID uuid.UUID, peerID uuid.UUID) error
// 	GetFriendIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
// 	GetIncomingBlockIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
// 	GetIncomingPendingIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
// 	GetOutgoingBlockIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
// 	GetOutgoingPendingIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
// 	RemoveFriendID(ctx context.Context, userID uuid.UUID, friendID uuid.UUID) error
// 	RemoveFriendPair(ctx context.Context, userA uuid.UUID, userB uuid.UUID) error
// 	RemoveIncomingBlockID(ctx context.Context, userID uuid.UUID, blockerUserID uuid.UUID) error
// 	RemoveIncomingPendingID(ctx context.Context, userID uuid.UUID, senderID uuid.UUID) error
// 	RemoveOutgoingBlockID(ctx context.Context, userID uuid.UUID, blockedUserID uuid.UUID) error
// 	RemoveOutgoingPendingID(ctx context.Context, userID uuid.UUID, targetID uuid.UUID) error
// 	RemovePendingPair(ctx context.Context, actorID uuid.UUID, peerID uuid.UUID) error
// 	SetFriendIDs(ctx context.Context, userID uuid.UUID, friendIDs []uuid.UUID) error
// 	SetIncomingBlockIDs(ctx context.Context, userID uuid.UUID, blockIDs []uuid.UUID) error
// 	SetIncomingPendingIDs(ctx context.Context, userID uuid.UUID, pendingIDs []uuid.UUID) error
// 	SetOutgoingBlockIDs(ctx context.Context, userID uuid.UUID, blockIDs []uuid.UUID) error
// 	SetOutgoingPendingIDs(ctx context.Context, userID uuid.UUID, pendingIDs []uuid.UUID) error
// 	UnblockUser(ctx context.Context, blockerID uuid.UUID, targetID uuid.UUID) error
// }

// type PresenceCache interface {
// 	Delete(ctx context.Context, id uuid.UUID) error
// 	Get(ctx context.Context, userID uuid.UUID) (presence.Presence, error)
// 	GetBatch(ctx context.Context, userIDs []uuid.UUID) (map[uuid.UUID]presence.Presence, error)
// 	Set(ctx context.Context, userID uuid.UUID, p presence.Presence) error
// }

// type UserCache interface {
// 	Delete(ctx context.Context, id uuid.UUID) error
// 	DeleteBatch(ctx context.Context, ids []uuid.UUID) error
// 	Get(ctx context.Context, id uuid.UUID) (*user.User, error)
// 	GetBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*user.User, []uuid.UUID, error)
// 	Set(ctx context.Context, usr *user.User) error
// 	SetBatch(ctx context.Context, users map[uuid.UUID]*user.User) error
// }

// type Repository interface {
// 	DeleteByUserID(ctx context.Context, user1ID uuid.UUID, user2ID uuid.UUID, actorID uuid.UUID) error
// 	Get(ctx context.Context, user1ID uuid.UUID, user2ID uuid.UUID) (*Relation, error)
// 	GetForUpdate(ctx context.Context, user1ID uuid.UUID, user2ID uuid.UUID) (*Relation, error)
// 	HasIncomingBlock(ctx context.Context, actorID uuid.UUID, peerIDs []uuid.UUID) error
// 	ListFriendsByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*Relation, error)
// 	ListIncomingBlocksByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*Relation, error)
// 	ListIncomingPendingsByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*Relation, error)
// 	ListOutgoingBlocksByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*Relation, error)
// 	ListOutgoingPendingsByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*Relation, error)
// 	ListTypeByUserID(ctx context.Context, userID uuid.UUID, relType Type, limit int) ([]*Relation, error)
// 	Save(ctx context.Context, rel *Relation) (*Relation, error)
// }

// type CachedRepository interface {
// 	GetFriendIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
// 	GetIncomingBlockIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
// 	GetIncomingPendingIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
// 	GetOutgoingBlockIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
// 	GetOutgoingPendingIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
// }

// type ChannelRepository interface {
// 	Create(ctx context.Context, ch *channel.Channel) (*channel.Channel, error)
// 	Delete(ctx context.Context, id uuid.UUID) error
// 	Get(ctx context.Context, id uuid.UUID) (*channel.Channel, error)
// 	GetBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*channel.Channel, error)
// 	GetForUpdate(ctx context.Context, id uuid.UUID) (*channel.Channel, error)
// 	UpdateGroup(ctx context.Context, id uuid.UUID, name *string, iconURL *string, updatedAt time.Time) (*channel.Channel, error)
// 	UpdateLastMessage(ctx context.Context, id uuid.UUID, lastMessageID *uuid.UUID, lastMessageAt *time.Time, updatedAt time.Time) (*channel.Channel, error)
// }

// type CachedChannelRepository interface {
// 	Get(ctx context.Context, id uuid.UUID) (*channel.Channel, error)
// 	GetBatch(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*channel.Channel, error)
// }

// type MemberRepository interface {
// 	CountByChannelID(ctx context.Context, channelID uuid.UUID) (int, error)
// 	CreateBatch(ctx context.Context, members []*channel.Member) ([]*channel.Member, error)
// 	Delete(ctx context.Context, channelID uuid.UUID, userID uuid.UUID) error
// 	Get(ctx context.Context, channelID uuid.UUID, userID uuid.UUID) (*channel.Member, error)
// 	GetBatchByChannelID(ctx context.Context, channelID uuid.UUID) ([]*channel.Member, error)
// 	GetBatchByChannelIDs(ctx context.Context, channelIDs []uuid.UUID) (map[uuid.UUID][]*channel.Member, error)
// 	ListVisibleByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*channel.Member, error)
// 	UpdateIsVisible(ctx context.Context, channelID uuid.UUID, userID uuid.UUID, isVisible bool, updatedAt time.Time) (*channel.Member, error)
// 	UpdateLastReadMessage(ctx context.Context, channelID uuid.UUID, userID uuid.UUID, lastReadMessageID *uuid.UUID, lastReadMessageAt time.Time, updatedAt time.Time, mentionCount *int) (*channel.Member, error)
// 	UpdateMutedUntil(ctx context.Context, channelID uuid.UUID, userID uuid.UUID, mutedUntil *time.Time, updatedAt time.Time) (*channel.Member, error)
// 	UpdatePinnedAt(ctx context.Context, channelID uuid.UUID, userID uuid.UUID, pinnedAt *time.Time, updatedAt time.Time) (*channel.Member, error)
// }

// type CachedMemberRepository interface {
// 	Get(ctx context.Context, channelID uuid.UUID, userID uuid.UUID) (*channel.Member, error)
// 	GetBatchByChannelID(ctx context.Context, channelID uuid.UUID) ([]*channel.Member, error)
// 	GetBatchByChannelIDs(ctx context.Context, channelIDs []uuid.UUID) (map[uuid.UUID][]*channel.Member, error)
// 	ListVisibleByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*channel.Member, error)
// }

// type OutboxRepository interface {
// 	Create(ctx context.Context, e *outbox.Event) error
// }
