package relation

import (
	"bonfire-api/internal/gateway"
	"bonfire-api/internal/user"

	"github.com/google/uuid"
)

const (
	MessageFriendRequestSent    = "relation.friend_request_sent"
	MessageFriendRequestDeleted = "relation.friend_request_deleted"
	MessageFriendAdded          = "relation.friend_added"
	MessageFriendDeleted        = "relation.friend_deleted"
	MessageUserBlocked          = "relation.user_blocked"
)

type MessageFriendRequestSentPayload struct {
	User *user.DTO `json:"user"`
}

func newFriendRequestSentPayload(user *user.DTO) MessageFriendRequestSentPayload {
	return MessageFriendRequestSentPayload{
		User: user,
	}
}

type MessageFriendAddedPayload struct {
	User     *user.DTO        `json:"user"`
	Presence gateway.Presence `json:"presence"`
	// Channel
	// Member
}

func newFriendAddedPayload(user *user.DTO, presence gateway.Presence) MessageFriendAddedPayload {
	return MessageFriendAddedPayload{
		User:     user,
		Presence: presence,
	}
}

type MessageUserIDPayload struct {
	UserID uuid.UUID `json:"user_id"`
}

func newUserIDPayload(userID uuid.UUID) MessageUserIDPayload {
	return MessageUserIDPayload{
		UserID: userID,
	}
}
