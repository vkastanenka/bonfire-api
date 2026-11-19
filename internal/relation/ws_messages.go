package relation

import (
	"bonfire-api/internal/user"
)

const (
	MessageFriendRequestSent    = "relation.friend_request_sent"
	MessageFriendRequestDeleted = "relation.friend_request_deleted"
	MessageFriendAdded          = "relation.friend_added"
	MessageFriendDeleted        = "relation.friend_deleted"
)

type MessageFriendRequestSentPayload struct {
	User *user.DTO `json:"user"`
}

func newFriendRequestSentPayload(user *user.DTO) MessageFriendRequestSentPayload {
	return MessageFriendRequestSentPayload{
		User: user,
	}
}
