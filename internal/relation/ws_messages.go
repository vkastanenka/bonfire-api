package relation

import (
	"bonfire-api/internal/channel"
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

func newFriendRequestSentPayload(u *user.User) MessageFriendRequestSentPayload {
	return MessageFriendRequestSentPayload{
		User: user.NewDTO(u),
	}
}

type MessageFriendAddedPayload struct {
	User     *user.DTO           `json:"user"`
	Presence gateway.Presence    `json:"presence"`
	Channel  *channel.ChannelDTO `json:"channel,omitempty"`
	Member   *channel.MemberDTO  `json:"member,omitempty"`
}

func newFriendAddedPayload(
	u *user.User,
	presence gateway.Presence,
	ch *channel.Channel,
	member *channel.Member,
) MessageFriendAddedPayload {
	return MessageFriendAddedPayload{
		User:     user.NewDTO(u),
		Presence: presence,
		Channel:  channel.NewDTO(ch),
		Member:   channel.NewMemberDTO(member),
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
