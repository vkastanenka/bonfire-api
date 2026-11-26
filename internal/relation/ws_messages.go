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
	PeerUser *user.DTO `json:"peerUser"`
}

func newFriendRequestSentPayload(u *user.User) MessageFriendRequestSentPayload {
	return MessageFriendRequestSentPayload{
		PeerUser: user.NewDTO(u),
	}
}

type MessageFriendAddedPayload struct {
	Channel      *channel.ChannelDTO `json:"channel"`
	Membership   *channel.MemberDTO  `json:"membership"`
	PeerUser     *user.DTO           `json:"peerUser"`
	PeerPresence gateway.Presence    `json:"peerPresence"`
}

func newFriendAddedPayload(
	ch *channel.Channel,
	mem *channel.Member,
	u *user.User,
	presence gateway.Presence,
) MessageFriendAddedPayload {
	return MessageFriendAddedPayload{
		Channel:      channel.NewDTO(ch),
		Membership:   channel.NewMemberDTO(mem),
		PeerUser:     user.NewDTO(u),
		PeerPresence: presence,
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
