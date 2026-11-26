package relation

import (
	"bonfire-api/internal/channel"
	"bonfire-api/internal/gateway"
	"bonfire-api/internal/user"

	"github.com/google/uuid"
)

const (
	MessageFriendRequestSent     = "relation.friend_request_sent"
	MessageFriendRequestReceived = "relation.friend_request_received"
	MessageFriendRequestDeleted  = "relation.friend_request_deleted"
	MessageFriendAdded           = "relation.friend_added"
	MessageFriendDeleted         = "relation.friend_deleted"
	MessageUserBlocked           = "relation.user_blocked"
	MessageBlockDeleted          = "relation.block_deleted"
)

type MessageFriendRequestPayload struct {
	PeerUser *user.DTO `json:"peerUser"`
}

func newFriendRequestSentPayload(u *user.User) MessageFriendRequestPayload {
	return MessageFriendRequestPayload{
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

type MessagePeerIDPayload struct {
	PeerID uuid.UUID `json:"peerId"`
}

func newPeerIDPayload(peerID uuid.UUID) MessagePeerIDPayload {
	return MessagePeerIDPayload{
		PeerID: peerID,
	}
}
