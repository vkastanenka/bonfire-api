package relation

import (
	"bytes"
	"cmp"
	"slices"
	"strings"
	"time"

	"bonfire-api/internal/user"

	"github.com/google/uuid"
)

const MaxTypeLimit int = 1000

type Relation struct {
	User1ID   uuid.UUID
	User2ID   uuid.UUID
	ActorID   uuid.UUID
	ChannelID *uuid.UUID
	Type      Type
	CreatedAt time.Time
	UpdatedAt time.Time
}

func New(
	user1ID,
	user2ID,
	actorID uuid.UUID,
	relType Type,
	channelID *uuid.UUID,
	now time.Time,
) *Relation {
	return &Relation{
		User1ID:   user1ID,
		User2ID:   user2ID,
		ActorID:   actorID,
		ChannelID: channelID,
		Type:      relType,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func NewPending(
	user1ID,
	user2ID,
	actorID uuid.UUID,
	now time.Time,
) *Relation {
	return New(
		user1ID,
		user2ID,
		actorID,
		TypePending,
		nil,
		now,
	)
}

func NewFriends(
	user1ID,
	user2ID,
	actorID uuid.UUID,
	channelID *uuid.UUID,
	now time.Time,
) *Relation {
	return New(
		user1ID,
		user2ID,
		actorID,
		TypeFriends,
		channelID,
		now,
	)
}

func NewBlocked(
	user1ID,
	user2ID,
	actorID uuid.UUID,
	now time.Time,
) *Relation {
	return New(
		user1ID,
		user2ID,
		actorID,
		TypeBlocked,
		nil,
		now,
	)
}

func Reconstitute(
	user1ID,
	user2ID,
	actorID uuid.UUID,
	channelID *uuid.UUID,
	relType Type,
	createdAt,
	updatedAt time.Time,
) *Relation {
	return &Relation{
		User1ID:   user1ID,
		User2ID:   user2ID,
		ActorID:   actorID,
		ChannelID: channelID,
		Type:      relType,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
}

func (r *Relation) IsPending() bool { return r.Type.IsPending() }
func (r *Relation) IsFriends() bool { return r.Type.IsFriends() }
func (r *Relation) IsBlocked() bool { return r.Type.IsBlocked() }

func (r *Relation) IsPendingActor(userID uuid.UUID) bool {
	return r.IsPending() && r.ActorID == userID
}

func (r *Relation) IsFriendsActor(userID uuid.UUID) bool {
	return r.IsFriends() && r.ActorID == userID
}

func (r *Relation) IsBlockedActor(userID uuid.UUID) bool {
	return r.IsBlocked() && r.ActorID == userID
}

func (r *Relation) IsParticipant(userID uuid.UUID) bool {
	return userID == r.User1ID || userID == r.User2ID
}

func (r *Relation) IsBlockedByPeer(userID uuid.UUID) bool {
	return r.IsBlocked() && r.IsParticipant(userID) && r.ActorID != userID
}

func (r *Relation) PeerID(userID uuid.UUID) uuid.UUID {
	if r.User1ID == userID {
		return r.User2ID
	}
	return r.User1ID
}

// TODO: Confirm what this is used for?
func (r *Relation) PeerIDs(userID uuid.UUID) []uuid.UUID {
	return []uuid.UUID{r.PeerID(userID)}
}

func (r *Relation) Accept(actorID uuid.UUID, channelID uuid.UUID, now time.Time) {
	r.Type = TypeFriends
	r.ActorID = actorID
	r.ChannelID = &channelID
	r.touch(now)
}

func (r *Relation) Block(actorID uuid.UUID, now time.Time) {
	r.Type = TypeBlocked
	r.ActorID = actorID
	r.touch(now)
}

func (r *Relation) touch(at time.Time) {
	r.UpdatedAt = at
}

func sortIDPair(id1, id2 uuid.UUID) (uuid.UUID, uuid.UUID) {
	if bytes.Compare(id1[:], id2[:]) < 0 {
		return id1, id2
	}
	return id2, id1
}

func sortFriendIDs(friendIDs []uuid.UUID, users map[uuid.UUID]*user.User) {
	slices.SortFunc(friendIDs, func(aID, bID uuid.UUID) int {
		uA := users[aID]
		uB := users[bID]

		var nameA, nameB string
		if uA != nil {
			if dn := uA.DisplayName; dn != "" {
				nameA = strings.ToLower(dn)
			} else {
				nameA = strings.ToLower(uA.Username)
			}
		}
		if uB != nil {
			if dn := uB.DisplayName; dn != "" {
				nameB = strings.ToLower(dn)
			} else {
				nameB = strings.ToLower(uB.Username)
			}
		}

		if c := cmp.Compare(nameA, nameB); c != 0 {
			return c
		}

		return bytes.Compare(aID[:], bID[:])
	})
}

func validateIncomingBlock(userID uuid.UUID, rel *Relation) error {
	if rel != nil && rel.IsBlockedByPeer(userID) {
		return ErrBlockedUser()
	}
	return nil
}

func validateAccept(userID uuid.UUID, rel *Relation) error {
	if rel == nil {
		return nil
	}
	if !rel.IsPending() {
		return ErrNotPending()
	}
	if rel.ActorID == userID {
		return ErrCannotRequestSelf()
	}
	return nil
}
