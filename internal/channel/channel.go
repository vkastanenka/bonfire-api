package channel

import (
	"time"

	"github.com/google/uuid"
)

const (
	ChannelMinMembers      = 1
	ChannelMaxMembers      = 10
	ChannelMaxPeers        = 9
	ChannelMaxSidebarItems = 100
)

type Channel struct {
	ID            uuid.UUID
	Type          ChannelType
	Name          *string
	IconURL       *string
	LastMessageID *uuid.UUID
	LastMessageAt time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func New(id uuid.UUID, chType ChannelType, now time.Time) *Channel {
	nowUTC := now.UTC()
	return &Channel{
		ID:            id,
		Type:          chType,
		LastMessageAt: nowUTC,
		CreatedAt:     nowUTC,
		UpdatedAt:     nowUTC,
	}
}

func NewDirect(id uuid.UUID, now time.Time) *Channel {
	return New(id, ChannelTypeDirect, now)
}

func NewGroup(id uuid.UUID, now time.Time) *Channel {
	return New(id, ChannelTypeGroup, now)
}

func Reconstitute(
	id uuid.UUID,
	chType ChannelType,
	name *string,
	iconURL *string,
	lastMessageID *uuid.UUID,
	lastMessageAt time.Time,
	createdAt time.Time,
	updatedAt time.Time,
) *Channel {
	return &Channel{
		ID:            id,
		Type:          chType,
		Name:          name,
		IconURL:       iconURL,
		LastMessageID: lastMessageID,
		LastMessageAt: lastMessageAt,
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
	}
}

func (c *Channel) IsDirect() bool { return c.Type.IsDirect() }
func (c *Channel) IsGroup() bool  { return c.Type.IsGroup() }

func (c *Channel) UpdateName(name *string, now time.Time) {
	c.Name = name
	c.touch(now)
}

func (c *Channel) UpdateIconURL(iconURL *string, now time.Time) {
	c.IconURL = iconURL
	c.touch(now)
}

func (c *Channel) UpdateMetadata(name, iconURL *string, now time.Time) {
	c.Name = name
	c.IconURL = iconURL
	c.touch(now)
}

func (c *Channel) RecordMessage(messageID uuid.UUID, at time.Time) {
	atUTC := at.UTC()
	c.LastMessageID = &messageID
	c.LastMessageAt = atUTC
	c.touch(atUTC)
}

func (c *Channel) touch(at time.Time) {
	c.UpdatedAt = at.UTC()
}

type ChannelDTO struct {
	ID            uuid.UUID   `json:"id"`
	Type          ChannelType `json:"type"`
	Name          *string     `json:"name,omitempty"`
	IconURL       *string     `json:"iconUrl,omitempty"`
	LastMessageID *uuid.UUID  `json:"lastMessageId,omitempty"`
	LastMessageAt time.Time   `json:"lastMessageAt"`
	CreatedAt     time.Time   `json:"createdAt"`
	UpdatedAt     time.Time   `json:"updatedAt"`
}

func NewDTO(c *Channel) *ChannelDTO {
	if c == nil {
		return nil
	}
	return &ChannelDTO{
		ID:            c.ID,
		Type:          c.Type,
		Name:          c.Name,
		IconURL:       c.IconURL,
		LastMessageID: c.LastMessageID,
		LastMessageAt: c.LastMessageAt,
		CreatedAt:     c.CreatedAt,
		UpdatedAt:     c.UpdatedAt,
	}
}

func NewDTOs(channels []*Channel) []ChannelDTO {
	if len(channels) == 0 {
		return nil
	}

	dtos := make([]ChannelDTO, 0, len(channels))
	for _, c := range channels {
		if dto := NewDTO(c); dto != nil {
			dtos = append(dtos, *dto)
		}
	}

	return dtos
}

func NewDTOMap(channels map[uuid.UUID]*Channel) map[uuid.UUID]ChannelDTO {
	if len(channels) == 0 {
		return make(map[uuid.UUID]ChannelDTO)
	}

	dtos := make(map[uuid.UUID]ChannelDTO, len(channels))
	for id, c := range channels {
		if dto := NewDTO(c); dto != nil {
			dtos[id] = *dto
		}
	}

	return dtos
}

// func getChannelUserIDs(memberIDs []uuid.UUID, messages []*Message) []uuid.UUID {
// 	seen := make(map[uuid.UUID]struct{}, len(memberIDs)+len(messages))
// 	result := make([]uuid.UUID, 0, len(memberIDs)+len(messages))

// 	for _, id := range memberIDs {
// 		if id == (uuid.UUID{}) {
// 			continue
// 		}
// 		if _, exists := seen[id]; !exists {
// 			seen[id] = struct{}{}
// 			result = append(result, id)
// 		}
// 	}

// 	for _, msg := range messages {
// 		if msg == nil || msg.AuthorID == nil {
// 			continue
// 		}

// 		authorID := *msg.AuthorID
// 		if authorID == (uuid.UUID{}) {
// 			continue
// 		}

// 		if _, exists := seen[authorID]; !exists {
// 			seen[authorID] = struct{}{}
// 			result = append(result, authorID)
// 		}
// 	}

// 	return result
// }

// func indexChannels(channels []*Channel) []uuid.UUID {
// 	channelIDs := make([]uuid.UUID, 0, len(channels))

// 	for _, ch := range channels {
// 		if ch == nil {
// 			continue
// 		}
// 		channelIDs = append(channelIDs, ch.ID)
// 	}

// 	return channelIDs
// }

// func validateMaxPeers(rawPeerIDs []uuid.UUID) error {
// 	if len(rawPeerIDs) > ChannelMaxPeers {
// 		return ErrMaxPeersExceeded()
// 	}
// 	return nil
// }

// func validateMinMembers(rawMemberIDs []uuid.UUID) error {
// 	if len(rawMemberIDs) < ChannelMinMembers {
// 		return ErrMinMembersInvalid()
// 	}
// 	return nil
// }
