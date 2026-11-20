package channel

import (
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	MessageListLimit       = 25
	MessageListBeforeLimit = 5
	MessageListAfterLimit  = 20
)

type Message struct {
	ID               uuid.UUID
	ChannelID        uuid.UUID
	AuthorID         *uuid.UUID
	Type             MessageType
	Content          *string
	Metadata         json.RawMessage
	ReplyToMessageID *uuid.UUID
	ForwardMessageID *uuid.UUID
	ForwardChannelID *uuid.UUID
	PinnedAt         *time.Time
	EditedAt         *time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

func NewMessage(
	id uuid.UUID,
	channelID uuid.UUID,
	authorID *uuid.UUID,
	msgType MessageType,
	content *string,
	metadata json.RawMessage,
	replyToMessageID *uuid.UUID,
	forwardMessageID *uuid.UUID,
	forwardChannelID *uuid.UUID,
	now time.Time,
) *Message {
	nowUTC := now.UTC()
	return &Message{
		ID:               id,
		ChannelID:        channelID,
		AuthorID:         authorID,
		Type:             msgType,
		Content:          content,
		Metadata:         metadata,
		ReplyToMessageID: replyToMessageID,
		ForwardMessageID: forwardMessageID,
		ForwardChannelID: forwardChannelID,
		CreatedAt:        nowUTC,
		UpdatedAt:        nowUTC,
	}
}

func ReconstituteMessage(
	id uuid.UUID,
	channelID uuid.UUID,
	authorID *uuid.UUID,
	msgType MessageType,
	content *string,
	metadata json.RawMessage,
	replyToMessageID *uuid.UUID,
	forwardMessageID *uuid.UUID,
	forwardChannelID *uuid.UUID,
	pinnedAt *time.Time,
	editedAt *time.Time,
	createdAt time.Time,
	updatedAt time.Time,
) *Message {
	return &Message{
		ID:               id,
		ChannelID:        channelID,
		AuthorID:         authorID,
		Type:             msgType,
		Content:          content,
		Metadata:         metadata,
		ReplyToMessageID: replyToMessageID,
		ForwardMessageID: forwardMessageID,
		ForwardChannelID: forwardChannelID,
		PinnedAt:         pinnedAt,
		EditedAt:         editedAt,
		CreatedAt:        createdAt,
		UpdatedAt:        updatedAt,
	}
}

func NewDefaultMessage(
	id uuid.UUID,
	channelID uuid.UUID,
	authorID uuid.UUID,
	content string,
	replyToMessageID *uuid.UUID,
	forwardMessageID *uuid.UUID,
	forwardChannelID *uuid.UUID,
	now time.Time,
) *Message {
	return NewMessage(
		id,
		channelID,
		&authorID,
		MessageTypeDefault,
		&content,
		nil,
		replyToMessageID,
		forwardMessageID,
		forwardChannelID,
		now,
	)
}

func NewSystemMessage(
	id uuid.UUID,
	channelID uuid.UUID,
	authorID uuid.UUID,
	msgType MessageType,
	metadata json.RawMessage,
	now time.Time,
) *Message {
	return NewMessage(
		id,
		channelID,
		&authorID,
		msgType,
		nil,
		metadata,
		nil,
		nil,
		nil,
		now,
	)
}

func NewMemberAddMessage(
	id uuid.UUID,
	channelID uuid.UUID,
	authorID uuid.UUID,
	memberID uuid.UUID,
	now time.Time,
) (*Message, error) {
	metadata, err := json.Marshal(map[string]any{"memberId": memberID.String()})
	if err != nil {
		return nil, err
	}

	return NewSystemMessage(
		id,
		channelID,
		authorID,
		MessageTypeMemberAdd,
		metadata,
		now,
	), nil
}

func NewMemberRemoveMessage(
	id uuid.UUID,
	channelID uuid.UUID,
	authorID uuid.UUID,
	now time.Time,
) *Message {
	return NewSystemMessage(
		id,
		channelID,
		authorID,
		MessageTypeMemberRemove,
		nil,
		now,
	)
}

func NewNameChangeMessage(
	id uuid.UUID,
	channelID uuid.UUID,
	authorID uuid.UUID,
	newName *string,
	now time.Time,
) (*Message, error) {
	metadata, err := json.Marshal(map[string]any{"name": newName})
	if err != nil {
		return nil, err
	}

	return NewSystemMessage(
		id,
		channelID,
		authorID,
		MessageTypeNameChange,
		metadata,
		now,
	), nil
}

func NewIconChangeMessage(
	id uuid.UUID,
	channelID uuid.UUID,
	authorID uuid.UUID,
	now time.Time,
) *Message {
	return NewSystemMessage(
		id,
		channelID,
		authorID,
		MessageTypeIconChange,
		nil,
		now,
	)
}

func NewPinMessage(
	id uuid.UUID,
	channelID uuid.UUID,
	authorID uuid.UUID,
	pinnedMessageID uuid.UUID,
	now time.Time,
) (*Message, error) {
	metadata, err := json.Marshal(map[string]any{"messageId": pinnedMessageID.String()})
	if err != nil {
		return nil, err
	}

	return NewSystemMessage(
		id,
		channelID,
		authorID,
		MessageTypePin,
		metadata,
		now,
	), nil
}

func (m *Message) IsPinned() bool { return m.PinnedAt != nil }
func (m *Message) IsEdited() bool { return m.EditedAt != nil }

func (m *Message) Edit(content string, now time.Time) {
	nowUTC := now.UTC()
	m.Content = &content
	m.EditedAt = &nowUTC
	m.touch(nowUTC)
}

func (m *Message) Pin(now time.Time) {
	nowUTC := now.UTC()
	m.PinnedAt = &nowUTC
	m.touch(nowUTC)
}

func (m *Message) Unpin(now time.Time) {
	m.PinnedAt = nil
	m.touch(now)
}

func (m *Message) touch(at time.Time) {
	m.UpdatedAt = at.UTC()
}

type MessageDTO struct {
	ID               uuid.UUID       `json:"id"`
	ChannelID        uuid.UUID       `json:"channelId"`
	AuthorID         *uuid.UUID      `json:"authorId,omitempty"`
	Type             MessageType     `json:"type"`
	Content          *string         `json:"content,omitempty"`
	Metadata         json.RawMessage `json:"metadata,omitempty"`
	ReplyToMessageID *uuid.UUID      `json:"replyToMessageId,omitempty"`
	ForwardMessageID *uuid.UUID      `json:"forwardMessageId,omitempty"`
	ForwardChannelID *uuid.UUID      `json:"forwardChannelId,omitempty"`
	PinnedAt         *time.Time      `json:"pinnedAt,omitempty"`
	EditedAt         *time.Time      `json:"editedAt,omitempty"`
	CreatedAt        time.Time       `json:"createdAt"`
	UpdatedAt        time.Time       `json:"updatedAt"`
}

func NewMessageDTO(m *Message) *MessageDTO {
	if m == nil {
		return nil
	}
	return &MessageDTO{
		ID:               m.ID,
		ChannelID:        m.ChannelID,
		AuthorID:         m.AuthorID,
		Type:             m.Type,
		Content:          m.Content,
		Metadata:         m.Metadata,
		ReplyToMessageID: m.ReplyToMessageID,
		ForwardMessageID: m.ForwardMessageID,
		ForwardChannelID: m.ForwardChannelID,
		PinnedAt:         m.PinnedAt,
		EditedAt:         m.EditedAt,
		CreatedAt:        m.CreatedAt,
		UpdatedAt:        m.UpdatedAt,
	}
}

func NewMessageDTOs(messages []*Message) []MessageDTO {
	if len(messages) == 0 {
		return nil
	}

	dtos := make([]MessageDTO, 0, len(messages))
	for _, m := range messages {
		if dto := NewMessageDTO(m); dto != nil {
			dtos = append(dtos, *dto)
		}
	}

	return dtos
}

func NewMessageDTOMap(messages []*Message) map[uuid.UUID]MessageDTO {
	if len(messages) == 0 {
		return make(map[uuid.UUID]MessageDTO)
	}

	dtos := make(map[uuid.UUID]MessageDTO, len(messages))
	for _, m := range messages {
		if dto := NewMessageDTO(m); dto != nil {
			dtos[m.ID] = *dto
		}
	}

	return dtos
}

func getMessageIDs(messages []*Message) ([]uuid.UUID, []uuid.UUID) {
	msgIDs := make([]uuid.UUID, 0, len(messages))
	authorIDs := make([]uuid.UUID, 0, len(messages))
	seenAuthors := make(map[uuid.UUID]struct{}, len(messages))

	for _, m := range messages {
		if m == nil {
			continue
		}
		msgIDs = append(msgIDs, m.ID)
		if m.AuthorID != nil && *m.AuthorID != uuid.Nil {
			if _, exists := seenAuthors[*m.AuthorID]; !exists {
				seenAuthors[*m.AuthorID] = struct{}{}
				authorIDs = append(authorIDs, *m.AuthorID)
			}
		}
	}
	return msgIDs, authorIDs
}

func sortMessages(messages []*Message) {
	slices.SortFunc(messages, func(a, b *Message) int {
		if a == nil && b == nil {
			return 0
		}
		if a == nil {
			return -1
		}
		if b == nil {
			return 1
		}
		return strings.Compare(a.ID.String(), b.ID.String())
	})
}

func sortPinnedMessages(messages []*Message) {
	slices.SortFunc(messages, func(a, b *Message) int {
		if a == nil && b == nil {
			return 0
		}
		if a == nil {
			return 1
		}
		if b == nil {
			return -1
		}
		return strings.Compare(b.ID.String(), a.ID.String())
	})
}

// func validateReply(hasReply bool, hasFwdMsg, hasFwdChan bool) error {
// 	if hasReply && (hasFwdMsg || hasFwdChan) {
// 		return ErrMessageReplyConflict()
// 	}
// 	return nil
// }

// func validateForward(hasFwdMsg, hasFwdChan bool) error {
// 	if hasFwdMsg != hasFwdChan {
// 		return ErrMessageForwardIncomplete()
// 	}
// 	return nil
// }
