package channel

import (
	"context"
	"time"

	"bonfire-api/internal/db"

	"github.com/google/uuid"
)

type ChannelRepository struct {
	store *db.Store
}

func NewChannelRepository(store *db.Store) *ChannelRepository {
	return &ChannelRepository{
		store: store,
	}
}

func (r *ChannelRepository) Create(ctx context.Context, ch *Channel) (*Channel, error) {
	row, err := r.store.ChannelCreate(ctx, db.ChannelCreateParams{
		ID:            db.ToUUID(ch.ID),
		OwnerID:       db.ToUUIDPtr(ch.OwnerID),
		LastMessageID: db.ToUUIDPtr(ch.LastMessageID),
		CreatedAt:     db.ToTimestamptz(ch.CreatedAt),
		UpdatedAt:     db.ToTimestamptz(ch.UpdatedAt),
		LastMessageAt: db.ToTimestamptz(ch.LastMessageAt),
		Type:          int16(ch.Type),
		Name:          db.ToTextPtr(ch.Name),
		IconURL:       db.ToTextPtr(ch.IconURL),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannel)
	}

	return channelFromRow(row), nil
}

func (r *ChannelRepository) Get(ctx context.Context, id uuid.UUID) (*Channel, error) {
	row, err := r.store.ChannelGet(ctx, db.ToUUID(id))
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannel)
	}

	return channelFromRow(row), nil
}

func (r *ChannelRepository) GetForUpdate(ctx context.Context, id uuid.UUID) (*Channel, error) {
	row, err := r.store.ChannelGetForUpdate(ctx, db.ToUUID(id))
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannel)
	}

	return channelFromRow(row), nil
}

func (r *ChannelRepository) GetBatch(
	ctx context.Context,
	ids []uuid.UUID,
) ([]*Channel, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := r.store.ChannelGetBatch(ctx, db.ToUUIDs(ids))
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannel)
	}

	return channelsFromRows(rows), nil
}

func (r *ChannelRepository) UpdateGroup(
	ctx context.Context,
	id uuid.UUID,
	name *string,
	iconURL *string,
	updatedAt time.Time,
) (*Channel, error) {
	row, err := r.store.ChannelUpdateGroup(ctx, db.ChannelUpdateGroupParams{
		ID:        db.ToUUID(id),
		Name:      db.ToTextPtr(name),
		IconURL:   db.ToTextPtr(iconURL),
		UpdatedAt: db.ToTimestamptz(updatedAt),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannel)
	}

	return channelFromRow(row), nil
}

func (r *ChannelRepository) UpdateOwnerID(
	ctx context.Context,
	id uuid.UUID,
	ownerID uuid.UUID,
	updatedAt time.Time,
) (*Channel, error) {
	row, err := r.store.ChannelUpdateOwnerID(ctx, db.ChannelUpdateOwnerIDParams{
		ID:        db.ToUUID(id),
		OwnerID:   db.ToUUID(ownerID),
		UpdatedAt: db.ToTimestamptz(updatedAt),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannel)
	}

	return channelFromRow(row), nil
}

func (r *ChannelRepository) UpdateLastMessage(
	ctx context.Context,
	id uuid.UUID,
	lastMessageID *uuid.UUID,
	lastMessageAt time.Time,
	updatedAt time.Time,
) (*Channel, error) {
	row, err := r.store.ChannelUpdateLastMessage(ctx, db.ChannelUpdateLastMessageParams{
		ID:            db.ToUUID(id),
		LastMessageID: db.ToUUIDPtr(lastMessageID),
		LastMessageAt: db.ToTimestamptz(lastMessageAt),
		UpdatedAt:     db.ToTimestamptz(updatedAt),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannel)
	}

	return channelFromRow(row), nil
}

func (r *ChannelRepository) Delete(ctx context.Context, id uuid.UUID) error {
	err := r.store.ChannelDelete(ctx, db.ToUUID(id))
	if err != nil {
		return db.NewError(err, db.ResourceChannel)
	}

	return nil
}

func channelFromRow(row db.Channel) *Channel {
	return Reconstitute(
		db.FromUUID(row.ID),
		ChannelType(row.Type),
		db.FromUUIDPtr(row.OwnerID),
		db.FromTextPtr(row.Name),
		db.FromTextPtr(row.IconURL),
		db.FromUUIDPtr(row.LastMessageID),
		db.FromTimestamptz(row.LastMessageAt),
		db.FromTimestamptz(row.CreatedAt),
		db.FromTimestamptz(row.UpdatedAt),
	)
}

func channelsFromRows(rows []db.Channel) []*Channel {
	if len(rows) == 0 {
		return nil
	}

	channels := make([]*Channel, 0, len(rows))
	for _, row := range rows {
		channels = append(channels, channelFromRow(row))
	}

	return channels
}
