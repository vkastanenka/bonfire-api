package channel

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

const cacheWarmTimeout = 2 * time.Second

type CachedChannelRepository struct {
	cache *ChannelCache
	repo  *ChannelRepository
}

func NewCachedChannelRepository(cache *ChannelCache, repo *ChannelRepository) *CachedChannelRepository {
	return &CachedChannelRepository{
		cache: cache,
		repo:  repo,
	}
}

func (r *CachedChannelRepository) Get(ctx context.Context, id uuid.UUID) (*Channel, error) {
	if r.cache != nil {
		ch, err := r.cache.Get(ctx, id)
		if err == nil && ch != nil {
			return ch, nil
		}

		if err != nil {
			slog.WarnContext(ctx, "redis channel cache read failure, falling back to database",
				slog.String("channel_id", id.String()),
				slog.Any("error", err),
			)
		}
	}

	ch, err := r.repo.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	if r.cache != nil && ch != nil {
		cacheCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cacheWarmTimeout)
		defer cancel()

		if setErr := r.cache.Set(cacheCtx, ch); setErr != nil {
			slog.WarnContext(cacheCtx, "failed to populate channel cache after database read",
				slog.String("channel_id", id.String()),
				slog.Any("error", setErr),
			)
		}
	}

	return ch, nil
}

func (r *CachedChannelRepository) GetBatch(
	ctx context.Context,
	ids []uuid.UUID,
) (map[uuid.UUID]*Channel, error) {
	if len(ids) == 0 {
		return map[uuid.UUID]*Channel{}, nil
	}

	var (
		found   map[uuid.UUID]*Channel
		missing []uuid.UUID
	)

	if r.cache != nil {
		var err error
		found, missing, err = r.cache.GetBatch(ctx, ids)
		if err != nil {
			slog.WarnContext(ctx, "redis channel cache batch read failure, falling back to database",
				slog.Int("requested_count", len(ids)),
				slog.Any("error", err),
			)
			missing = ids
			found = make(map[uuid.UUID]*Channel)
		}
	} else {
		missing = ids
		found = make(map[uuid.UUID]*Channel)
	}

	channelsMap := make(map[uuid.UUID]*Channel, len(ids))
	for id, ch := range found {
		if ch != nil {
			channelsMap[id] = ch
		}
	}

	if len(missing) == 0 {
		return channelsMap, nil
	}

	dbChannels, err := r.repo.GetBatch(ctx, missing)
	if err != nil {
		return nil, err
	}

	dbChannelsMap := make(map[uuid.UUID]*Channel, len(dbChannels))
	for _, ch := range dbChannels {
		if ch != nil {
			dbChannelsMap[ch.ID] = ch
		}
	}

	if r.cache != nil && len(dbChannelsMap) > 0 {
		cacheCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cacheWarmTimeout)
		defer cancel()

		if setErr := r.cache.SetBatch(cacheCtx, dbChannelsMap); setErr != nil {
			slog.WarnContext(cacheCtx, "failed to populate channel batch cache after database read",
				slog.Int("count", len(dbChannelsMap)),
				slog.Any("error", setErr),
			)
		}
	}

	for id, ch := range dbChannelsMap {
		channelsMap[id] = ch
	}

	return channelsMap, nil
}
