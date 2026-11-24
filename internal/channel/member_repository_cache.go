package channel

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

const (
	memberCacheWarmTimeout = 2 * time.Second
	defaultUserMemberLimit = 200
)

type CachedMemberRepository struct {
	cache *MemberCache
	repo  *MemberRepository
}

func NewCachedMemberRepository(cache *MemberCache, repo *MemberRepository) *CachedMemberRepository {
	return &CachedMemberRepository{
		cache: cache,
		repo:  repo,
	}
}

func (r *CachedMemberRepository) GetIDsByChannelID(
	ctx context.Context,
	channelID uuid.UUID,
) ([]uuid.UUID, error) {
	if r.cache != nil {
		ids, err := r.cache.GetIDsByChannelID(ctx, channelID)
		if err == nil && len(ids) > 0 {
			return ids, nil
		}
		if err != nil {
			slog.WarnContext(ctx, "redis channel member IDs cache read failure, falling back to database",
				slog.String("channel_id", channelID.String()),
				slog.Any("error", err),
			)
		}
	}

	members, err := r.repo.GetBatchByChannelID(ctx, channelID)
	if err != nil {
		return nil, err
	}

	ids := extractMemberUserIDs(members)
	if r.cache != nil && len(ids) > 0 {
		cacheCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), memberCacheWarmTimeout)
		defer cancel()

		if setErr := r.cache.SetIDsByChannelID(cacheCtx, channelID, ids); setErr != nil {
			slog.WarnContext(cacheCtx, "failed to populate channel member IDs cache after database read",
				slog.String("channel_id", channelID.String()),
				slog.Any("error", setErr),
			)
		}
	}

	return ids, nil
}

func (r *CachedMemberRepository) GetBatchIDsByChannelIDs(
	ctx context.Context,
	channelIDs []uuid.UUID,
) (map[uuid.UUID][]uuid.UUID, error) {
	if len(channelIDs) == 0 {
		return map[uuid.UUID][]uuid.UUID{}, nil
	}

	var (
		found   map[uuid.UUID][]uuid.UUID
		missing []uuid.UUID
	)

	if r.cache != nil {
		var err error
		found, missing, err = r.cache.GetBatchIDsByChannelIDs(ctx, channelIDs)
		if err != nil {
			slog.WarnContext(ctx, "redis batch channel member IDs cache read failure, falling back to database",
				slog.Int("requested_count", len(channelIDs)),
				slog.Any("error", err),
			)
			found = make(map[uuid.UUID][]uuid.UUID, len(channelIDs))
			missing = channelIDs
		}
	} else {
		found = make(map[uuid.UUID][]uuid.UUID, len(channelIDs))
		missing = channelIDs
	}

	result := make(map[uuid.UUID][]uuid.UUID, len(channelIDs))
	for chID, ids := range found {
		if len(ids) > 0 {
			result[chID] = ids
		}
	}

	if len(missing) == 0 {
		return result, nil
	}

	dbMembersByCh, err := r.repo.GetBatchByChannelIDs(ctx, missing)
	if err != nil {
		return nil, err
	}

	warmMap := make(map[uuid.UUID][]uuid.UUID, len(dbMembersByCh))
	for chID, members := range dbMembersByCh {
		ids := extractMemberUserIDs(members)
		result[chID] = ids
		if len(ids) > 0 {
			warmMap[chID] = ids
		}
	}

	if r.cache != nil && len(warmMap) > 0 {
		cacheCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), memberCacheWarmTimeout)
		defer cancel()

		if setErr := r.cache.SetBatchIDsByChannelIDs(cacheCtx, warmMap); setErr != nil {
			slog.WarnContext(cacheCtx, "failed to populate batch channel member IDs cache after database read",
				slog.Int("channel_count", len(warmMap)),
				slog.Any("error", setErr),
			)
		}
	}

	return result, nil
}

func (r *CachedMemberRepository) GetVisibleByUserID(
	ctx context.Context,
	userID uuid.UUID,
) (map[uuid.UUID]*Member, error) {
	if r.cache != nil {
		members, err := r.cache.GetVisibleByUserID(ctx, userID)
		if err == nil && len(members) > 0 {
			return members, nil
		}
		if err != nil {
			slog.WarnContext(ctx, "redis user channel members cache read failure, falling back to database",
				slog.String("user_id", userID.String()),
				slog.Any("error", err),
			)
		}
	}

	dbMembers, err := r.repo.ListVisibleByUserID(ctx, userID, defaultUserMemberLimit)
	if err != nil {
		return nil, err
	}

	membersMap := make(map[uuid.UUID]*Member, len(dbMembers))
	for _, m := range dbMembers {
		if m != nil {
			membersMap[m.ChannelID] = m
		}
	}

	if r.cache != nil && len(membersMap) > 0 {
		cacheCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), memberCacheWarmTimeout)
		defer cancel()

		if setErr := r.cache.SetVisibleByUserID(cacheCtx, userID, membersMap); setErr != nil {
			slog.WarnContext(cacheCtx, "failed to populate user channel members cache after database read",
				slog.String("user_id", userID.String()),
				slog.Int("count", len(membersMap)),
				slog.Any("error", setErr),
			)
		}
	}

	return membersMap, nil
}

func extractMemberUserIDs(members []*Member) []uuid.UUID {
	if len(members) == 0 {
		return nil
	}

	ids := make([]uuid.UUID, 0, len(members))
	seen := make(map[uuid.UUID]struct{}, len(members))
	for _, m := range members {
		if m == nil {
			continue
		}
		if _, ok := seen[m.UserID]; ok {
			continue
		}
		seen[m.UserID] = struct{}{}
		ids = append(ids, m.UserID)
	}

	return ids
}
