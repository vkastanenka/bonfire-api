#!/bin/bash
set -e

CONTAINER_NAME="bonfire_postgres"
DB_USER="postgres"
DB_NAME="bonfire_db"

echo "Clearing relations, channels, and channel_members..."
docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -c \
  "TRUNCATE TABLE relations, channels, channel_members CASCADE;"
echo "Done."

REDIS_CONTAINER="bonfire_redis"

echo "Flushing Redis cache..."
docker exec -i "$REDIS_CONTAINER" redis-cli FLUSHALL