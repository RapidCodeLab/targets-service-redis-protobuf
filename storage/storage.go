package storage

import (
	"context"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

type Storage struct {
	rdb *redis.Client
}

func New(addr, password string, db int) *Storage {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	err := redisClient.Ping(context.Background()).Err()
	if err != nil {
		slog.Error("redis ping", "error", err.Error())
	}

	return &Storage{
		rdb: redisClient,
	}
}

func (s *Storage) Set(
	ctx context.Context,
	key string,
	value []byte,
) error {
	return s.rdb.Set(ctx, key, value, 0).Err()
}

func (s *Storage) Get(
	ctx context.Context,
	key string,
) ([]byte, error) {
	return s.rdb.Get(ctx, key).Bytes()
}
