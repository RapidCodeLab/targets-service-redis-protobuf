package storage

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Storage struct {
	rdb *redis.Client
}

func New(addr, password string, db int) *Storage {
	return &Storage{
		rdb: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		}),
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
