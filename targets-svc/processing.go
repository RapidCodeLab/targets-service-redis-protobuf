package targets_svc

import (
	"context"
	"errors"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
)

const (
	addBit       = 1
	removeBit    = 0
	StatusBitmap = "bitmap.status"
)

func (s *Service) Proccess(
	ctx context.Context,
	msg IncomingMsg,
) error {
	s.logger.Info("recieved msg", "msg", msg)

	switch msg.Status {
	case StatusEnabled:
		return s.UpdateBitmap(ctx, StatusBitmap, msg.IDx, addBit)
	case StatusDisabled:
		return s.UpdateBitmap(ctx, StatusBitmap, msg.IDx, removeBit)
	default:
		s.logger.Warn("unrecognized operation", "value", msg.Status)
	}

	return nil
}

func (s *Service) UpdateBitmap(
	ctx context.Context,
	key string,
	idx uint64,
	value uint,
) error {
	bitmapExists := true

	data, err := s.storage.Get(ctx, key)
	if err != nil {
		s.logger.Warn(
			"bitmap not found",
			"error",
			errors.New(fmt.Sprintf("get from storage: %s", err.Error())),
		)

		bitmapExists = false
	}

	bitmap := roaring64.NewBitmap()
	if bitmapExists {
		err = bitmap.UnmarshalBinary(data)
		if err != nil {
			return errors.New(fmt.Sprintf("unmarshal bitmap: %s", err.Error()))
		}
	}

	switch value {
	case removeBit:
		bitmap.Remove(idx)
	case addBit:
		bitmap.Add(idx)
	default:
		s.logger.Warn("unrecognized operation", "value", value)
	}

	res, err := bitmap.MarshalBinary()
	if err != nil {
		return errors.New(fmt.Sprintf("marshal bitmap: %s", err.Error()))
	}

	return s.storage.Set(ctx, key, res)
}
