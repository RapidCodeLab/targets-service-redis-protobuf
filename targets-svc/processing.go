package targets_svc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/RoaringBitmap/roaring/roaring64"
)

const (
	addBit                    uint = 1
	removeBit                 uint = 0
	StatusBitmap                   = "bitmap:status"
	FilterCountryBitmapPrefix      = "bitmap:country:"
)

func (s *Service) Proccess(
	ctx context.Context,
	msg IncomingMsg,
) error {
	s.logger.Info("recieved msg", "msg", msg)

	filerExists := make(map[string]bool)
	filerExists[FilterTargetCountry] = false

	switch msg.Status {
	case StatusEnabled:
		err := s.UpdateBitmap(ctx, StatusBitmap, msg.IDx, addBit)
		if err != nil {
			s.logger.Error("set status bitmap", "error", err.Error())
			// collect error
		}
	case StatusDisabled:
		err := s.UpdateBitmap(ctx, StatusBitmap, msg.IDx, removeBit)
		if err != nil {
			s.logger.Error("set status bitmap", "error", err.Error())
			// collect error
		}
	default:
		s.logger.Warn("unrecognized operation", "value", msg.Status)
	}

	// set Filters
	for _, filter := range msg.Filters {
		slog.Info("filter to set", "data", filter)
		switch filter.Target {
		case FilterTargetCountry:
			err := s.SetCountryBitmap(
				ctx,
				msg.IDx,
				filter.Type,
				filter.Values)
			if err != nil {
				s.logger.Error("set country bitmap", "error", err.Error())
				// collect error
			}
			filerExists[FilterTargetCountry] = true
		default:
			slog.Warn("unrecognized filter target", "msg", filter.Target)
			continue
		}
	}

	// set if filter not exists
	if !filerExists[FilterTargetCountry] {
		err := s.SetCountryBitmap(
			ctx,
			msg.IDx,
			"none",
			[]string{})
		if err != nil {
			s.logger.Error("set country bitmap", "error", err.Error())
			// collect error
		}
	}

	return nil
}

func (s *Service) SetCountryBitmap(
	ctx context.Context,
	idx uint64,
	filter_type string,
	values []string,
) error {
	var err error

	for _, cc := range CountryCodes {
		bit := addBit

		if contains(values, cc) && filter_type == FilterTypeDisallowed {
			bit = removeBit
		}

		err := s.UpdateBitmap(
			ctx,
			FilterCountryBitmapPrefix+cc,
			idx,
			bit,
		)
		if err != nil {
			slog.Error("update bitmap", "error", err.Error())
			// collect err
		}
	}

	return err
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
		s.logger.Info("add bit", "idx", idx, "key", key)
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

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
