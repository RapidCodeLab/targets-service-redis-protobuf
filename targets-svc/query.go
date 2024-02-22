package targets_svc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/RoaringBitmap/roaring/roaring64"
)

func (s *Service) GetByTarget(
	ctx context.Context,
	countryCode string,
) ([]uint64, error) {
	s.logger.Info("request handled",
		"data",
		countryCode)

	// status bitmap
	data, err := s.storage.Get(ctx, StatusBitmap)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("get from storage: %s", err.Error()))
	}

	bitmap := roaring64.NewBitmap()
	err = bitmap.UnmarshalBinary(data)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("unmarshal bitmap: %s", err.Error()))
	}

	slog.Info("staus", "values", bitmap.ToArray())

	// country bitmap
	countryData, err := s.storage.Get(
		ctx,
		FilterCountryBitmapPrefix+countryCode)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("get from storage: %s", err.Error()))
	}
	countryBitmap := roaring64.NewBitmap()
	err = countryBitmap.UnmarshalBinary(countryData)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("unmarshal bitmap: %s", err.Error()))
	}
	slog.Info("country", "values", countryBitmap.ToArray())

	bitmap.And(countryBitmap)

	return bitmap.ToArray(), nil
}
