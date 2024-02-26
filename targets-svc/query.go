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
	countryCode,
	browser,
	platform,
	device string,
) ([]uint64, error) {
	s.logger.Info("request handled",
		"cc", countryCode,
		"browser", browser,
		"platform", platform,
		"device", device)

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

	// browser bitmap
	browserData, err := s.storage.Get(
		ctx,
		FilterBrowserBitmapPrefix+browser)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("get from storage: %s", err.Error()))
	}
	browserBitmap := roaring64.NewBitmap()
	err = browserBitmap.UnmarshalBinary(browserData)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("unmarshal bitmap: %s", err.Error()))
	}
	slog.Info("browser", "values", browserBitmap.ToArray())

	// browser bitmap
	platformData, err := s.storage.Get(
		ctx,
		FilterPlatformBitmapPrefix+platform)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("get from storage: %s", err.Error()))
	}
	platformBitmap := roaring64.NewBitmap()
	err = platformBitmap.UnmarshalBinary(platformData)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("unmarshal bitmap: %s", err.Error()))
	}
	slog.Info("platform", "values", platformBitmap.ToArray())

	// device bitmap
	deviceData, err := s.storage.Get(
		ctx,
		FilterDeviceBitmapPrefix+device)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("get from storage: %s", err.Error()))
	}
	deviceBitmap := roaring64.NewBitmap()
	err = deviceBitmap.UnmarshalBinary(deviceData)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("unmarshal bitmap: %s", err.Error()))
	}
	slog.Info("device", "values", deviceBitmap.ToArray())

	// AND bit ops
	bitmap.And(countryBitmap)
	bitmap.And(browserBitmap)
	bitmap.And(platformBitmap)
	bitmap.And(deviceBitmap)

	return bitmap.ToArray(), nil
}
