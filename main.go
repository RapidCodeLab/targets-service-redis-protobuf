package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/RapidCodeLab/experiments/targets-service-redis-protobuf/consumer"
	"github.com/RapidCodeLab/experiments/targets-service-redis-protobuf/storage"
	targets_svc "github.com/RapidCodeLab/experiments/targets-service-redis-protobuf/targets-svc"
	"github.com/caarlos0/env/v9"
)

const timeFormat = "2006-01-02 15:04:05"

type (
	Config struct {
		ServerListenNetwork string `env:"SERVER_LISTEN_NETWORK,required"`
		ServerListenAddr    string `env:"SERVER_LISTEN_ADDR,required"`
		BaseURL             string `env:"BASE_URL,required"`
		RedisConfig
	}

	RedisConfig struct {
		RedisAddr     string `env:"REDIS_ADDR,required"`
		RedisPassword string `env:"REDIS_PASSWORD,required"`
		RedisDB       int    `env:"REDIS_DB,required"`
	}

)

func main() {
	slog.Info(
		"Target Service Started",
		"datetime", time.Now().Format(timeFormat),
	)

	config := &Config{}
	err := env.Parse(config)
	if err != nil {
		slog.Error(
			"Parsing Config Error",
			"datetime", time.Now().Format(timeFormat),
			"error", err.Error(),
		)
		os.Exit(1)
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		exit := make(chan os.Signal, 1)
		signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(exit)
		<-exit
		cancel()
	}()

	cnsmr := consumer.New()

	strg := storage.New()

	s := targets_svc.New(strg, cnsmr)

	err = s.Run(
		ctx,
		config.ServerListenNetwork,
		config.ServerListenAddr,
	)
	if err != nil {
		slog.Error(
			"Stopped With Error",
			"datetime", time.Now().Format(timeFormat),
			"error", err.Error(),
		)
	}

	slog.Info(
		"Successfully Stopped",
		"datetime", time.Now().Format(timeFormat),
	)
}
