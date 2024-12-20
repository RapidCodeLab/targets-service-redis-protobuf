package targets_svc

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/RapidCodeLab/experiments/targets-service-redis-protobuf/pkg/targets"
	"github.com/valyala/fasthttp/reuseport"
	"google.golang.org/grpc"
)

const (
	StatusEnabled  = "enabled"
	StatusDisabled = "disabled"

	FilterTypeAllowed    = "included"
	FilterTypeDisallowed = "excluded"

	FilterTargetCountry     = "country"
	FilterTargetDevice      = "device"
	FilterTargetPlatform    = "platform"
	FilterTargetBrowser     = "browser"
	FilterTargetPublisherID = "publisher_id"
	FilterTargetSourceID    = "source_id"
	FilterTargetEndpointID  = "endpoint_id"
	FilterTargetAdType      = "ad_type"
)

type (
	Storage interface {
		Set(ctx context.Context, key string, value []byte) error
		Get(ctx context.Context, key string) (value []byte, err error)
	}

	Consumer interface {
		Read(ctx context.Context) (data []byte, err error)
		Stop() error
	}

	Service struct {
		targets.UnimplementedTargetsServer
		storage  Storage
		consumer Consumer
		logger   *slog.Logger
	}
)

func (s *Service) Get(
	ctx context.Context,
	req *targets.Request,
) (*targets.Response, error) {
	s.logger.Info("request handled",
		"cc", req.GetCountryCode(),
		"browser", req.GetBrowser(),
		"platform", req.GetPlatform(),
		"device", req.GetDevice())
	res := &targets.Response{}

	data, err := s.GetByTarget(
		ctx,
		req.GetCountryCode(),
		req.GetBrowser(),
		req.GetPlatform(),
		req.GetDevice(),
	)
	if err != nil {
		s.logger.Error("get by target", "error", err.Error())
		return res, err
	}

	res.Ids = data

	return res, nil
}

func New(
	s Storage,
	c Consumer,
) *Service {
	return &Service{
		storage:  s,
		consumer: c,
		logger:   slog.Default(),
	}
}

func (s *Service) Run(
	ctx context.Context,
	listenNetwork,
	listeAddr string,
) error {
	ln, err := reuseport.Listen(listenNetwork, listeAddr)
	if err != nil {
		return err
	}
	defer ln.Close()

	srv := grpc.NewServer()
	targets.RegisterTargetsServer(srv, s)

	go func() {
		for {
			select {
			case <-ctx.Done():
				err := s.consumer.Stop()
				if err != nil {
					s.logger.Error("stop consumer", "error", err.Error())
				}
				srv.GracefulStop()
			default:
				msg, err := s.consumer.Read(ctx)
				if err != nil {
					s.logger.Error("read from kafka", "error", err.Error())
					continue
				}

				var incomingMsg IncomingMsg
				err = json.Unmarshal(msg, &incomingMsg)
				if err != nil {
					s.logger.Error("unmarshaling", "error", err.Error())
					continue
				}

				err = s.Proccess(ctx, incomingMsg)
				if err != nil {
					s.logger.Error("processing msg", "error", err.Error())
					continue
				}

			}
		}
	}()

	return srv.Serve(ln)
}
