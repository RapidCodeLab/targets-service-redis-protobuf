package targets_svc

import (
	"context"
	"log/slog"

	"github.com/RapidCodeLab/experiments/targets-service-redis-protobuf/pkg/targets"
	"github.com/valyala/fasthttp/reuseport"
	"google.golang.org/grpc"
)

type (
	Storage interface {
		Set(ctx context.Context, key string, value []byte) error
		Get(ctx context.Context, key string) (value []byte, err error)
	}

	Consumer interface {
		Read(ctx context.Context) (data []byte, err error)
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
		"data",
		req.GetCountryCode())

	return &targets.Response{
		Ids: []uint64{1, 2, 3},
	}, nil
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
		<-ctx.Done()
		srv.GracefulStop()
	}()

	return srv.Serve(ln)
}
