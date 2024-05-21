package dbrds_grpc

import (
	"context"
	"log/slog"

	dashboards "github.com/veska-io/grpc-dashboards-public/src/services/dashboards"
	"github.com/veska-io/grpc-dashboards-public/src/storage"
	dpgen "github.com/veska-io/proto-dashboards-public/gen/go/dashboards"
	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type dashboardsServer struct {
	dpgen.UnimplementedDashboardsServer

	Logger  *slog.Logger
	Storage *storage.Storage
}

func Register(gRPCServer *grpc.Server, strg *storage.Storage, logger *slog.Logger) {
	logger.Info("registering gRPC service", slog.String("service", "dashboards"))

	dpgen.RegisterDashboardsServer(gRPCServer, &dashboardsServer{
		Logger:  logger,
		Storage: strg,
	})
}

func (s *dashboardsServer) Status(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	s.Logger.Debug("received status request")

	return &emptypb.Empty{}, nil
}

func (s *dashboardsServer) GetPriceDiff(
	ctx context.Context, in *dpgen.BasicRequest) (*dpgen.BasicResponse, error) {
	s.Logger.Debug("received price diff request")

	response := dashboards.GetPriceDiff(in, s.Storage, s.Logger)

	return response, nil
}
