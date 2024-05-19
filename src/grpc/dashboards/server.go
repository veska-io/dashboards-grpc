package dbrds_grpc

import (
	"log/slog"

	dpgen "github.com/veska-io/proto-dashboards-public/gen/go/dashboards"
	"google.golang.org/grpc"
)

type dashboardsServer struct {
	dpgen.UnimplementedDashboardsServer

	logger *slog.Logger
}

func Register(gRPCServer *grpc.Server, logger *slog.Logger) {
	logger.Info("registering gRPC service", slog.String("service", "dashboards"))

	dpgen.RegisterDashboardsServer(gRPCServer, &dashboardsServer{
		logger: logger,
	})
}
