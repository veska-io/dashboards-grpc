package grpc_app

import (
	"database/sql"
	"fmt"
	"log/slog"
	"net"

	dbrds_grpc "github.com/veska-io/grpc-dashboards-public/src/grpc/dashboards"
	"google.golang.org/grpc"
)

type GrpcApp struct {
	logger *slog.Logger
	port   int

	GrpcServer *grpc.Server
}

func New(port int, maxDataCorrupt int16, strg *sql.DB, logger *slog.Logger) *GrpcApp {
	gRPCServer := grpc.NewServer()

	dbrds_grpc.Register(gRPCServer, strg, logger, maxDataCorrupt)

	return &GrpcApp{
		logger: logger,
		port:   port,

		GrpcServer: gRPCServer,
	}
}

func (a *GrpcApp) MustStart() {
	if err := a.Start(); err != nil {
		panic(fmt.Errorf("failed to start gRPC server: %w", err))
	}
}

func (a *GrpcApp) Start() error {
	a.logger.Info("about to listen on port", slog.Int("port", a.port))
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", a.port))

	if err != nil {
		a.logger.Error(fmt.Sprint(err), slog.Int("port", a.port))

		return fmt.Errorf("failed to listen: %w", err)
	}

	a.logger.Info("gRPC server started", slog.String("addr", l.Addr().String()))

	if err := a.GrpcServer.Serve(l); err != nil {
		a.logger.Error(fmt.Sprint(err))

		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
}

func (a *GrpcApp) Stop() {
	a.GrpcServer.GracefulStop()
}
