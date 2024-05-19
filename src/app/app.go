package app

import (
	"log/slog"

	grpc_app "github.com/veska-io/grpc-dashboards-public/src/app/grpc"
	"github.com/veska-io/grpc-dashboards-public/src/config"
)

type App struct {
	Config *config.Config
	Logger *slog.Logger

	GrpcApp *grpc_app.GrpcApp
}

func New(cfg *config.Config, logger *slog.Logger) *App {
	gRpcApp := grpc_app.New(cfg.GrpcPort, logger)

	return &App{
		Config: cfg,
		Logger: logger,

		GrpcApp: gRpcApp,
	}
}
