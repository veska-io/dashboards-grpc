package app

import (
	"fmt"
	"log/slog"

	grpc_app "github.com/veska-io/grpc-dashboards-public/src/app/grpc"
	"github.com/veska-io/grpc-dashboards-public/src/config"
	"github.com/veska-io/grpc-dashboards-public/src/storage"
)

type App struct {
	Config *config.Config
	Logger *slog.Logger

	GrpcApp *grpc_app.GrpcApp
	Storage *storage.Storage
}

func New(cfg *config.Config, logger *slog.Logger) *App {
	strg := storage.New(
		fmt.Sprintf("%s:%d", cfg.ClickhouseHost, cfg.ClickhousePort),
		cfg.ClickhouseDb,
		cfg.ClickhouseUser,
		cfg.ClickhousePassword,
		logger,
	)
	gRpcApp := grpc_app.New(cfg.GrpcPort, strg, logger)

	return &App{
		Config: cfg,
		Logger: logger,

		GrpcApp: gRpcApp,
	}
}
