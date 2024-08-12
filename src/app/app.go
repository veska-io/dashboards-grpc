package app

import (
	"database/sql"
	"fmt"
	"log/slog"

	grpc_app "github.com/veska-io/dashboards-grpc/src/app/grpc"
	"github.com/veska-io/dashboards-grpc/src/config"
	"github.com/veska-io/dashboards-grpc/src/storage/clickhouse"
)

type App struct {
	Config *config.Config
	Logger *slog.Logger

	GrpcApp *grpc_app.GrpcApp
	Storage *sql.DB
}

func New(cfg *config.Config, logger *slog.Logger) *App {
	strg := clickhouse.New(
		fmt.Sprintf("%s:%d", cfg.ClickhouseHost, cfg.ClickhousePort),
		cfg.ClickhouseDb,
		cfg.ClickhouseUser,
		cfg.ClickhousePassword,
		logger,
	)
	gRpcApp := grpc_app.New(cfg.GrpcPort, cfg.MaximumDataCorrupt, strg, logger)

	return &App{
		Config: cfg,
		Logger: logger,

		GrpcApp: gRpcApp,
	}
}
