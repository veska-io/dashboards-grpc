package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/veska-io/dashboards-grpc/src/app"
	"github.com/veska-io/dashboards-grpc/src/config"
	"github.com/veska-io/dashboards-grpc/src/logger"
)

func main() {
	cfg := config.MustNew()
	logger := logger.New(cfg.Debug)

	logger.Debug("app started with config", slog.Any("config", cfg))

	appInstance := app.New(cfg, logger)
	go appInstance.GrpcApp.MustStart()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	s := <-stop
	logger.Info("stopping application", slog.String("signal", s.String()))

	appInstance.GrpcApp.Stop()
	logger.Info("application stopped")
}
