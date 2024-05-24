package dbrds_grpc

import (
	"context"
	"database/sql"
	"log/slog"

	"github.com/veska-io/grpc-dashboards-public/src/grpc/dashboards/serializers"
	dashboards "github.com/veska-io/grpc-dashboards-public/src/services/dashboards"
	dpgen "github.com/veska-io/proto-dashboards-public/gen/go/dashboards"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type dashboardsServer struct {
	dpgen.UnimplementedDashboardsServer

	maxDataCorrupt int16

	Logger  *slog.Logger
	Storage *sql.DB
}

func Register(gRPCServer *grpc.Server, strg *sql.DB, logger *slog.Logger, maxDataCorrupt int16) {
	logger.Info("registering gRPC service", slog.String("service", "dashboards"))

	dpgen.RegisterDashboardsServer(gRPCServer, &dashboardsServer{
		Logger:  logger,
		Storage: strg,

		maxDataCorrupt: maxDataCorrupt,
	})
}

func (s *dashboardsServer) Status(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	s.Logger.Debug("received status request")

	return &emptypb.Empty{}, nil
}

func (s *dashboardsServer) GetMarkets(
	ctx context.Context, in *dpgen.BasicRequest,
) (*dpgen.MarketsResponse, error) {
	s.Logger.Debug("received markets request", slog.Any("request", in))

	response := dpgen.MarketsResponse{}

	serializedRequest, err := serializers.NewBasicRequest(in)
	if err != nil {
		s.Logger.Error("unable to process the request", slog.String("err", err.Error()))
		return nil, status.Error(codes.InvalidArgument, "unable to process the request")
	}

	marketsChan, err := dashboards.GetMarkets(dashboards.MarketsFilter{
		Exchanges:  serializedRequest.Exchanges,
		StartTime:  serializedRequest.StartTime,
		EndTime:    serializedRequest.EndTime,
		WindowSize: serializedRequest.WindowSize,
	}, s.Storage, s.Logger, ctx)

	if err != nil {
		s.Logger.Error("unable to process the request", slog.String("err", err.Error()))
		return nil, status.Error(codes.Internal, "unable to process the request")
	}

	errors := 0
	for market := range marketsChan {
		if market.Err != nil {
			s.Logger.Error("error processing market", slog.String("err", market.Err.Error()))
			errors++
			continue
		}

		response.Markets = append(response.Markets, market.Name)
	}

	if errors != 0 && float64(errors/len(response.Markets)) > float64(s.maxDataCorrupt/100) {
		s.Logger.Error("too many errors processing markets")
		return nil, status.Error(codes.Internal, "unable to process the request")
	}

	return &response, nil
}

func (s *dashboardsServer) GetExchanges(
	ctx context.Context, in *emptypb.Empty,
) (*dpgen.ExchangesResponse, error) {
	s.Logger.Debug("received exchanges request")

	response := dashboards.GetExchanges()

	return &dpgen.ExchangesResponse{Exchanges: response}, nil
}

func (s *dashboardsServer) GetOhlcvDiff(
	ctx context.Context, in *dpgen.BasicRequest) (*dpgen.OhlcvDiffResponse, error) {
	s.Logger.Debug("received price diff request", slog.Any("request", in))

	response := dpgen.OhlcvDiffResponse{}

	serializedRequest, err := serializers.NewBasicRequest(in)
	if err != nil {
		s.Logger.Error("unable to process the request", slog.String("err", err.Error()))
		return nil, status.Error(codes.InvalidArgument, "unable to process the request")
	}

	diffsChan, err := dashboards.GetPriceDiff(dashboards.PriceDiffFilter{
		Exchanges:   serializedRequest.Exchanges,
		Markets:     serializedRequest.Markets,
		StartTime:   serializedRequest.StartTime,
		EndTime:     serializedRequest.EndTime,
		WindowSize:  serializedRequest.WindowSize,
		Granularity: ParseGranularity(serializedRequest.StartTime, serializedRequest.EndTime),
	}, s.Storage, s.Logger, ctx)
	if err != nil {
		s.Logger.Error("unable to process the request", slog.String("err", err.Error()))
		return nil, status.Error(codes.Internal, "unable to process the request")
	}

	errors := 0
	for diff := range diffsChan {
		if diff.Err != nil {
			s.Logger.Error("error processing price diff", slog.String("err", diff.Err.Error()))
			errors++
			continue
		}

		response.Points = append(response.Points, &dpgen.OhlcvDiff{
			Timestamp:   diff.Timestamp.UnixMilli(),
			Market:      diff.Market,
			Avg:         ShieldZeros(diff.Price),
			VolumeToken: ShieldZeros(diff.VolumeToken),
		})
	}

	if errors != 0 && float64(errors/len(response.Points)) > float64(s.maxDataCorrupt/100) {
		s.Logger.Error("too many errors processing price diff")
		return nil, status.Error(codes.Internal, "unable to process the request")
	}

	return &response, nil
}
