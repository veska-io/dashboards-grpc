package dbrds_grpc

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

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
	s.Logger.Debug("received markets request")

	response := dpgen.MarketsResponse{}

	marketsChan, err := dashboards.GetMarkets(dashboards.MarketsFilter{
		Exchanges:  ParseRepeatedValue(in.GetExchanges()),
		StartTime:  ParseTime(in.GetStart()),
		EndTime:    ParseTime(in.GetEnd()),
		WindowSize: in.GetWindowSize(),
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

func (s *dashboardsServer) GetPriceDiff(
	ctx context.Context, in *dpgen.BasicRequest) (*dpgen.BasicResponse, error) {
	s.Logger.Debug("received price diff request")

	response := dpgen.BasicResponse{}
	err := ValidateWindowSize(in.GetWindowSize(), ParseTime(in.GetStart()), ParseTime(in.GetEnd()))
	if err != nil {
		s.Logger.Error("validation error", slog.String("err", err.Error()))
		return nil, status.Error(codes.Internal, err.Error())
	}

	diffsChan, err := dashboards.GetPriceDiff(dashboards.PriceDiffFilter{
		Exchanges:   ParseRepeatedValue(in.GetExchanges()),
		Markets:     ParseRepeatedValue(in.GetMarkets()),
		StartTime:   ParseTime(in.GetStart()),
		EndTime:     ParseTime(in.GetEnd()),
		WindowSize:  in.GetWindowSize(),
		Granularity: ParseGranularity(ParseTime(in.GetStart()), ParseTime(in.GetEnd())),
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

		response.Points = append(response.Points, &dpgen.Point{
			Timestamp: diff.Timestamp.UnixMilli(),
			Market:    diff.Market,
			Value:     diff.Value,
		})
	}

	if errors != 0 && float64(errors/len(response.Points)) > float64(s.maxDataCorrupt/100) {
		s.Logger.Error("too many errors processing price diff")
		return nil, status.Error(codes.Internal, "unable to process the request")
	}

	return &response, nil
}

func ParseTime(t int64) time.Time {
	rawDate := time.Unix(0, t*int64(time.Millisecond))
	toHourDate := time.Date(
		rawDate.Year(),
		rawDate.Month(),
		rawDate.Day(),
		rawDate.Hour(),
		0, 0, 0,
		time.UTC,
	)

	return toHourDate
}

func ParseRepeatedValue(e []string) []string {
	if len(e) == 1 && e[0] == "-1" {
		return []string{}
	}

	return e
}

func ParseGranularity(start, end time.Time) string {
	pointsOnScreen := 168

	diffHours := end.Sub(start).Hours()

	if diffHours <= float64(pointsOnScreen) {
		return "1 hour"
	} else if diffHours <= float64(pointsOnScreen*4) {
		return "4 hour"
	} else if diffHours <= float64(pointsOnScreen*8) {
		return "8 hour"
	} else if diffHours <= float64(pointsOnScreen*24) {
		return "1 day"
	} else if diffHours <= float64(pointsOnScreen*24*7) {
		return "7 day"
	} else {
		return "1 month"
	}
}

func ValidateWindowSize(windowSize int32, start, end time.Time) error {
	pointsOnScreen := 168
	diffHours := end.Sub(start).Hours()

	if diffHours <= float64(pointsOnScreen) {
		if windowSize < 1 {
			return fmt.Errorf("too small window size for the selected time range")
		}
	} else if diffHours <= float64(pointsOnScreen*4) {
		if windowSize < 4 {
			return fmt.Errorf("too small window size for the selected time range")
		}
	} else if diffHours <= float64(pointsOnScreen*8) {
		if windowSize < 8 {
			return fmt.Errorf("too small window size for the selected time range")
		}
	} else if diffHours <= float64(pointsOnScreen*24) {
		if windowSize < 24 {
			return fmt.Errorf("too small window size for the selected time range")
		}
	} else if diffHours <= float64(pointsOnScreen*24*7) {
		if windowSize < 24*7 {
			return fmt.Errorf("too small window size for the selected time range")
		}
	} else {
		if windowSize < 24*30 {
			return fmt.Errorf("too small window size for the selected time range")
		}
	}

	return nil
}
