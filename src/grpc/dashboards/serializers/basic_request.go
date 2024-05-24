package serializers

import (
	"fmt"
	"time"

	dpgen "github.com/veska-io/proto-dashboards-public/gen/go/dashboards"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BasicRequest struct {
	Exchanges  []string
	Markets    []string
	StartTime  time.Time
	EndTime    time.Time
	WindowSize int32
}

func NewBasicRequest(in *dpgen.BasicRequest) (*BasicRequest, error) {
	request := BasicRequest{
		Exchanges:  ParseRepeatedValue(in.GetExchanges()),
		Markets:    ParseRepeatedValue(in.GetMarkets()),
		StartTime:  ParseTime(in.GetStart()),
		EndTime:    ParseTime(in.GetEnd()),
		WindowSize: parseWindowSize(in.GetWindowSize(), in.GetWindowUnit()),
	}

	err := ValidateWindowSize(request.WindowSize, request.StartTime, request.EndTime)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &request, nil
}

func parseWindowSize(i int32, windowUnit dpgen.WindowUnit) int32 {
	if windowUnit == dpgen.WindowUnit_DAYS {
		return i * 24
	}
	if windowUnit == dpgen.WindowUnit_WEEKS {
		return i * 24 * 7
	}
	if windowUnit == dpgen.WindowUnit_MONTHS {
		return i * 24 * 30
	}

	return i
}

func ValidateWindowSize(windowSize int32, start, end time.Time) error {
	pointsOnScreen := 168
	diffHours := end.Sub(start).Hours()
	err := fmt.Errorf("too small window size for the selected time range")

	if diffHours <= float64(pointsOnScreen) {
		if windowSize < 1 {
			return err
		}
	} else if diffHours <= float64(pointsOnScreen*4) {
		if windowSize < 4 {
			return err
		}
	} else if diffHours <= float64(pointsOnScreen*8) {
		if windowSize < 8 {
			return err
		}
	} else if diffHours <= float64(pointsOnScreen*24) {
		if windowSize < 24 {
			return err
		}
	} else if diffHours <= float64(pointsOnScreen*24*7) {
		if windowSize < 24*7 {
			return err
		}
	} else {
		if windowSize < 24*30 {
			return err
		}
	}

	return nil
}
