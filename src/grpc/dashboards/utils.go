package dbrds_grpc

import "time"

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

func ShieldZeros(v float64) float64 {
	if v == 0 {
		return 0.00000000000000001
	}

	return v
}
