package serializers

import "time"

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
