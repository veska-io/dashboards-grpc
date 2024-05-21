package dashboards

import (
	"bytes"
	_ "embed"
	"log/slog"
	"strings"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/veska-io/grpc-dashboards-public/src/storage"
	dpgen "github.com/veska-io/proto-dashboards-public/gen/go/dashboards"
)

//go:embed price_diff.sql
var priceDiffTemplate string

func GetPriceDiff(r *dpgen.BasicRequest, strg *storage.Storage, logger *slog.Logger) *dpgen.BasicResponse {
	var (
		rawTmpl  bytes.Buffer
		response dpgen.BasicResponse
	)

	tmpl, err := template.New("price_diff.sql").Parse(priceDiffTemplate)
	if err != nil {
		panic(err)
	}

	err = tmpl.Execute(&rawTmpl, nil)
	if err != nil {
		panic(err)
	}

	startTime := time.Unix(0, r.GetStart()*int64(time.Millisecond))
	endTime := time.Unix(0, r.GetEnd()*int64(time.Millisecond))
	e := strings.Replace(r.GetExchanges()[0].String(), "_", "-", -1)

	rows, err := strg.Conn.Query(rawTmpl.String(),
		clickhouse.Named("markets", r.GetMarkets()),
		clickhouse.Named("startTime", startTime),
		clickhouse.Named("endTime", endTime),
		clickhouse.Named("exchange", strings.ToLower(e)),
		clickhouse.Named("windowSize", r.GetWindowSize()),
	)
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	for rows.Next() {
		var (
			point    dpgen.Point
			datetime time.Time
		)

		if err := rows.Scan(&datetime, &point.Market, &point.Value); err != nil {
			panic(err)
		}

		point.Timestamp = datetime.UnixNano() / int64(time.Millisecond)

		response.Points = append(response.Points, &point)
	}

	return &response
}
