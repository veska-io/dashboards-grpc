package dashboards

import (
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"log/slog"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

//go:embed sql/price_diff.sql
var priceDiffTemplate string

type PriceDiffFilter struct {
	Exchanges  []string
	Markets    []string
	StartTime  time.Time
	EndTime    time.Time
	WindowSize int32
}

type PriceDiff struct {
	Timestamp time.Time
	Market    string
	Value     float64

	Err error
}

func GetPriceDiff(
	filter PriceDiffFilter,

	strg *sql.DB,
	logger *slog.Logger,
	ctx context.Context,
) (<-chan PriceDiff, error) {
	logger.Debug("executing query", slog.Any("filter", filter))

	responseChan := make(chan PriceDiff)

	diffQuery, err := BuildDiffQuery(filter)
	if err != nil {
		return nil, fmt.Errorf("unable to build query: %w", err)
	}

	rows, err := strg.QueryContext(ctx, diffQuery,
		clickhouse.Named("markets", filter.Markets),
		clickhouse.Named("startTime", filter.StartTime),
		clickhouse.Named("endTime", filter.EndTime),
		clickhouse.Named("exchanges", filter.Exchanges),
		clickhouse.Named("windowSize", filter.WindowSize),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to execute query: %w", err)
	}

	go func() {
		defer rows.Close()
		defer close(responseChan)

		for rows.Next() {
			select {
			case <-ctx.Done():
				responseChan <- PriceDiff{
					Err: fmt.Errorf("cancelled by context: %w", ctx.Err()),
				}
				return
			default:
				diff := PriceDiff{}
				if err := rows.Scan(&diff.Timestamp, &diff.Market, &diff.Value); err != nil {
					diff.Err = fmt.Errorf("unable to scan row: %w", err)
				}

				responseChan <- diff
			}
		}
	}()

	return responseChan, nil
}

func BuildDiffQuery(d PriceDiffFilter) (string, error) {
	var rawTmpl bytes.Buffer

	tmpl, err := template.New("price_diff.sql").Parse(priceDiffTemplate)
	if err != nil {
		return *new(string), fmt.Errorf("unable to parse template: %w", err)
	}

	err = tmpl.Execute(&rawTmpl, d)
	if err != nil {
		return *new(string), fmt.Errorf("unable to execute template: %w", err)
	}

	return rawTmpl.String(), nil
}
