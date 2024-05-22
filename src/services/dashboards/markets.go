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

//go:embed sql/markets.sql
var marketsTemplate string

type MarketsFilter struct {
	Exchanges  []string
	Markets    []string
	StartTime  time.Time
	EndTime    time.Time
	WindowSize int32
}

type Market struct {
	Name string

	Err error
}

func GetMarkets(
	filter MarketsFilter,

	strg *sql.DB,
	logger *slog.Logger,
	ctx context.Context,
) (<-chan Market, error) {
	responseChan := make(chan Market)

	marketsQuery, err := BuildMarketsQuery(filter)
	if err != nil {
		return nil, fmt.Errorf("unable to build query: %w", err)
	}

	rows, err := strg.QueryContext(ctx, marketsQuery,
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
				responseChan <- Market{
					Err: fmt.Errorf("cancelled by context: %w", ctx.Err()),
				}
				return
			default:
				market := Market{}
				if err := rows.Scan(&market.Name); err != nil {
					market.Err = fmt.Errorf("unable to scan row: %w", err)
				}

				responseChan <- market
			}
		}
	}()

	return responseChan, nil
}

func BuildMarketsQuery(d MarketsFilter) (string, error) {
	var rawTmpl bytes.Buffer

	tmpl, err := template.New("markets.sql").Parse(marketsTemplate)
	if err != nil {
		return *new(string), fmt.Errorf("unable to parse template: %w", err)
	}

	err = tmpl.Execute(&rawTmpl, d)
	if err != nil {
		return *new(string), fmt.Errorf("unable to execute template: %w", err)
	}

	return rawTmpl.String(), nil
}
