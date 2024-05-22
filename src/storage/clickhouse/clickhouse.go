package clickhouse

import (
	"database/sql"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func New(Addr, Database, Username, Password string, logger *slog.Logger) *sql.DB {
	return clickhouse.OpenDB(&clickhouse.Options{
		Addr:     []string{Addr},
		Protocol: clickhouse.HTTP,
		Auth: clickhouse.Auth{
			Database: Database,
			Username: Username,
			Password: Password,
		},
	})
}
