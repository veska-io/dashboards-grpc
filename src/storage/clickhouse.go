package storage

import (
	"database/sql"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type Storage struct {
	logger *slog.Logger

	Conn *sql.DB
}

func New(Addr, Database, Username, Password string, logger *slog.Logger) *Storage {
	return &Storage{
		logger: logger,

		Conn: clickhouse.OpenDB(&clickhouse.Options{
			Addr:     []string{Addr},
			Protocol: clickhouse.HTTP,
			Auth: clickhouse.Auth{
				Database: Database,
				Username: Username,
				Password: Password,
			},
		}),
	}
}
