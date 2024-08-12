package dashboards_test

import (
	"testing"

	"github.com/veska-io/dashboards-grpc/src/services/dashboards"
)

func TestBuildDiffQuery(t *testing.T) {
	diffQuery, err := dashboards.BuildDiffQuery(dashboards.PriceDiffFilter{
		Exchanges: []string{},
		Markets:   []string{},
	})
	if err != nil {
		t.Error(err)
	}

	t.Log(diffQuery)
}
