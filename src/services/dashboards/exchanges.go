package dashboards

const (
	BINANCE = 0
	BYBIT   = 1
	DYDX_V3 = 2
	DYDX_V4 = 3
)

var exchangesNames = []string{
	"Binance",
	"ByBit",
	"dYdX-v3",
	"dYdX-v4",
}

func GetExchanges() []string {
	return exchangesNames
}
