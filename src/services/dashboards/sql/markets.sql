WITH exchanges as (
	SELECT lower(arrayJoin(@exchanges)) as exchange
),

exchange_data_frame as (
	SELECT
		date_time datetime,
		exchange,
		market
	FROM (
		SELECT
			*
		FROM
			candles_1h
		WHERE
			date_time >= date_sub(hour, 2*@windowSize, @startTime)
			AND date_time <= @endTime
			{{ with .Exchanges }} AND exchange in (SELECT exchange FROM exchanges) {{ end }}
		ORDER BY
			date_time DESC, updated_at DESC
	)

	GROUP BY date_time, exchange, market
)

SELECT market from exchange_data_frame group by market