WITH exchange_data_frame as (
	SELECT
		date_time datetime,
		exchange,
		market,
		groupArray(close)[1] as price
	FROM (
		SELECT
			*
		FROM
			candles_1h
		WHERE
			market in @markets
			AND
			exchange = @exchange
			AND
			date_time >= date_sub(hour, 2*@windowSize, @startTime)
			AND
			date_time <= @endTime
		ORDER BY
			date_time DESC, updated_at DESC
	)

	GROUP BY date_time, exchange, market
),

pre_load AS (
	SELECT	
		datetime,
		market,
		SUM(price) OVER (
			PARTITION BY market
			ORDER BY datetime ASC
			RANGE BETWEEN toUInt64((@windowSize-1)*60*60) PRECEDING AND CURRENT ROW
			) AS sum_cur,
		SUM(price) OVER (
			PARTITION BY market
			ORDER BY datetime ASC
			RANGE BETWEEN
				toUInt64((2*(@windowSize)*60*60)-(60*60)) PRECEDING
				AND
				toUInt64((@windowSize)*60*60) PRECEDING
			) AS sum_prev,
		COUNT(datetime) OVER (
			PARTITION BY market
			ORDER BY datetime ASC
			RANGE BETWEEN toUInt64((@windowSize-1)*60*60) PRECEDING AND CURRENT ROW
		) AS c_cur,
		COUNT(datetime) OVER (
			PARTITION BY market
			ORDER BY datetime ASC
			RANGE BETWEEN
				toUInt64((2*(@windowSize)*60*60)-(60*60)) PRECEDING
				AND
				toUInt64((@windowSize)*60*60) PRECEDING
			) AS c_prev
	FROM  exchange_data_frame
)

SELECT
	datetime as time,
	market,
	(GREATEST(0, sum_cur - sum_prev) - GREATEST(0, sum_prev - sum_cur)) / sum_prev as "_"
FROM pre_load
WHERE
	datetime >= @startTime

ORDER BY time ASC