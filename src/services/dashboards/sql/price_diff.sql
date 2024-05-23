WITH exchanges as (
	SELECT lower(arrayJoin(@exchanges)) as exchange
),
markets as (
	SELECT upper(arrayJoin(@markets)) as market 
),

raw_data_frame as (
	SELECT
		date_time,
		toStartOfInterval(date_time, INTERVAL {{ .Granularity }}) as grouped_datetime,
		exchange,
		market,
		groupArray(close)[1] as price
	FROM (
		SELECT
			*
		FROM
			candles_1h
		WHERE
			date_time >= date_sub(hour, 2*@windowSize, @startTime)
			AND date_time <= @endTime
			{{ with .Markets }} AND market in (SELECT market FROM markets) {{ end }}
			{{ with .Exchanges }} AND exchange in (SELECT exchange FROM exchanges) {{ end }}
		ORDER BY
			date_time DESC, updated_at DESC
	)

	GROUP BY date_time, exchange, market
),

exchange_data_frame as (
	SELECT
		grouped_datetime as datetime,
		exchange,
		market,
		avg(price) as price
	FROM
		raw_data_frame
	GROUP BY
		grouped_datetime, exchange, market
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
HAVING min2(c_cur, c_prev) / max2(c_cur, c_prev) > 0.9

ORDER BY time ASC