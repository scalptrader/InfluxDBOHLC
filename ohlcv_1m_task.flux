parameters = { start: -3m, every: 1m, limit: 3, bucket: "marketdata", org: "test"}

dataset = from(bucket: parameters.bucket)
	|> range(start: parameters.start)
	|> filter(fn: (r) =>
		(r["_measurement"] == "tradedata"))
	|> filter(fn: (r) =>
		(r["_field"] == "Size" or r["_field"] == "Last"))

dataset
	|> filter(fn: (r) =>
		(r["_field"] == "Size"))
	|> aggregateWindow(every: parameters.every, fn: sum)
	|> map(fn: (r) =>
		({
			_time: r._time,
			_field: "volume",
			_measurement: r._measurement,
			_value: r._value,
			topic: r.topic,
			symbol: r.symbol,
		}))
	|> limit(n: parameters.limit)
	|> to(bucket: "ohlc_1m_data", org: "test")
dataset
	|> filter(fn: (r) =>
		(r["_field"] == "Last"))
	|> aggregateWindow(every: parameters.every, fn: first)
	|> map(fn: (r) =>
		({
			_time: r._time,
			_field: "open",
			_measurement: r._measurement,
			_value: r._value,
			topic: r.topic,
			symbol: r.symbol,
		}))
	|> limit(n: parameters.limit)
	|> to(bucket: "ohlc_1m_data", org: "test")
dataset
	|> filter(fn: (r) =>
		(r["_field"] == "Last"))
	|> aggregateWindow(every: parameters.every, fn: min)
	|> map(fn: (r) =>
		({
			_time: r._time,
			_field: "low",
			_measurement: r._measurement,
			_value: r._value,
			topic: r.topic,
			symbol: r.symbol,
		}))
	|> limit(n: parameters.limit)
	|> to(bucket: "ohlc_1m_data", org: "test")
dataset
	|> filter(fn: (r) =>
		(r["_field"] == "Last"))
	|> aggregateWindow(every: parameters.every, fn: max)
	|> map(fn: (r) =>
		({
			_time: r._time,
			_field: "high",
			_measurement: r._measurement,
			_value: r._value,
			topic: r.topic,
			symbol: r.symbol,
		}))
	|> limit(n: parameters.limit)
	|> to(bucket: "ohlc_1m_data", org: "test")
dataset
	|> filter(fn: (r) =>
		(r["_field"] == "Last"))
	|> aggregateWindow(every: parameters.every, fn: last)
	|> map(fn: (r) =>
		({
			_time: r._time,
			_field: "close",
			_measurement: r._measurement,
			_value: r._value,
			topic: r.topic,
			symbol: r.symbol,
		}))
	|> limit(n: parameters.limit)
	|> to(bucket: "ohlc_1m_data", org: "test")

