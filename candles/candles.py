from decimal import Decimal as D
import os
import sys

from influxdb import InfluxDBClient
from loguru import logger
import arrow

IS_PYTEST = "pytest" in sys.modules


class Candles(object):
    """For interacting with candle data

    ALL results returned in milliseconds. Parse with arrow.get(ts / 1000), and use .float_timestamp to get back
    millis.

    If you are using a query that compares dates, you MUST send then in nanos. arrow.get(ts).float_timestamp * 1e9

    TODO(charlie): refactor market_data so instead of passing a magic string that says "not really candles," we
    instead make these bits a more abstract DataSync class, with children CandleSync and RatesSync..
    """

    INFLUX_TIMEOUT = 60

    def __init__(self, exchange, symbol, interval, create_if_missing=False, host=None, data_type="candles"):
        self.exchange = exchange.lower()
        self.symbol = symbol
        self.interval = interval
        self.data_type = data_type
        if host:
            self.client = InfluxDBClient(
                host=host,
                port=os.getenv("CANDLES_DB_PORT", "8086"),
                timeout=self.INFLUX_TIMEOUT,
            )
        else:
            self.client = InfluxDBClient(
                host=os.getenv("CANDLES_DB_HOST", "localhost"),
                port=os.getenv("CANDLES_DB_PORT", "8086"),
                timeout=self.INFLUX_TIMEOUT,
            )
        if IS_PYTEST:
            db = "test_" + self.exchange
        else:
            db = os.getenv("CANDLES_DB_PREFIX", "") + self.exchange

        if create_if_missing or "test_" in db:
            if not [x for x in self.client.get_list_database() if x["name"] == exchange]:
                logger.warning(
                    "Influx Database '{}' not found. Creating. This is expected if you're using a new exchange.".format(
                        exchange
                    )
                )
                self.client.create_database(exchange)
                self.client.create_database(db)

        self.client.switch_database(db)

    def write_points(self, *args, **kwargs):
        return self.client.write_points(time_precision="ms", *args, **kwargs)

    def query(self, *args, **kwargs):
        """Wrapper for queries.
        Read the docs:
        https://influxdb-python.readthedocs.io/en/latest
        """
        return self.client.query(epoch="ms", *args, **kwargs)
        # return self.client.query(*args, **kwargs)

    def _time_parser(self, params={}, start=None, end=None):
        """Returns (params, query) to use in influx call.
        The query is just the portion where time comparison is happening, so you need to append it
        to your query.
        """
        if start and not end:
            start = arrow.get(start).float_timestamp * 1e9
            params.update({"start": start})
            return params, "time > $start"
        if end and not start:
            end = arrow.get(end).float_timestamp * 1e9
            params.update({"end": end})
            return params, "time < $end"
        elif start and end:
            start = arrow.get(start).float_timestamp * 1e9
            end = arrow.get(end).float_timestamp * 1e9
            params.update({"start": start, "end": end})
            return params, "time > $start AND time < $end"
        else:
            return params, ""

    def get(self, query, fetch_latest=False, start=None, end=None):
        """Simple method to get a specific key
        >>> get('*')
        SELECT * FROM candles_1m WHERE symbol='fUSD'
        """
        from candles.sync_candles import get_sync_candles_class

        query = f"SELECT {query} FROM {self.data_type}_{self.interval} WHERE symbol=$symbol"
        params = {"symbol": self.symbol}

        if fetch_latest:
            client = get_sync_candles_class(exchange=self.exchange, symbol=self.symbol, interval=self.interval)
            client.pull_data()

        if start or end:
            q_where = query + " AND "
        else:
            q_where = query

        params, time_q = self._time_parser(params, start, end)
        res = self.query(q_where + time_q, bind_params=params)
        res = list(res.get_points())
        if not res:
            return None
        return res

    def get_lowhigh(self, start=None, end=None):
        """Returns (low, high) for all candles in the provided date range"""
        # date comparison queries in influx must be sent in nanos:
        query = f"SELECT low, high FROM {self.data_type}_{self.interval} WHERE symbol=$symbol"
        params = {"symbol": self.symbol}
        if start or end:
            q_where = query + " AND "
        else:
            q_where = query

        params, time_q = self._time_parser(params, start, end)
        res = self.query(q_where + time_q, bind_params=params)

        res = list(res.get_points())
        if not res:
            return None, None

        lowest = D(min([x["low"] for x in res]))
        highest = D(max([x["high"] for x in res]))
        return lowest, highest

    def get_percentile(self, field, percentile, start=None, end=None):
        """Returns $percentile of the $field price for all candles in the provided date range"""
        params = {"symbol": self.symbol}
        query = f"SELECT PERCENTILE({field}, {percentile}) FROM {self.data_type}_{self.interval} WHERE symbol=$symbol"
        if start or end:
            q_where = query + " AND "
        else:
            q_where = query

        params, time_q = self._time_parser(params, start, end)
        res = self.query(q_where + time_q, bind_params=params)

        res = list(res.get_points())
        if not res:
            return None

        return res[0]["percentile"]  # it only returns one result, the percentile calculation result
