from exchanges.apis.bitfinex import BitfinexApi
from exchanges.apis.binance import BinanceApi

from ratelimit import limits, sleep_and_retry
from candles.candles import Candles
from loguru import logger

import arrow
import datetime
import math
import sys

IS_PYTEST = "pytest" in sys.modules


def get_sync_candles_class(exchange, symbol, interval, start=None, end=None, host=None):
    """ Abstraction layer that returns an instance of the right SyncCandles class """
    if exchange.lower() == "bitfinex":
        return SyncBitfinexCandles(symbol, interval, start, end, host)
    if exchange.lower() == "binance":
        return SyncBinanceCandles(symbol, interval, start, end, host)


class BaseSyncCandles(object):
    """ Base class for syncing candles
        NOTE candles must come in oldest->newest order. Thanks.
    """

    BIN_SIZES = {"1m": 1, "1h": 60, "1d": 1440}
    API_MAX_RECORDS = 10000
    EXCHANGE = None
    DEFAULT_SYNC_DAYS = 90
    start = end = client = None

    def __init__(self, symbol, interval, start=None, end=None, host=None):
        self.candle_order = None
        self.symbol = symbol
        self.interval = interval
        if start:
            self.start = arrow.get(start).float_timestamp
        if end:
            self.end = arrow.get(end).float_timestamp

        self.influx_client = Candles(self.EXCHANGE, self.symbol, self.interval, create_if_missing=True, host=host)
        self.client = self.api_client()

    def api_client(self):
        "Abstract Method: must be implemented in the child class, and populate self.client " ""
        raise NotImplementedError

    def call_api(self):
        "Abstract Method: must be implemented in the child class " ""
        raise NotImplementedError

    def get_earliest_latest_timestamps_in_db(self):
        """ Returns (earliest,latest) timestamp in the database for the current symbol/interval, or 0 if there
            isn't one
        """

        query = "SELECT open,time FROM candles_{} WHERE symbol=$symbol".format(self.interval)
        params = {"symbol": self.symbol}

        latest = self.influx_client.query(query + " ORDER BY time DESC LIMIT 1", bind_params=params)
        earliest = self.influx_client.query(query + " ORDER BY time ASC LIMIT 1", bind_params=params)

        earliest = arrow.get(list(earliest)[0][0]["time"] / 1000).float_timestamp if earliest else 0
        latest = arrow.get(list(latest)[0][0]["time"] / 1000).float_timestamp if latest else 0
        return earliest, latest

    def get_iterations_for_range(self, batch_limit):
        """ Given a start, end timstamp, return the incremental steps required to fill in data, for
            a specified batch_limit, along with a new start and end time.
            If the start is earlier than the earliest in the DB, it will return the first range so that
            the beginning of the gap is filled, and also return fetch_again_from_ts=latest so the caller
            knows to fetch again from there, to fill in the missing "latest to now" data as well.
        """
        assert self.start and self.end, "Start and End times are not defined!"

        new_start = self.start
        new_end = self.end
        delta_mins = 0
        fetch_again_from_ts = False
        earliest, latest = self.get_earliest_latest_timestamps_in_db()

        if latest == 0:  # no existing data
            delta_mins = abs((self.end - self.start)) / 60
        elif self.start < earliest:
            delta_mins = abs(self.start - earliest) / 60  # only from start to earliest
            new_end = earliest
            fetch_again_from_ts = latest
        else:
            delta_mins = abs((latest - (self.end))) / 60
            new_start = latest
        data_to_fetch = math.ceil(delta_mins / self.BIN_SIZES[self.interval])
        return math.ceil(data_to_fetch / batch_limit), new_start, new_end, fetch_again_from_ts

    def write_candles(self, candles, extra_tags=None):
        """ Writes candle data to influxdb. """
        out = []
        tags = {"symbol": self.symbol, "interval": self.interval}

        if extra_tags:
            assert "symbol" not in extra_tags, "Not allowed to override symbol when you've already instantiated"
            " the class with a specific symbol."
            assert "interval" not in extra_tags, "Not allowed to override interval when you've already instantiated"
            " the class with a specific interval."

            tags.update(extra_tags)

        for c in candles:
            _open = float(c[self.candle_order["open"]])
            _high = float(c[self.candle_order["high"]])
            _low = float(c[self.candle_order["low"]])
            _close = float(c[self.candle_order["close"]])
            _volume = float(c[self.candle_order["volume"]])
            assert _low <= _high, f"Low price must be <= the High price. Candle: {c}"
            assert _low <= _close, f"Low price must be <= the Close price. Candle: {c}"
            assert _high >= _open, f"High price must be <= the Open price. Candle: {c}"
            out.append(
                {
                    "measurement": "candles_" + self.interval,
                    "tags": tags,
                    "time": c[0],
                    "fields": {"open": _open, "high": _high, "low": _low, "close": _close, "volume": _volume},
                }
            )

        self.influx_client.write_points(out)

    @staticmethod
    def timestamp_ranges(start, end, steps):
        """ Returns a list of timestamps for the (start,end) range, representing the steps needed to
            iterate through. You'll need to iterate pairwise... use a magic zip:
            >>> zip([1,2,3,4,5,6], [1,2,3,4,5,6][1:])
            [(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]
        """
        start = arrow.get(start)
        end = arrow.get(end)
        if not steps:
            return start, end
        diff = (end - start) / steps
        for i in range(steps):
            yield (start + diff * i).timestamp
        yield end.timestamp

    def sync(self, endpoint, extra_params={}, extra_tags=None, start_format=None, end_format=None):
        """ Pulls data from the exchange, and assumes it takes params: limit, start, end
            extra_params: will be added to each exchange request
            extra_tags: added to each measurement written to influx
        """
        assert not any(key in extra_params for key in ["limit", "start", "end"]), "Cannot"
        " override the following params: limit, start, end"

        if self.start and not self.end:
            self.end = datetime.datetime.now().timestamp()

        if not self.start and not self.end:
            now = datetime.datetime.now()
            self.start = (now - datetime.timedelta(days=self.DEFAULT_SYNC_DAYS)).timestamp()
            self.end = now.timestamp()

        steps, start, end, fetch_again = self.get_iterations_for_range(self.API_MAX_RECORDS)
        if start > end:
            logger.debug(
                f"Nothing to sync, as we have already have {arrow.get(start).isoformat()} in the database, "
                f"and end date {arrow.get(end).isoformat()} was selected."
            )
            return
        logger.debug("Using the following time ranges to complete the sync: {} to {}".format(start, end))

        time_steps = list(self.timestamp_ranges(start, end, steps))
        self.do_fetch(
            time_steps, start, end, endpoint, extra_params, extra_tags, start_format=start_format, end_format=end_format
        )

        # Date requested was before latest in the db, so the first fetch grabbed start->earliest_in_db,
        # and this one will grab latests_in_db->now.
        if fetch_again:
            logger.debug("Fetching again, this time from the latest in the db, to now()")
            self.start = fetch_again
            self.end = datetime.datetime.now().timestamp()
            steps, start, end, _ = self.get_iterations_for_range(self.API_MAX_RECORDS)
            time_steps = list(self.timestamp_ranges(start, end, steps))
            self.do_fetch(
                time_steps,
                start,
                end,
                endpoint,
                extra_params,
                extra_tags,
                start_format=start_format,
                end_format=end_format,
            )

    def do_fetch(self, time_steps, start, end, endpoint, extra_params, extra_tags, start_format=None, end_format=None):
        first_iteration = True
        for start, end in zip(time_steps, time_steps[1:]):
            params = {"limit": self.API_MAX_RECORDS, "start": start * 1000, "end": end * 1000}
            if start_format and end_format:
                params = {"limit": self.API_MAX_RECORDS, start_format: start * 1000, end_format: end * 1000}

            if extra_params:
                params.update(extra_params)

            logger.debug(
                "Pulling {} from {} for {} from {} to {}".format(
                    endpoint,
                    self.EXCHANGE,
                    self.symbol,
                    arrow.get(start).format("YYYY-MM-DD HH:mm:ss"),
                    arrow.get(end).format("YYYY-MM-DD HH:mm:ss"),
                )
            )

            res = self.call_api(endpoint, params)
            # ensure at least the first candle exists, to avoid making a bunch of pointless API calls
            if first_iteration:
                assert res, (
                    "Did not receive any candles from the start of the requested range!"
                    " Did it exist on the exchange at the start time?"
                )
                # check we got a candle within 10 mins of the start date (sometimes exchanges don't have exact
                # minute boundaries
                assert abs(res[0][0] - (start * 1000)) < 600000, (
                    "Did not receive a first candle within 10 mins of the start of the requested range!"
                    f" Did it exist on the exchange at the start time? Got: {res[0][0]} wanted: {start * 1000}"
                )
            first_iteration = False
            self.write_candles(res, extra_tags)


class SyncBinanceCandles(BaseSyncCandles):
    """ Sync candles for Binance """

    DEFAULT_SYNC_DAYS = 90
    API_MAX_RECORDS = 1000
    API_CALLS_PER_MIN = 100000 if IS_PYTEST else 1200
    EXCHANGE = "binance"

    def api_client(self):
        if not self.client:
            # Cache/reuse the client object so that sessions are re-used, which enables HTTP Keep-Alive
            self.client = BinanceApi()
        return self.client

    @sleep_and_retry
    @limits(calls=API_CALLS_PER_MIN, period=60)  # calls per minute
    def call_api(self, endpoint, params):
        """ Binance specific rate limits and brequest """
        return self.client.brequest(api_version=3, endpoint=endpoint, params=params)

    def pull_data(self):
        self.candle_order = {"open": 1, "high": 2, "low": 3, "close": 4, "volume": 5}
        endpoint = "klines"
        self.sync(
            endpoint,
            extra_params={"interval": self.interval, "symbol": self.symbol},
            start_format="startTime",
            end_format="endTime",
        )


class SyncBitfinexCandles(BaseSyncCandles):
    """ Sync candles for Bitfinex """

    DEFAULT_SYNC_DAYS = 90
    API_MAX_RECORDS = 10000
    API_CALLS_PER_MIN = 100000 if IS_PYTEST else 60
    EXCHANGE = "bitfinex"
    PERIODS = [f"p{n}" for n in range(2, 31)]

    def api_client(self):
        if not self.client:
            # Cache/reuse the client object so that sessions are re-used, which enables HTTP Keep-Alive
            self.client = BitfinexApi()
        return self.client

    def get_earliest_latest_timestamps_in_db(self):
        """ Overriding base class, as we need to include the period for bitfinex lending data) """
        query = "SELECT open,time FROM candles_{} WHERE symbol='{}'".format(self.interval, self.symbol)

        if self.symbol.startswith("f"):
            latest = self.influx_client.query(
                query + " AND period=$period ORDER BY time DESC LIMIT 1", bind_params={"period": str(self.cur_period)}
            )
            earliest = self.influx_client.query(
                query + " AND period=$period ORDER BY time ASC LIMIT 1", bind_params={"period": str(self.cur_period)}
            )
        else:
            latest = self.influx_client.query(query + " ORDER BY time DESC LIMIT 1")
            earliest = self.influx_client.query(query + " ORDER BY time ASC LIMIT 1")

        earliest = arrow.get(list(earliest)[0][0]["time"] / 1000).float_timestamp if earliest else 0
        latest = arrow.get(list(latest)[0][0]["time"] / 1000).float_timestamp if latest else 0
        return earliest, latest

    @sleep_and_retry
    @limits(calls=API_CALLS_PER_MIN, period=60)  # calls per minute
    def call_api(self, endpoint, params):
        """ Bitfinex specific rate limits and brequest """
        return self.client.brequest(api_version=2, endpoint=endpoint, params=params)

    def pull_data(self):
        self.candle_order = {"open": 1, "close": 2, "high": 3, "low": 4, "volume": 5}
        if self.symbol.startswith("f"):
            return self.pull_data_funding()
        else:
            return self.pull_data_trading()

    def pull_data_trading(self):
        assert self.symbol.startswith("t"), "Bitfinex trading symbols must start with 't'"

        endpoint = "candles/trade:{interval}:{symbol}/hist".format(interval=self.interval, symbol=self.symbol)
        self.sync(endpoint, extra_params={"sort": 1})  # get oldest candles first
        return True

    def pull_data_funding(self):
        assert self.symbol.startswith("f"), "Bitfinex funding symbols must start with 'f'"

        extra_tags = {}
        self.cur_period = ""

        logger.info("Syncing candles for {} for {}".format(self.EXCHANGE, self.symbol))

        for period in self.PERIODS:
            self.cur_period = period  # used in get_earliest_latest_timestamps_in_db()
            extra_tags["period"] = period
            endpoint = "candles/trade:{interval}:{symbol}:{period}/hist".format(
                interval=self.interval, symbol=self.symbol, period=period
            )

            self.sync(endpoint, extra_tags=extra_tags)
        return True
