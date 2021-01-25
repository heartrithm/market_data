from freezegun import freeze_time
from requests_mock import mock, ANY
from candles.candles import Candles
from candles.sync_candles import get_sync_candles_class
from loguru import logger
import arrow
import re


class TestSyncSFOXCandles:
    def test_pull_candles(self):
        exchange = "sfox"
        symbol = "BTCUSD"
        interval = "1m"
        start = 1611594060
        end = 1611595781
        db = "test_" + exchange

        influx = Candles(exchange, symbol, interval, create_if_missing=True)

        if [x for x in influx.client.get_list_database() if x["name"] == db]:
            assert db.startswith(
                "test_"
            ), "DB name doesn't start with 'test_'; aborting to avoid dropping the prod db..."
            logger.info("dropping existing database: %s" % db)
            influx.client.drop_database(db)
        influx.client.create_database(db)

        btc_candles = open("tests/data/candles_sfox_btcusd.json", "r").read()
        with freeze_time(arrow.get(end).datetime):
            # load all data in, insert in influx, verify rows count
            client = get_sync_candles_class(exchange=exchange, symbol=symbol, interval=interval, start=start)
            with mock() as m:
                m.register_uri(
                    "GET", re.compile(r"chartdata.sfox.com\/.*"), text=btc_candles,
                )
                m.register_uri(ANY, re.compile(r"localhost:8086.*"), real_http=True)
                client.pull_data()
            assert len(influx.get("*")) == 29

        influx.client.query("DROP SERIES FROM /.*/")
        with freeze_time(arrow.get(end).datetime):
            # verify start/end dates are sent to exchange properly:
            client = get_sync_candles_class(exchange=exchange, symbol=symbol, interval=interval, start=start, end=end)
            with mock() as m:
                m.register_uri("GET", re.compile(r"chartdata.sfox.com\/.*"), text=btc_candles)
                m.register_uri(ANY, re.compile(r"localhost:8086.*"), real_http=True)
                client.pull_data()
            reqs = [x for x in m.request_history if x.hostname.startswith("chartdata.sfox.com")]
            exchange_req = reqs[0]  # this only takes 1 query on sfox
            # NOTE: we aren't dividing by 1000 here, as it isn't being fetched form influx with precision=ms.
            # Exchange data is seconds.
            assert int(arrow.get(float(exchange_req.qs["starttime"][0])).timestamp) == start
            assert int(arrow.get(float(exchange_req.qs["endtime"][0])).timestamp) == end


class TestSyncBitfinexCandles:
    def test_pull_candles(self):
        exchange = "bitfinex"
        symbol = "fUSD"
        interval = "1m"
        start = 1590889920.0
        end = 1590891120.0
        db = "test_" + exchange

        influx = Candles(exchange, symbol, interval, create_if_missing=True)

        if [x for x in influx.client.get_list_database() if x["name"] == db]:
            assert db.startswith(
                "test_"
            ), "DB name doesn't start with 'test_'; aborting to avoid dropping the prod db..."
            logger.info("dropping existing database: %s" % db)
            influx.client.drop_database(db)
        influx.client.create_database(db)

        funding_candles = open("tests/data/candles_fUSD.json", "r").read()
        with freeze_time(arrow.get(end).datetime):
            # load all data in, insert in influx, verify rows count
            client = get_sync_candles_class(exchange=exchange, symbol=symbol, interval=interval, start=start)
            with mock() as m:
                m.register_uri(
                    "GET", re.compile(r"api-pub.bitfinex.com\/v2.*"), text=funding_candles,
                )
                m.register_uri(ANY, re.compile(r"localhost:8086.*"), real_http=True)
                client.pull_data()
            assert len(influx.get("*")) == 290

        influx.client.query("DROP SERIES FROM /.*/")
        with freeze_time(arrow.get(end).datetime):
            # verify start/end dates are sent to exchange properly:
            client = get_sync_candles_class(exchange=exchange, symbol=symbol, interval=interval, start=start, end=end)
            with mock() as m:
                m.register_uri("GET", re.compile(r"api-pub.bitfinex.com\/v2.*"), text=funding_candles)
                m.register_uri(ANY, re.compile(r"localhost:8086.*"), real_http=True)
                client.pull_data()
            reqs = [x for x in m.request_history if x.hostname.startswith("api-pub.bitfinex")]
            first_exchange_req = reqs[0]
            last_exchange_req = reqs[1]
            assert arrow.get(float(first_exchange_req.qs["start"][0]) / 1000).timestamp == start
            assert arrow.get(float(last_exchange_req.qs["end"][0]) / 1000).timestamp == end

        start = 1590889920.0
        end = 1590890040.0
        influx.client.query("DROP SERIES FROM /.*/")
        with freeze_time(arrow.get(end).datetime):
            # with only start specified, verify end is now() - the frozen time
            client = get_sync_candles_class(exchange=exchange, symbol=symbol, interval=interval, start=start)
            with mock() as m:
                m.register_uri("GET", re.compile(r"api-pub.bitfinex.com\/v2.*"), text=funding_candles)
                m.register_uri(ANY, re.compile(r"localhost:8086.*"), real_http=True)
                client.pull_data()
            reqs = [x for x in m.request_history if x.hostname.startswith("api-pub.bitfinex")]
            first_exchange_req = reqs[0]
            last_exchange_req = reqs[1]
            assert arrow.get(float(first_exchange_req.qs["start"][0]) / 1000).timestamp == start
            assert arrow.get(float(last_exchange_req.qs["end"][0]) / 1000).timestamp == end


class TestCandles:
    def test_get(self):
        exchange = "bitfinex"
        symbol = "fUSD"
        interval = "1m"
        influx = Candles(exchange, symbol, interval)

        start = 1590889920.0
        end = 1590891120.0
        fh = open("tests/data/lowhigh_candles.json", "r")
        with mock() as m:
            m.register_uri("GET", re.compile(r"localhost:8086"), text=fh.read())
            res = influx.get("*")
            assert res is not None
            res = influx.get("*", start=start)
            assert res is not None
            res = influx.get("*", end=end)
            assert res is not None
            res = influx.get("*", start=start, end=end)
            assert res is not None

    def test_binance_get(self):
        exchange = "binance"
        symbol = "ETHUSDT"
        interval = "1m"
        influx = Candles(exchange, symbol, interval)

        start = 1590889920.0
        end = 1590891120.0
        fh = open("tests/data/lowhigh_candles.json", "r")
        with mock() as m:
            m.register_uri("GET", re.compile(r"localhost:8086"), text=fh.read())
            res = influx.get("*")
            assert res is not None
            res = influx.get("*", start=start)
            assert res is not None
            res = influx.get("*", end=end)
            assert res is not None
            res = influx.get("*", start=start, end=end)
            assert res is not None

    def test_sfox_get(self):
        exchange = "sfox"
        symbol = "ETHUSD"
        interval = "1m"
        influx = Candles(exchange, symbol, interval)

        start = 1590889920.0
        end = 1590891120.0
        fh = open("tests/data/lowhigh_candles.json", "r")
        with mock() as m:
            m.register_uri("GET", re.compile(r"localhost:8086"), text=fh.read())
            res = influx.get("*")
            assert res is not None
            res = influx.get("*", start=start)
            assert res is not None
            res = influx.get("*", end=end)
            assert res is not None
            res = influx.get("*", start=start, end=end)
            assert res is not None

    def test_get_percentile(self):
        """ Test percentile query """
        exchange = "bitfinex"
        symbol = "fUSD"
        interval = "1m"
        influx = Candles(exchange, symbol, interval)

        start = 1590889920.0
        end = 1590891120.0
        res = influx.get_percentile("high", 95)
        assert res > 0
        res = influx.get_percentile("close", 95, start=start, end=end)
        assert res > 0

    def test_get_lowhigh(self):
        """ Test fetching actual data that was populated in previous tests (of syncs) """
        exchange = "bitfinex"
        symbol = "fUSD"
        interval = "1m"
        influx = Candles(exchange, symbol, interval)

        start = 1590889920.0
        end = 1590891120.0
        low, high = influx.get_lowhigh()
        assert low > 0.0 and high > 0.0

        # exercise code path for start/end dates specified in each combination
        start = 1590889920.0
        end = None
        low, high = influx.get_lowhigh(start=start, end=end)
        assert low > 0.0 and high > 0.0

        start = None
        end = 1590891120.0
        low, high = influx.get_lowhigh(start=start, end=end)
        assert low > 0.0 and high > 0.0

        start = 1590889920
        end = 1590891120.0
        low, high = influx.get_lowhigh(start=start, end=end)
        assert low > 0.0 and high > 0.0
