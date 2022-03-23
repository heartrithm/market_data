import re

import arrow
from freezegun import freeze_time
from loguru import logger
from requests_mock import mock, ANY

from candles.candles import Candles
from futures.sync_futures import get_sync_futures_class


class TestSyncFTXCandles:
    def test_pull_candles(self):
        exchange = "ftx"
        symbol = "BTC-PERP"
        interval = "1h"
        start = 1641484800
        end = 1643281200
        db = "test_" + exchange

        influx = Candles(exchange, symbol, interval, create_if_missing=True, data_type="futures")

        if [x for x in influx.client.get_list_database() if x["name"] == db]:
            assert db.startswith(
                "test_"
            ), "DB name doesn't start with 'test_'; aborting to avoid dropping the prod db..."
            logger.info("dropping existing database: %s" % db)
            influx.client.drop_database(db)
        influx.client.create_database(db)

        btc_rates = open("tests/data/futures_ftx_btc.json", "r").read()
        with freeze_time(arrow.get(end).datetime):
            # load all data in, insert in influx, verify rows count
            client = get_sync_futures_class(exchange=exchange, symbol=symbol, interval=interval, start=start)
            with mock() as m:
                m.register_uri("GET", re.compile(r"api.tardis.dev\/.*"), text=btc_rates)
                m.register_uri("GET", re.compile(r"ftx.com\/api.*"), text=btc_rates)
                m.register_uri(ANY, re.compile(r"localhost:8086.*"), real_http=True)
                client.pull_data()
            assert len(influx.get("*")) == 1

        influx.client.query("DROP SERIES FROM /.*/")
        with freeze_time(arrow.get(end).datetime):
            # verify start/end dates are sent to exchange properly:
            client = get_sync_futures_class(exchange=exchange, symbol=symbol, interval=interval, start=start, end=end)
            with mock() as m:
                m.register_uri("GET", re.compile(r"api.tardis.dev\/.*"), text=btc_rates)
                m.register_uri("GET", re.compile(r"ftx.com\/api.*"), text=btc_rates)
                m.register_uri(ANY, re.compile(r"localhost:8086.*"), real_http=True)
                client.pull_data()
            reqs = [x for x in m.request_history if x.hostname.startswith("ftx.com")]
            exchange_req = reqs[0]  # this only takes 1 query on ftx
            # NOTE: we aren't dividing by 1000 here, as it isn't being fetched from influx with precision=ms.
            # Exchange data is ISO8601 hourly, which loads into arrow as seconds.
            assert int(arrow.get(float(exchange_req.qs["start_time"][0])).timestamp()) == start
            assert int(arrow.get(float(exchange_req.qs["end_time"][0])).timestamp()) == end
