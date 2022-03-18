import json

import arrow

from candles.sync_candles import BaseSyncCandles
from exchanges.apis.tardis import TardisApi
from utils import get_aws_secret


class SyncHistorical(BaseSyncCandles):
    """ Class to enable syncing historical data via Tardis """

    AVAILABLE_FROM = arrow.get("2019-08-01").timestamp()
    DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

    API_MAX_RECORDS = 1
    EXCHANGE = "ftx"

    def __init__(self, symbol, interval, start=None, end=None, host=None):
        super().__init__(symbol, interval, start, end, host, data_type="futures")
        self.start = max(self.start, self.AVAILABLE_FROM)
        self.end = max(self.end, self.AVAILABLE_FROM)

    def api_client(self):
        if not self.client:
            self.client = TardisApi(get_aws_secret("tardis.dev").get("api_key"))
        return self.client

    def call_api(self, endpoint, params):
        # is there a less hackish way to do this?
        params["from"] = arrow.get(params["from"]).strftime(self.DATE_FORMAT)
        params["to"] = arrow.get(params["to"]).strftime(self.DATE_FORMAT)
        res = self.client.brequest(1, endpoint=endpoint, params=params)
        return res

    def pull_data(self):
        self.candle_order = None
        endpoint = f"data-feeds/{self.EXCHANGE}"
        filters = [{"channel": "instrument", "symbols": [self.symbol]}]

        self.sync(
            endpoint,
            extra_params={"filters": json.dumps(filters)},
            extra_tags={"source": "tardis"},
            start_format="from",
            end_format="to",
            timestamp_units="s",
            result_key="result",
            merge_endpoint_results_dict=True,
        )
