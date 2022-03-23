import json

import arrow

from candles.sync_candles import BaseSyncCandles
from exchanges.apis.tardis import TardisApi
from utils import get_aws_secret


class SyncHistorical(BaseSyncCandles):
    """ Class to enable syncing historical data via Tardis """

    AVAILABLE_FROM = "2020-05-13"
    DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"  # seconds resolution

    API_MAX_RECORDS = 1
    EXCHANGE = "ftx"

    def __init__(self, symbol, interval, start=None, end=None, host=None):
        super().__init__(
            symbol, interval, self.trim_date(start), self.trim_date(end), host, data_type="futures",
        )

    def trim_date(self, date_string):
        """No historical data prior to AVAILABLE_FROM
            https://docs.tardis.dev/historical-data-details/ftx#captured-real-time-channels
        """
        if not date_string:
            return None

        if arrow.get(self.AVAILABLE_FROM).timestamp() < arrow.get(date_string).timestamp():
            return date_string
        else:
            return self.AVAILABLE_FROM

    def api_client(self):
        if not self.client:
            self.client = TardisApi(get_aws_secret("tardis.dev").get("api_key"))
        return self.client

    def call_api(self, endpoint, params):
        res = self.client.brequest(1, endpoint=endpoint, params=params)
        if "nextFundingTime" in res:
            res["time"] = res["nextFundingTime"]  # helps us play nice with sync
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
