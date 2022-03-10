from candles.sync_candles import BaseSyncCandles
from exchanges.apis.tardis import TardisApi

from utils import get_aws_secret


class SyncHistorical(BaseSyncCandles):
    """ Class to enable syncing historical data via Tardis """
    def __init__(exchange, symbol, interval, start=None, end=None):
        self.exchange = exchange
        self.symbol = symbol
        self.interval = interval
        self.start = start
        self.end = end

        self.client = TardisApi(get_aws_secret("tardis.dev"))

        # if you ask for early data, the API doesn't just give you what's available
        # HTTP/2 400, "code": 150   "message": "There is no data available for '2009-06-01T00:00:00.000Z' date ('from'+'offset' params combination). Data for 'ftx' exchange is available since: '2019-08-01T00:00:00.000Z'."
        # TODO parse that crap and re-send the request

    def call_api(self, endpoint, params):
        res = self.client.brequest(1, endpoint=endpoint, params=params)
        return res

    def pull_data(self):
        self.candle_order = None
        endpoint = f"data-feeds/{self.exchange}"

        self.sync(
            endpoint,
            #extra_params={"future": self.symbol},
            start_format="start_time",
            end_format="end_time",
            timestamp_units="s",
            result_key="result",
            merge_endpoint_results_dict=True,
        )
