import logging
import sys
import os
import pandas as pd
import pytz
import unittest
from unittest.mock import patch, PropertyMock, MagicMock

from datetime import datetime
from pricedata.models import DataSource, candle_periods
from mt5datasource import MT5DataSource


class TestMT5DataSource(unittest.TestCase):
    """
    Unit test to test MT5DataSource.
    """

    # Logger
    __log = logging.getLogger(__name__)

    def test_get_symbols(self):
        symbols = MT5DataSource(data_source_model=DataSource()).get_symbols()
        self.assertTrue(len(symbols) > 0, "No symbols were returned.")

    def test_get_prices(self):
        from_date = datetime(2021, 7, 6, 18, 0, 0)
        to_date = datetime(2021, 7, 6, 19, 0, 0)

        # 1M candles, should use copy_rates_range api
        prices = MT5DataSource(data_source_model=DataSource()).get_prices('GBPUSD', from_date, to_date, '1M')
        self.assertTrue(len(prices) > 0, "No prices were returned for 1M period.")

        # 1S candles, should use copy_ticks_range api
        prices = MT5DataSource(data_source_model=DataSource()).get_prices('GBPUSD', from_date, to_date, '1S')
        self.assertTrue(len(prices) > 0, "No prices were returned for 1S period.")


if __name__ == '__main__':
    unittest.main()
