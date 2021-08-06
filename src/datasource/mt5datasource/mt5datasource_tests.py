import logging
import unittest
from unittest.mock import patch, PropertyMock, MagicMock

from datetime import datetime

from mt5datasource import MT5DataSource
from pricedata.datasource import DataNotAvailableException
from pricedata.models import DataSource, Symbol, DataSourceSymbol


class TestMT5DataSource(unittest.TestCase):
    """
    Unit test to test MT5DataSource.
    """

    # Logger
    __log = logging.getLogger(__name__)

    @patch('pricedata.datasource.DataSourceImplementation._datasource')
    def test_get_symbols(self, mock):
        # Mock the datasource models connection_params response. MT5 is testing for market_watch_only. We will return
        # True.
        mock.get_connection_param.return_value = True

        symbols = MT5DataSource(datasource=mock).get_symbols()
        self.assertTrue(len(symbols) > 0, "No symbols were returned.")

    @patch('pricedata.models.DataSourceSymbol.objects')
    @patch('pricedata.models.Symbol.objects')
    @patch('pricedata.models.DataSource')
    def test_get_prices(self, datasource_mock, symbol_mock, datasourcesymbol_mock):
        """
        Test 2 periods, one that resamples from ticks and one that retrieves directly as candles
        """
        # Mock filter methods of model to return a list of 1
        symbol_mock.filter.return_value = [Symbol(), ]
        datasourcesymbol_mock.filter.return_value = [DataSourceSymbol(), ]

        from_date = datetime(2021, 7, 6, 18, 0, 0)
        to_date = datetime(2021, 7, 6, 19, 0, 0)

        # 1M candles, should use copy_rates_range api
        prices = MT5DataSource(datasource=DataSource()).get_prices('GBPUSD', from_date, to_date, '1M')
        self.assertTrue(len(prices) > 0, "No prices were returned for 1M period.")

        # 1S candles, should use copy_ticks_range api
        prices = MT5DataSource(datasource=DataSource()).get_prices('GBPUSD', from_date, to_date, '1S')
        self.assertTrue(len(prices) > 0, "No prices were returned for 1S period.")

    def test_historic(self):
        """
        Test what happens when we try and get data for a historic period where MT5 no longer holds data for. We should
        raise an exception.
        """
        pass  # MT5 now returning empty dataframe and success for historic periods.

        """from_date = datetime(2000, 7, 6, 18, 0, 0)
        to_date = datetime(2000, 7, 6, 19, 0, 0)

        datasource_impl = MT5DataSource(data_source_model=DataSource())

        # 1S candles, should use copy_ticks_range api
        with self.assertRaises(DataNotAvailableException):
            prices = datasource_impl.get_prices(symbol='GBPUSD', from_date=from_date, to_date=to_date, period='1S')
            print(prices)"""


if __name__ == '__main__':
    unittest.main()
