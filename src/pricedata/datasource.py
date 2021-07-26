"""
Lightweight DataSourceImplementation base class, contains only the methods required to test the plugins
"""

import abc
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict

from pricedata.models import DataSource


class PeriodNotImplementedError(Exception):
    """
    An exception that can be raised by sub classes get_prices method for periods that are not implemented.
    """
    pass


class DataNotAvailableException(Exception):
    """
    An exception that can be raised if data is not available for the specified symbol / timeframe
    """
    pass


class DataSourceImplementation:
    """
    The interface for applications datasources
    """

    # Logger
    __log = logging.getLogger(__name__)

    # The model class containing the properties for the data source. Protected to enable implementations of this class
    # only to access.
    _data_source_model = None

    # The get_prices implementation should return a pandas dataframe with the following columns
    _prices_columns = ['time', 'period', 'bid_open', 'bid_high', 'bid_low', 'bid_close', 'ask_open', 'ask_high',
                       'ask_low', 'ask_close', 'volume']

    def __init__(self, data_source_model: DataSource) -> None:
        """
        Construct the datasource implementation and stores its model
        :param data_source_model: The DataSource model instance containing the details required to create and connect to
        this datasource.
        """
        self._data_source_model = data_source_model

    @abc.abstractmethod
    def get_symbols(self) -> List[Dict[str, str]]:
        """
        Get symbols from datasource

        :return: list of dictionaries containing symbol_name and instrument_type
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_prices(self, symbol: str, from_date: datetime, to_date: datetime, period: str) -> pd.DataFrame:
        """
        Gets OHLC price data for the specified symbol.
        :param symbol: The name of the symbol to get the price data for.
        :param from_date: Date from when to retrieve data
        :param to_date: Date to receive data up to
        :param period: The period for the candes. Possible values are defined in models.candle_periods.

        :return: Price data for symbol as pandas dataframe containing the following columns:
            ['time', 'period', 'bid_open', 'bid_high', 'bid_low', 'bid_close',
            'ask_open', 'ask_high', 'ask_low', 'ask_close', 'volume']
        """
        raise NotImplementedError
