import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict

import MetaTrader5

from pricedata.datasource import DataSourceImplementation


class MT5DataSource(DataSourceImplementation):
    """
    MetaTrader 5 DataSource
    """
    def __init__(self, data_source_model):
        # Super
        DataSourceImplementation.__init__(self, data_source_model=data_source_model)

        # Connect to MetaTrader5. Opens if not already open.

        # Logger
        self.__log = logging.getLogger(__name__)

        # Open MT5 and log error if it could not open
        if not MetaTrader5.initialize():
            self.__log.error("initialize() failed")
            MetaTrader5.shutdown()

        # Print connection status
        self.__log.debug(MetaTrader5.terminal_info())

        # Print data on MetaTrader 5 version
        self.__log.debug(MetaTrader5.version())

    def __del__(self):
        # shut down connection to the MetaTrader 5 terminal
        MetaTrader5.shutdown()

    def get_symbols(self) -> List[Dict[str, str]]:
        """
        Get symbols from MetaTrader5

        :return: list of dictionaries containing symbol_name and instrument_type
        """

        all_symbols = MetaTrader5.symbols_get()

        # Are we returning MarketWatch symbols only
        market_watch_only = self._data_source_model.get_connection_param('market_watch_only')

        # We are returning the symbol names and instrument types
        symbols = []

        # Iterate all symbols, and populate symbol names, taking into account visible flag if we are returning market
        # watch symbols only.
        for symbol in all_symbols:
            if market_watch_only is False or (market_watch_only is True and symbol.visible):
                instrument_type = None
                if symbol.path.startswith('CFD'):
                    instrument_type = 'CFD'
                elif symbol.path.startswith('Forex'):
                    instrument_type = 'FOREX'

                symbols.append({'symbol_name': symbol.name, 'instrument_type': instrument_type})

        # Log symbol counts
        total_symbols = MetaTrader5.symbols_total()
        num_selected_symbols = len(symbols)
        self.__log.debug(f"{num_selected_symbols} of {total_symbols} returned. market_watch_only={market_watch_only}.")

        return symbols

    def get_prices(self, symbol: str, from_date: datetime, to_date: datetime, period: str) -> pd.DataFrame:
        """
        Gets OHLC price data for the specified symbol from MT5.
        :param symbol: The name of the symbol to get the price data for.
        :param from_date: Date from when to retrieve data
        :param to_date: Date to receive data up to
        :param period: The period for the candes. Possible values are defined in models.candle_periods:

        :return: Price data for symbol as pandas dataframe containing the following columns:
            ['time', 'period', 'bid_open', 'bid_high', 'bid_low', 'bid_close',
            'ask_open', 'ask_high', 'ask_low', 'ask_close', 'volume']
        """

        prices_dataframe = None

        # Mappings between defined periods and MT5 timeframes
        period_mapping = {
            '1S': None,
            '5S': None,
            '10S': None,
            '15S': None,
            '30S': None,
            '1M': MetaTrader5.TIMEFRAME_M1,
            '5M': MetaTrader5.TIMEFRAME_M5,
            '10M': MetaTrader5.TIMEFRAME_M10,
            '15M': MetaTrader5.TIMEFRAME_M15,
            '30M': MetaTrader5.TIMEFRAME_M30,
            '1H': MetaTrader5.TIMEFRAME_H1,
            '3H': MetaTrader5.TIMEFRAME_H3,
            '6H': MetaTrader5.TIMEFRAME_H6,
            '12H': MetaTrader5.TIMEFRAME_H12,
            '1D': MetaTrader5.TIMEFRAME_D1,
            '1W': MetaTrader5.TIMEFRAME_W1,
            '1MO': MetaTrader5.TIMEFRAME_MN1
        }

        # Get the MT5 timeframe from the supplied period
        timeframe = period_mapping[period]

        # If we have an equivalent timeframe in MT5, get candles, otherwise get ticks and resample
        if timeframe is not None:
            # Get prices from MT5
            prices = MetaTrader5.copy_rates_range(symbol, timeframe, from_date, to_date)
            if prices is None:
                error = MetaTrader5.last_error()
                self.__log.error(f"Error retrieving prices for {symbol}: {error}")
            else:
                self.__log.debug(f"{len(prices)} prices retrieved for {symbol}.")

                # Create dataframe from data and convert time in seconds to datetime format
                prices_dataframe = \
                    pd.DataFrame(columns=self._prices_columns,
                                 data={'time': prices['time'], 'period': period, 'bid_open': prices['open'],
                                       'bid_high': prices['high'], 'bid_low': prices['low'],
                                       'bid_close': prices['close'],
                                       'ask_open': prices['open'] + prices['spread'] / 10000,
                                       'ask_high': prices['high'] + prices['spread'] / 10000,
                                       'ask_low': prices['low'] + prices['spread'] / 10000,
                                       'ask_close': prices['close'] + prices['spread'] / 10000,
                                       'volume': prices['tick_volume']})

                prices_dataframe['time'] = pd.to_datetime(prices_dataframe['time'], unit='s')
        else:
            # Get ticks from MT5
            ticks = MetaTrader5.copy_ticks_range(symbol, from_date, to_date, MetaTrader5.COPY_TICKS_ALL)

            # If ticks is None, there was an error
            if ticks is None:
                error = MetaTrader5.last_error()
                self.__log.error(f"Error retrieving ticks for {symbol}: {error}")
            else:
                self.__log.debug(f"{len(ticks)} ticks retrieved for {symbol}.")

                try:
                    # Create dataframe from data and convert time in seconds to datetime format
                    ticks_dataframe = pd.DataFrame(ticks)
                    ticks_dataframe['time'] = pd.to_datetime(ticks_dataframe['time'], unit='s')

                    # Set the index, resample bid and ask columns, merge, then reset index
                    ticks_dataframe = ticks_dataframe.set_index('time')
                    ohlc_calcs = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'count'}
                    bid_candles = ticks_dataframe['bid'].resample(period).agg(ohlc_calcs)
                    ask_candles = ticks_dataframe['ask'].resample(period).agg(ohlc_calcs)
                    prices_dataframe = pd.concat([bid_candles, ask_candles], axis=1)
                    prices_dataframe.reset_index(inplace=True)

                    # We should now have time and (open, high, low, close, volume) * 2, one for bid and one for ask.
                    # Add period, rename the columns, then delete the first volume column.
                    prices_dataframe.insert(1, 'period', period)
                    prices_dataframe.columns = ['time', 'period', 'bid_open', 'bid_high', 'bid_low', 'bid_close',
                                                'bid_volume', 'ask_open', 'ask_high', 'ask_low', 'ask_close',
                                                'volume']
                    prices_dataframe.drop('bid_volume', axis=1, inplace=True)  # First volume column

                    # Remove n/a
                    prices_dataframe = prices_dataframe.dropna()

                except RecursionError as ex:
                    self.__log.warning("Error converting ticks to dataframe and resampling.", ex)

        return prices_dataframe

