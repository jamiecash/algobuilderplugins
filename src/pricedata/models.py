"""
Lightweight model used to test plugins
"""


import ast
import logging

candle_periods = [
        ('1S', '1 Second'),
        ('5S', '5 Second'),
        ('10S', '10 Second'),
        ('15S', '15 Second'),
        ('30S', '30 Second'),
        ('1M', '1 Minute'),
        ('5M', '5 Minute'),
        ('10M', '10 Minute'),
        ('15M', '15 Minute'),
        ('30M', '30 Minute'),
        ('1H', '1 Hour'),
        ('3H', '3 Hour'),
        ('6H', '6 Hour'),
        ('12H', '12 Hour'),
        ('1D', '1 Day'),
        ('1W', '1 Week'),
        ('1MO', '1 Month')
    ]


class DataSource:
    """
    A datasource to retrieve data from
    """

    # Logger
    __log = logging.getLogger(__name__)

    # The datasource name
    name = 'MT5'

    # The datasource implementation class
    module = ''
    class_name = ''

    # The requirements.txt file containing any dependencies
    requirements_file = ''

    # The datasource connection parameters
    connection_params = "{'market_watch_only': True}"

    def get_connection_param(self, param_name: str):
        """
        Returns the value of the specified connection param
        :return:
        """
        return ast.literal_eval(self.connection_params)[param_name]

    def __repr__(self):
        return f"DataSource(name={self.name}, module={self.module}, class={self.class_name}, " \
               f"requirements_file={self.requirements_file}, connection_params={self.connection_params})"

    def __str__(self):
        return f"{self.name}"


class Symbol:
    """
    A financial Symbol. e.g. GBPUSD
    """
    # Name of the symbol.
    name = ''
    instrument_type = ''

    def __repr__(self):
        return f"Symbol(name={self.name}, instrument_type={self.instrument_type})"

    def __str__(self):
        return f"{self.name}"


class DataSourceSymbol:
    """
    The mapping between data sources and symbols, including a flag to determine whether price data will be retrieved
    from that datasource for the symbol.
    """

    datasource = ''
    symbol = ''

    # Flag to determine whether price data will be retrieved for this datasource / symbol combination
    retrieve_price_data = True

    # Any broker specific data required for the symbol. Stored as json text.
    symbol_info = ''

    def __repr__(self):
        return f"DataSourceSymbol(datasource={self.datasource}, symbol={self.symbol}, " \
               f"retrieve_price_data={self.retrieve_price_data})"

    def __str__(self):
        return f"datasource={self.datasource}, symbol={self.symbol}, retrieve_price_data={self.retrieve_price_data}"
