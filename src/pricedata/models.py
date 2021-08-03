""""
A mock up of the AlgoBuilder models for use in unit testing. Very lightweight and with the minimum methods required for
testing. They should be mocked in your plugin tests.
"""

candle_periods = [
        ('1S', '1 Second'), ('5S', '5 Second'), ('10S', '10 Second'), ('15S', '15 Second'), ('30S', '30 Second'),
        ('1M', '1 Minute'), ('5M', '5 Minute'), ('10M', '10 Minute'), ('15M', '15 Minute'), ('30M', '30 Minute'),
        ('1H', '1 Hour'), ('3H', '3 Hour'), ('6H', '6 Hour'), ('12H', '12 Hour'), ('1D', '1 Day'), ('1W', '1 Week'),
        ('1MO', '1 Month')
    ]

instrument_types = [
        ('FOREX', 'Foreign Exchange'), ('CFD', 'Contract for Difference'), ('STOCK', 'Company Stock'),
        ('CRYPTO', 'Crypto Currency')
    ]

aggregation_periods = [
        ('minutes', 'Minutes'), ('hours', 'Hours'), ('days', 'Days'), ('weeks', 'Weeks'), ('months', 'Months')
    ]


class DataSource:
    objects = None
    name = None


class Symbol:
    objects = None


class DataSourceSymbol:
    objects = None
    symbol_info = '{"point": 0, "digits": 0}'
