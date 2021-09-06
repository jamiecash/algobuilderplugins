import logging

from features import feature as ft


class MovingAverage(ft.FeatureImplementation):
    """
    Calculates a moving average
    """

    # Logger
    __log = logging.getLogger(__name__)

    def execute(self, feature_execution):
        """
        Executes the feature calculation for a single execution
        :param feature_execution:
        :return:
        """
        # Get the symbols. There should only be 1
        assert len(feature_execution.featureexecutionsymbol_set) == 1
        symbol = feature_execution.featureexecutionsymbol_set[0].datasource_symbol

        self.__log.debug(f"Calculating moving average for {feature_execution}. Symbol={symbol}")
