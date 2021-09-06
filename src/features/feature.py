import abc
import logging
from features import models


class FeatureInstanceNotImplementedError(Exception):
    """
    An exception that can be raised by instance if specified feature is not implemented.
    """
    pass


class FeatureImplementation:
    """
    The interface for applications features
    """

    # Logger
    __log = logging.getLogger(__name__)

    # The model class containing the properties for the feature. Protected to enable implementations of this class
    # only to access.
    _feature = None

    def __init__(self, feature: models.Feature) -> None:
        """
        Construct the feature implementation and stores its model
        :param feature: The Feature model instance.
        """
        self._feature = feature

    @staticmethod
    def instance(name: str):
        """
        Get the Feature instance specified by the name.
        :param name:
        :return:
        """

        features = models.Feature.objects.filter(name=name)

        if len(features) != 0:
            f = features[0]

            # Get plugin class
            clazz = f.pluginclass.plugin_class
        else:
            raise FeatureInstanceNotImplementedError(f'FeatureImplementation instance cannot be created for '
                                                     f'feature {name}. Feature could not be found.')

        return clazz(f)

    @staticmethod
    def all_instances():
        """
        Returns a list of all Features
        :return:
        """
        all_feature_models = models.Feature.objects.all()

        all_feature_implementations = []
        for feature_model in all_feature_models:
            all_feature_implementations.append(FeatureImplementation.instance(feature_model))

        return all_feature_implementations

    def execute_all(self):
        """
        Executes all feature executions defined for this feature
        :return:
        """
        if self._feature.active:
            for feature_execution in self._feature.featureexecution_set:
                if feature_execution.active:
                    self.execute(feature_execution)

    @abc.abstractmethod
    def execute(self, feature_execution):
        """
        Executes the feature calculation for a single feature execution

        :param feature_execution: The feature execution containing the datasource_candleperiod, calculation_period and
            symbols for the calculation
        """
        raise NotImplementedError
