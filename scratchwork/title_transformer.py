from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


class TitleLabeler(Transformer, HasInputCol, HasOutputCol):

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(TitleLabeler, self).__init__()
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        out_col = self.getOutputCol()
        in_col = dataset[self.getInputCol()]

        def title_labels(s):
            labels = {'Emma': 0,
                      'MansfieldPark': 2,
                      'Persuasion': 3,
                      'PrideAndPrejudice': 4,
                      'SenseAndSensibility': 5,
                      'AChristmasCarol': 6,
                      'DavidCopperfield': 7,
                      'GreatExpectations': 8,
                      'OliverTwist': 9,
                      'ATaleOfTwoCities': 10,
                      'MyFirstSummerInTheSierra': 11,
                      'Stickeen': 12,
                      'TheStoryofMyBoyhoodAndYouth': 13,
                      'TravelsInAlaska': 14,
                      'TheYosemite': 15,
                      'TheAdventuresOfHuckleberryFinn': 16,
                      'AConnecticutYankeeInKingArthursCourt': 17,
                      'TheInnocentsAbroad': 18,
                      'RoughingIt': 19,
                      'TheTragedyofPuddnheadWilson': 20}
            return labels[s]

        t = IntegerType()
        return dataset.withColumn(out_col, udf(title_labels, t)(in_col))
