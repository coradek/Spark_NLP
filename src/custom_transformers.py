from spacy.en import English
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, ArrayType, StringType


class SpacyTokenizer(Transformer, HasInputCol, HasOutputCol):

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, stopwords=None):
        super(SpacyTokenizer, self).__init__()
        self.stopwords = Param(self, "stopwords", "")
        self._setDefault(stopwords=set())
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, stopwords=None):
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setStopwords(self, value):
        self._paramMap[self.stopwords] = value
        return self

    def getStopwords(self):
        return self.getOrDefault(self.stopwords)

    def _transform(self, dataset):
        stopwords = self.getStopwords()

        def f(s):
            parser = English()
            # tokens = [str(tok) for tok in parser(s)]
            # return [t for t in tokens if t.lower() not in stopwords]
            return [tok.lower_ for tok in parser(s) \
                    if tok.lower_ not in stopwords]

        t = ArrayType(StringType())
        out_col = self.getOutputCol()
        in_col = dataset[self.getInputCol()]
        return dataset.withColumn(out_col, udf(f, t)(in_col))


class AuthorLabeler(Transformer, HasInputCol, HasOutputCol):
    """
    Maps Author names to IntegerType
    alphabetically by last name
    (IntegerType needed for multiclass classifiers in Spark ML)
    """
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(AuthorLabeler, self).__init__()
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        out_col = self.getOutputCol()
        in_col = dataset[self.getInputCol()]

        def author_labels(s):
            labels = {'JaneAusten': 0,
                      'CharlesDickens': 1,
                      'JohnMuir': 2,
                      'MarkTwain': 3}
            return labels[s]

        t = IntegerType()
        return dataset.withColumn(out_col, udf(author_labels, t)(in_col))


class TitleLabeler(Transformer, HasInputCol, HasOutputCol):
    """
    Maps book titles to IntegerType
    alphabetically by author and title
    (IntegerType needed for multiclass classifiers in Spark ML)
    """
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
                      'MansfieldPark': 1,
                      'Persuasion': 2,
                      'PrideAndPrejudice': 3,
                      'SenseAndSensibility': 4,
                      'AChristmasCarol': 5,
                      'DavidCopperfield': 6,
                      'GreatExpectations': 7,
                      'OliverTwist': 8,
                      'ATaleOfTwoCities': 9,
                      'MyFirstSummerInTheSierra': 10,
                      'Stickeen': 11,
                      'TheStoryofMyBoyhoodAndYouth': 12,
                      'TravelsInAlaska': 13,
                      'TheYosemite': 14,
                      'TheAdventuresOfHuckleberryFinn': 15,
                      'AConnecticutYankeeInKingArthursCourt': 16,
                      'TheInnocentsAbroad': 17,
                      'RoughingIt': 18,
                      'TheTragedyofPuddnheadWilson': 19}
            return labels[s]

        t = IntegerType()
        return dataset.withColumn(out_col, udf(title_labels, t)(in_col))
