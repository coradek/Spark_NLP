
from pyspark import keyword_only
from pyspark.ml.pipeline import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from spacy.en import English



# Custom stemming transformer class for pyspark
class SpacyTokenize_Transformer(Transformer, HasInputCol, HasOutputCol):
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(SpacyTokenize_Transformer, self).__init__()
        self.stopwords = Param(self, "stopwords", "")
        self._setDefault(stopwords=set())
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)
        # self.parser = English()

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, stopwords=None):
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def setStopwords(self, value):
        self._paramMap[self.stopwords] = value
        return self

    def getStopwords(self):
        return self.getOrDefault(self.stopwords)

    def tokenize(self, excerpt):
        # stopwords = self.getStopwords()
        # parser = English()
        # return [tok.lower_ for tok in parser(excerpt)\
        #         if tok.lower_ not in stopwords]
        parser = English()
        return [tok.lower_ for tok in parser(text)]

    def _transform(self, dataset):
        tokenize_udf = udf(lambda x: tokenize(x), ArrayType(StringType()))
        inCol = dataset[self.getInputCol()]
        outCol = self.getOutputCol()

        return dataset.withColumn(outCol, tokenize_udf(inCol))

        # def f(s):
        #     parser = English()
        #     tokens = [str(tok) for tok in parser(s)]
        #     return [t for t in tokens if t.lower() not in stopwords]

        # t = ArrayType(StringType())
        # out_col = self.getOutputCol()
        # in_col = dataset[self.getInputCol()]
        # return dataset.withColumn(out_col, udf(f, t)(in_col))
