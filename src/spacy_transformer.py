from pyspark import keyword_only
from pyspark.ml.util import Identifiable
from pyspark.ml.pipeline import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from spacy.en import English
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType



# Custom stemming transformer class for pyspark
class Tokenize_Transformer(Transformer, HasInputCol, HasOutputCol):
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(Tokenize_Transformer, self).__init__()
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, language='english', ):
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        """
        INPUT: text
        OUTPUT: list of lowercase tokens
        """
        parser = English()
        udf_token = udf(lambda excerpt: \
                    [tok.lower_ for tok in parser(excerpt)]\
                    , ArrayType(StringType()))

        inCol = self.getInputCol()
        outCol = self.getOutputCol()

        return dataset.withColumn(outCol, udf_tokenize(inCol))
