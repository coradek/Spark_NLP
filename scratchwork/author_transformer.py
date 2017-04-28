from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


class AuthorLabeler(Transformer, HasInputCol, HasOutputCol):

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
