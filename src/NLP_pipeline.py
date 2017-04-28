
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import IDF
from pyspark.ml.feature import Word2Vec
# from pyspark.ml.feature import NGram

from src.custom_transformers import SpacyTokenizer

# Hard Coded Alphabetical Book and Author IDs
from src.custom_transformers import AuthorLabeler
from src.custom_transformers import TitleLabeler


def get_pipeline():

    # Hard Coded Labels (original texts only):
    auth_hard_lbl = AuthorLabeler(inputCol='author', outputCol='author_label')
    ttl_hard_lbl = TitleLabeler(inputCol='title', outputCol='title_label')

    # Labels
    author_labeler = StringIndexer(inputCol="author", outputCol="author_id")
    title_labeler = StringIndexer(inputCol="title", outputCol="title_id")
    vector_ider = VectorAssembler(
                  inputCols=["author_id", "title_id", "excerpt_number"],
                  outputCol="id_vector")

    tokenizer = SpacyTokenizer(inputCol='excerpt', outputCol='words')

    # TF-IDF
    countvec = CountVectorizer(inputCol=tokenizer.getOutputCol()
                              , outputCol='termfreq')
    idf = IDF(inputCol=countvec.getOutputCol(), outputCol='tfidf')

    # Word2Vec
    word2vec = Word2Vec(vectorSize=250, minCount=2
                        , inputCol=tokenizer.getOutputCol(), outputCol="w2v")
    w2v_2d = Word2Vec(vectorSize=2, minCount=2
                        , inputCol=tokenizer.getOutputCol(), outputCol="w2v_2d")

    # TODO: Include Metadata
    # char_count =
    # word_count =
    # sent_count =
    # para_count =

    # TODO: Play with n-grams
    # NGram(n=2, inputCol=tokenizer.getOutputCol(), outputCol="2_gram")
    # NGram(n=3, inputCol=tokenizer.getOutputCol(), outputCol="3_gram")
    # NGram(n=4, inputCol=tokenizer.getOutputCol(), outputCol="4_gram")
    # NGram(n=5, inputCol=tokenizer.getOutputCol(), outputCol="5_gram")

    pipeline = Pipeline(stages=[author_labeler, title_labeler, vector_ider,
                                tokenizer, countvec, idf, word2vec, w2v_2d])

    return pipeline


def run_pipeline(input_data='data/excerpts.json' , load=False, save_loc=None):
    """
    Not yet working
    """

    # Will need spark context and session - how to include in ".py" file?
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.json(input_data)
    pipe = get_pipeline()
    df = pipe.fit(df).transform(df)

    #Save
    if save_loc:
        df.write.mode('overwrite').save(save_loc, format="parquet")

    #return
    if load:
        return df
