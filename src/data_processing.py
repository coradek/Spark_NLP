from sys import argv
import pyspark as ps    # import the spark suite
import warnings         # display warning if spark context already exists
import os

import string

from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, FloatType

from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import IDF



# define functions to apply to a row

def _char_count(text):
    return len(text)

def _word_count(text):
    return len(text.split())

def _avg_word_length(text):
    return sum([len(t) for t in text.split()]) / float(len(text.split()))

def _sentence_count(text):
    return len(text.split('.'))

# ? Could use sentence count col and word count col to do thie w/o udf
def _sentence_length(text):
    return sum([len(t.split()) for t in text.split('.')]) / float(len(text.split('.')))

#TODO: ? Create count of paragraphs per excerpt?
def _paragraph_count(text):
    pass


def _remove_punctuation(text):
    # return text.translate(None, punctuation)
    return "".join(c for c in text if c not in set(string.punctuation))



def excerpt_metadata(df):

    # create User Defined Functions from above
    charcount_udf = udf(lambda x : _char_count(x))
    wordcount_udf = udf(lambda x: _word_count(x))
    avgwordlen_udf = udf(lambda x: _avg_word_length(x))
    sentencecount_udf = udf(lambda x: _sentence_count(x))
    sentencelength_udf = udf(lambda x: _sentence_length(x))

    # add columns to datafram

    df = df.withColumn("char_count",
                    charcount_udf(df.excerpt).cast(FloatType())) \
            .withColumn("word_count",
                    wordcount_udf(df.excerpt).cast(FloatType())) \
            .withColumn("avg_wordlen",
                    avgwordlen_udf(df.excerpt).cast(FloatType())) \
            .withColumn("sent_count",
                    sentencecount_udf(df.excerpt).cast(FloatType())) \
            .withColumn("sent_length",
                    sentencelength_udf(df.excerpt).cast(FloatType()))
    return df


def tfidf(df):

    # Add words_only column to df (punctuation removed)
    removepunctuation_udf = udf(lambda x : remove_punctuation(x))
    df = df.withColumn("words_only",
                    removepunctuation_udf(df.excerpt).cast(StringType()))

    # Tokenize the words_only string
    tokenizer = Tokenizer(inputCol="words_only", outputCol="tokenized")
    df = tokenizer.transform(df)

    # CountVectorize the token list
    cv = CountVectorizer(inputCol="tokenized", outputCol="count_vectorized")
    cvmodel = cv.fit(df)
    df = cvmodel.transform(df)

    # apply IDF function to countvector to tfidf
    idf = IDF(inputCol="count_vectorized", outputCol="tfidf")
    idfmodel = idf.fit(df)
    df = idfmodel.transform(df)

    return df


def process_data(data_file, save_loc="data/sparkDF"):

    df = spark.read.json(data_file)
    print "\nJSON successfully read into Spark."
    df = excerpt_metadata(df)
    print "\nMetadata added."
    df = tfidf(df)
    print "\ntfidf (and intermediate columns) added."

    if save


if __name__ == '__main__':

    process_data(argv[1])
