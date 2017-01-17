# Spark Natural Language Processing:

### Project Introduction:

This repository serves as a playground for exploring Spark 2.0 and Natural Language Processing through Spark's Machine Learning Library.

The project takes twenty novels from Project Gutenberg, divided into excerpts and uses a number of techniques (including TF-IDF and Word2Vec) to identify the author of the text. The Jupyter Notebooks in this repository serve as chapters in the various phases of data exploration and processing.

![wordcloud][wordcloud]

<br>
### _Dependencies:_
This Project Requires:
*  pyspark
*  jupyter notebook
*  numpy
*  pandas
*  matplotlib
*  wordcloud (pip install - conda caused errors)

<br>
### __To follow along with this exploration:__
* Navigate to this repository's main directory:
`$ cd Spark_NLP`

* Launch Jupyter Notebook via the script:
  (You will need to configure Jupyter Notebook to run with Spark)
`sparkjupyter.sh`

* run the data_setup python script:
  `$ python src/data_setup.py`
  * (Alternatively - run all cells in the 'CreateData' Jupyter Notebook)

<br>

## Construction Zone:
#### WARNING: Everything below here is essentially stream of consciousness

### __Read scratchwork/Notes.md before running any of the notebooks in that folder__

This project is a work in progress, and a sandbox for learning to make better use of the newest version of Spark. All code will continue to be imporved upon as we learn more effective techniques.

If you have thoughts of potential improveements, please get in touch. We would love to hear from you.

Questions:
Word2Vec optimal vector size?
- 200 ~ 400 (david valpey)


Thoughts on data sets: Multiple Pipelines

raw text: leave in punctuation and stopwords
- determine whether differing use of these elements improves classification outcome

No Punctuation or Stopwords:
- for comparrison with above

Punctuation but no stopwords?
Stopwords but no punctuation?
- overkill? or valuable for deeper insight
- will depend on outcome of all vs nothing sets above


### _Things to Include:_

for data vis - Word2Vec vector size 2 for easy 2D plotability
- (out os curiosity how will this compare to PCA of larger w2v vector?)


[wordcloud]: images/wordclouds.png "wordclouds for collected texts bby each author"
