# Spark Natural Language Processing:

### Project Introduction:

This repository serves as a playground for exploring Spark 2.0 and Natural Language Processing through Spark's Machine Learning Library.

The project takes twenty novels from Project Gutenberg, divided into excerpts and uses a number of techniques (including TF-IDF and Word2Vec) to identify the author of the text. The Jupyter Notebooks in this repository serve as chapters in the various phases of data exploration and processing.

![wordcloud][wordcloud]


## _NOTE: create and use a python 3.5 (py35) environment for this project_

(until Spark 2.2 arrives, using python 3.5 "fixes" spark2.1/python3.6 compatability issues)

- create python 3.5 env named py35
  `conda create --name py35 python=3.5 anaconda`

- change 3.5 kernel name to avoid conflict with pre-existing python 3.6 env in jupyter notebook
  `python -m ipykernel install --name py35 --display-name py35`

- activate the py35 environment
  `source activate py35`

- in jupyter notebook you may need to manually change the kernel to py35



### _Dependencies:_
This Project Uses:
*  anaconda
*  (pyspark)
*  wordcloud

  `conda install -c https://conda.anaconda.org/amueller wordcloud`

*  spacy

  `conda config --add channels conda-forge`

  `conda install spacy`

  `python -m spacy.en.download`


### __To follow along with this exploration:__
* Navigate to this repository's main directory:

  `$ cd Spark_NLP`

* Use a python 3.5 env

  `source activate py35`

* Launch Jupyter Notebook via the script:
  (You will need to configure Jupyter Notebook to run with Spark)

  `sparkjupyter3.sh`

Once you have Jupyter Notebook and Spark playing together nicely, it is time to get into NLP with Spark! The notebooks in this project are meant to be run/read in order (yes, they are named alphabetically). If you would like to skip around, below are the essential parts.

* run the data_setup python script:

  `$ python src/data_setup.py`

  * (Alternatively - run all cells in 'build_the_raw_data_set.ipynb')


* run all cells in 'data_preparation.ipynb'

  (this may take a looong time: over 2 hr on my tiny laptop)

  (while you wait, 'creating_a_pipeline' is optional reading, but contains lots of good Spark how-to info about what this notebook is doing)


## Construction Zone:

#### WARNING: Everything below here is essentially stream of consciousness

#### __Read scratchwork/Notes.md before running any of the notebooks in that folder__

This project is a work in progress, and a sandbox for learning to make better use of the newest version of Spark. All code will continue to be improved upon as we learn more effective techniques.

If you have thoughts of potential improvements, please get in touch. We would love to hear from you.

Questions:

Word2Vec optimal vector size?
- 200 ~ 400 (david valpey)



__Thoughts on data sets: (Multiple Pipelines)__

raw text: leave in punctuation and stopwords
- determine whether differing use of these elements improves classification outcome

No Punctuation or Stopwords:
- for comparison with above

Punctuation but no stopwords?
Stopwords but no punctuation?
- overkill? or valuable for deeper insight
- will depend on outcome of all vs nothing sets above


### _Things to Include:_

for data vis - Word2Vec vector size 2 for easy 2D plotability
- (out of curiosity how will this compare to PCA of the larger w2v vector?)


[wordcloud]: images/wordclouds.png "wordclouds for collected texts bby each author"
