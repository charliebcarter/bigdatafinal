# importing required libraries
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
from pyspark.ml import Pipeline
# from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import Row, Column
import sys


def prediction(tweet):
    try:

        tweet = tweet.filter(lambda x: len(x) > 0)
        rowRdd = tweet.map(lambda w: Row(tweet=w))
        wordsDataFrame = spark.createDataFrame(rowRdd)
        pipelineFit.transform(wordsDataFrame).select('tweet', 'prediction').show(20, False)

    except:
        print('No data')


if __name__ == "__main__":
    sc = SparkContext(appName="tweetSentiment")
    spark = SparkSession(sc)

    # define the schema
    schema = tp.StructType([
        tp.StructField(name='label', dataType=tp.IntegerType(), nullable=True),
        tp.StructField(name='id', dataType=tp.IntegerType(), nullable=True),
        tp.StructField(name='time', dataType=tp.StringType(), nullable=True),
        tp.StructField(name='query', dataType=tp.StringType(), nullable=True),
        tp.StructField(name='user', dataType=tp.StringType(), nullable=True),
        tp.StructField(name='tweet', dataType=tp.StringType(), nullable=True)
    ])
    # reading the data set
    print('\n\nReading the dataset...........................\n')
    tweet_data = spark.read.csv('training.1600000.processed.noemoticon.csv', schema=schema,
                                header=True)
    tweet_data.show(2)

    tweet_data.printSchema()
    stage_1 = RegexTokenizer(inputCol='tweet', outputCol='tokens', pattern='\\W')

    stage_2 = StopWordsRemover(inputCol='tokens', outputCol='filtered_words')

    stage_3 = Word2Vec(inputCol='filtered_words', outputCol='vector', vectorSize=100)

    model = LogisticRegression(featuresCol='vector', labelCol='label')

    pipeline = Pipeline(stages=[stage_1, stage_2, stage_3, model])

    pipelineFit = pipeline.fit(tweet_data)

    print('\nWaiting for the Data\n')
    ssc = StreamingContext(sc, batchDuration=2)
    lines = ssc.socketTextStream('localhost', 2000)
    words = lines.flatMap(lambda line: line.split('@'))

    words.foreachRDD(prediction)

    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
