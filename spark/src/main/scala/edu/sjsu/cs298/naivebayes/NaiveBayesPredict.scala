package edu.sjsu.cs298.naivebayes

import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.sql.{Row, SparkSession}

object NaiveBayesPredict {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/yelp_reviews.sentiment_results")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/yelp_reviews.nb_sentiment_results")
      .getOrCreate()
    import spark.sqlContext.implicits._

    val reviewSentences = MongoSpark.load(spark)
    val naiveBayesModel: NaiveBayesModel = NaiveBayesModel.load(spark.sparkContext, "./NBModelLambda1")

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("sentenceWords")
      .setPattern("\\W")
    val sentencesWords = regexTokenizer.transform(reviewSentences)

    // Remove Stop words
    val stopWordRemover = new StopWordsRemover()
      .setInputCol("sentenceWords")
      .setOutputCol("wordsFiltered")
    val reviewsFiltered = stopWordRemover.transform(sentencesWords)

    val hashingTF = new HashingTF()

    val NBSentimentResults = reviewsFiltered.select('review_id, 'user_id, 'sentence, 'wordsFiltered, 'position).map {
      case Row(reviewId: String, userId: String, sentence: String, filteredWords: Seq[String], position: Int) =>
        val sentiment = naiveBayesModel.predict(hashingTF.transform(filteredWords))
        (reviewId, userId, sentence, filteredWords, position, sentiment.toInt)
    }.withColumnRenamed("_1", "review_id").withColumnRenamed("_2", "user_id").withColumnRenamed("_3", "sentence")
      .withColumnRenamed("_4", "wordsFiltered").withColumnRenamed("_5", "position").withColumnRenamed("_6", "sentiment")

    MongoSpark.save(NBSentimentResults)
  }
}
