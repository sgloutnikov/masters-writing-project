package edu.sjsu.cs298.naivebayes

import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SparkSession}


object NaiveBayesTrain {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/yelp_reviews.review_lte50")
      .getOrCreate()
    import spark.sqlContext.implicits._
    val trainReviews = MongoSpark.load(spark)

    // Tokenize
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("words")
      .setPattern("\\W")
    val reviewsWithWords = regexTokenizer.transform(trainReviews)

    // Remove Stop words
    // Custom or Default Stop Words
    //val stopWordsList  = spark.sparkContext.textFile("stopwords.txt").collect()
    val stopWordRemover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("wordsFiltered")
      //.setStopWords(stopWordsList)
    val reviewsFiltered = stopWordRemover.transform(reviewsWithWords)

    // Train NB Model
    val hashingTF = new HashingTF()
    val labeledReviews = reviewsFiltered.select('stars, 'wordsFiltered).rdd.map {
      case Row(stars: Long, filteredWords: Seq[String]) =>
        // Start sentiment from 0
        LabeledPoint(stars - 1, hashingTF.transform(filteredWords))
    }
    labeledReviews.cache()
    val naiveBayesModel: NaiveBayesModel = NaiveBayes.train(labeledReviews, lambda = 1.0, modelType = "multinomial")
    naiveBayesModel.save(spark.sparkContext, "./NBModelLambda1")
  }
}
