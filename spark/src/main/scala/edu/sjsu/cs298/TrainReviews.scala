package edu.sjsu.cs298

import com.databricks.spark.corenlp.functions.ssplit
import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.{DenseVector, VectorUDT, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.posexplode
import org.apache.spark.sql.types.LongType

import scala.collection.mutable

object TrainReviews {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/yelp_reviews.review_lte50")
      //.config("spark.mongodb.output.uri", "mongodb://localhost:27017/yelp_reviews.review_lte50")
      .getOrCreate()
    import spark.sqlContext.implicits._

    val trainReviews = MongoSpark.load(spark)


    // Tokenize
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("words")
      .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

    val reviewsWithWords = regexTokenizer.transform(trainReviews)

    // Remove Stop words
    // Custom or Default Stop Words
    //val stopWordsList  = spark.sparkContext.textFile("stopwords.txt").collect()
    val stopWordRemover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("wordsFiltered")
      //.setStopWords(stopWordsList)

    val reviewsFiltered = stopWordRemover.transform(reviewsWithWords)
    //reviewsFiltered.select('stars, 'wordsFiltered).show(false)

    // Train NB Model
    val hashingTF = new HashingTF()

    val labeledReviews = reviewsFiltered.select('stars, 'wordsFiltered).rdd.map {
      case Row(stars: Long, filteredWords: Seq[String]) =>
        // Start sentiment from 0
        LabeledPoint(stars - 1, hashingTF.transform(filteredWords))
    }

    labeledReviews.cache()
    val naiveBayesModel: NaiveBayesModel = NaiveBayes.train(labeledReviews, lambda = 1.0, modelType = "multinomial")
    val t = naiveBayesModel.predict(hashingTF.transform(Seq("worst")))
    print(t)

    //naiveBayesModel.save(spark.sparkContext, "./naiveBayesModel")

    //MongoSpark.save(trainReviews)
  }


}
