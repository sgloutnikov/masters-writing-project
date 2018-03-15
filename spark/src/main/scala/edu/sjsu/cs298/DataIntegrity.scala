package edu.sjsu.cs298

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataIntegrity {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/yelp_reviews.sentiment_vectors")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/yelp_reviews.sentimentVectors")
      .getOrCreate()
    import spark.sqlContext.implicits._

    // All sentiment vectors
    val allSentimentVectors = MongoSpark.load(spark)
    //println(allSentimentVectors.count())

    // All available reviews
    //val reviewsReadConfig = ReadConfig(Map("collection" -> "review_50"), Some(ReadConfig(spark)))
    //val allReviews = MongoSpark.load(spark, reviewsReadConfig)
    //println(allReviews.count())

    val noDuplicateVectors = allSentimentVectors.dropDuplicates("review_id")
    println("No Duplicates: " + noDuplicateVectors.count())

    MongoSpark.save(noDuplicateVectors)


  }

}
