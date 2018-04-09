package edu.sjsu.cs298.corenlp

import com.databricks.spark.corenlp.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Will not run sentiment score in parallel.
  * Switched to own Java implementation.
  */
object SparkCoreNLP {

  val REVIEWS_LOCATION = ""

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[*]")
      //.config("spark.mongodb.input.uri", "mongodb://localhost:27017/yelp_reviews.sentiment_results")
      //.config("spark.mongodb.output.uri", "mongodb://localhost:27017/yelp_reviews.sentiment_results")
      .getOrCreate()
    import spark.sqlContext.implicits._

    val reviews = spark.read.json(REVIEWS_LOCATION)
    println("Total reviews: " + reviews.count())

    val t0 = System.currentTimeMillis()
    val results = reviews.select($"review_id", posexplode(ssplit('text)).as(Seq("pos", "review_sentence")))
      .withColumn("sentiment", sentiment('review_sentence))
    println("Total sentences: " + results.count())

    results.groupBy("sentiment").count().show()

    //MongoSpark.write(results)
    val t1 = System.currentTimeMillis()
    println("Total time: " + (t1 - t0)/ 1000 + " seconds.")

  }

}
