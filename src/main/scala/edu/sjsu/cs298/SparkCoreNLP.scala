package edu.sjsu.cs298

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._
import com.mongodb.spark.MongoSpark

object SparkCoreNLP {

  val REVIEWS_LOCATION = "C:\\Users\\Admin\\Downloads\\YelpDataset11\\dataset\\review.json"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder()
      .appName("WritingProject")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/yelp_reviews.sentiment_results")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/yelp_reviews.sentiment_results")
      .getOrCreate()
    import spark.sqlContext.implicits._

    val reviews = spark.read.json(REVIEWS_LOCATION)

    /*
    val topReviewers = reviews.groupBy('user_id).count().sort(desc("count"))
    val moreThanFiveReviews = topReviewers.filter('count > 5)
    println(moreThanFiveReviews.count())
    */

    val t0 = System.currentTimeMillis()
    val sentences = reviews.limit(10).select($"review_id", posexplode(ssplit('text)).as(Seq("pos", "review_sentence")))
    val results  = sentences.withColumn("sentiment", sentiment('review_sentence))
    println(results.count())

    MongoSpark.save(results)
    val t1 = System.currentTimeMillis()
    println("Total time: " + (t1 - t0)/ 1000 + " seconds.")


    /*
    val input = Seq(
      (1, "<xml>Stanford University is located in California. It is a great university.</xml>")
    ).toDF("id", "text")

    val output = input
      .select(cleanxml('text).as('doc))
      .select(explode(ssplit('doc)).as('sen))
      .select('sen, tokenize('sen).as('words), ner('sen).as('nerTags), sentiment('sen).as('sentiment))

    output.show(truncate = false)
    */

  }

}
