package edu.sjsu.cs298

import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Users {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/yelp_reviews.review_50")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/yelp_reviews.users")
      .getOrCreate()
    import sparkSession.sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val allReviews = MongoSpark.load(sparkSession)
    val users = allReviews.groupBy("user_id").count()
    users.show(truncate = false)
    println(users.count())
    MongoSpark.save(users)

  }

}
