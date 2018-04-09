package edu.sjsu.cs298.corenlp

import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ReviewCounter {

  val REVIEWS_LOCATION = "C:\\Users\\Admin\\Downloads\\YelpDataset11\\dataset\\review.json"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/yelp_reviews.review_50")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/yelp_reviews.review_50")
      .getOrCreate()
    import spark.sqlContext.implicits._

    val reviews = spark.read.json(REVIEWS_LOCATION)

    //println("Total reviews: " + reviews.count())
    val selectedReviews = reviews.groupBy('user_id).count().filter('count > 50)
    //selectedReviews.show()
    println(selectedReviews.count())

    val myReviews = reviews.join(selectedReviews, Seq("user_id"))
    println(myReviews.count())

    MongoSpark.save(myReviews)

  }

}
