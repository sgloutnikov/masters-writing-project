package edu.sjsu.cs298

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Results {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/yelp_reviews.sentimentTuples")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/yelp_reviews.userScoreTest1")
      .getOrCreate()
    import sparkSession.sqlContext.implicits._
    import org.apache.spark.sql.functions._


    val allTuples = MongoSpark.load(sparkSession)
    //allTuples.√∑filter(length('sentimentTuple).gt(5) && length('sentimentTuple).lt(15)).groupBy('sentimentTuple, 'user_id)
    //  .count().sort(desc("count")).show(1000, truncate = false)

    //allTuples.filter('user_id === "QuZbJquRtbY9O9JrB9NpxQ" && length('sentimentTuple).gt(200)).groupBy('sentimentTuple)
    //  .count().sort(desc("count")).show(2000, truncate = false)

    val userTupleStats = allTuples.groupBy('user_id, 'sentimentTuple)
      .agg(countDistinct('review_id).as("foundInNumReviews"), count('sentimentTuple).as("sentimentTupleCount"),
        length('sentimentTuple).as("tupleLength"))


    // Read users table from different collection
    val usersReadConfig = ReadConfig(Map("collection" -> "users"),
      Some(ReadConfig(sparkSession)))

    val users = MongoSpark.load(sparkSession, usersReadConfig)
    val userTupleExtendedStats = userTupleStats.join(users, Seq("user_id"))
      .drop('_id).withColumnRenamed("count", "totalReviews")

    val userTupleExtendedStatsWithFreq = userTupleExtendedStats.withColumn("tupleFrequency",
      'foundInNumReviews.divide('totalReviews))

    /*
    val tupleWeight = userTupleExtendedStatsWithFreq.groupBy('user_id, 'tupleLength)
      .agg(max("tupleFrequency"))
      .withColumnRenamed("max(tupleFrequency)", "maxFreq")
      .withColumn("tupleLengthWeight", 'tupleLength.multiply('tupleLength).multiply('maxFreq))
    */

    //New with distinct
    val tupleWeight = userTupleExtendedStatsWithFreq.groupBy('user_id, 'tupleLength)
      .agg(max("tupleFrequency"), countDistinct('sentimentTuple))
      .withColumnRenamed("max(tupleFrequency)", "maxFreq")
      .withColumnRenamed("count(DISTINCT sentimentTuple)", "countDistinctTuple")
      .withColumn("tupleLengthWeight", 'tupleLength.multiply('tupleLength).multiply('maxFreq))


    val userScore = tupleWeight.groupBy('user_id).sum("tupleLengthWeight")
      .withColumnRenamed("sum(tupleLengthWeight)", "userScore")
      .sort(desc("userScore"))


    /*
    val userScoreDescribed = userScore.describe("userScore")
    userScoreDescribed.show(100)
    */

    userScore.show(200, truncate = false)
    //MongoSpark.save(userScore)

  }

}
