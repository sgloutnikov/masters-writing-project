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
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/yelp_reviews.nb_userTupleStats")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/yelp_reviews.nb_userAbnormalityScore")
      .getOrCreate()
    import org.apache.spark.sql.functions._
    import sparkSession.sqlContext.implicits._


    val allTuples = MongoSpark.load(sparkSession)

    val userTupleStats = allTuples.groupBy('user_id, 'sentimentTuple)
      .agg(countDistinct('review_id).as("foundInNumReviews"), count('sentimentTuple).as("sentimentTupleCount"),
        length('sentimentTuple).as("tupleLength"))

    val usersReadConfig = ReadConfig(Map("collection" -> "users"),
      Some(ReadConfig(sparkSession)))

    val users = MongoSpark.load(sparkSession, usersReadConfig)
    val userTupleExtendedStats = userTupleStats.join(users, Seq("user_id"))
      .drop('_id).withColumnRenamed("count", "totalReviews")

    val userTupleExtendedStatsWithFreq = userTupleExtendedStats.withColumn("tupleFrequency",
      'foundInNumReviews.divide('totalReviews))

    val userTupleLengthStats = userTupleExtendedStatsWithFreq.groupBy('user_id, 'tupleLength)
      .agg(sum('sentimentTupleCount), countDistinct('sentimentTuple))
      .withColumnRenamed("sum(sentimentTupleCount)", "totalTuplesOfLength")
      .withColumnRenamed("count(DISTINCT sentimentTuple)", "uniqueTuplesOfLength")

    val userTupleFullStats = userTupleExtendedStatsWithFreq.join(userTupleLengthStats,
      Seq("user_id", "tupleLength"))
        .withColumn("observedTupleLengthFreq", 'sentimentTupleCount.divide('totalTuplesOfLength))
        .withColumn("expectedTupleLenthFreq", lit(1).divide('uniqueTuplesOfLength))
        .withColumn("absDiffObsExpected", abs('observedTupleLengthFreq.minus('expectedTupleLenthFreq)))
        .withColumn("abnormalityScore", pow('tupleFrequency, 2).multiply(pow('tupleLength, 2))
          .multiply(pow('absDiffObsExpected, 2)))


    //val userAbnormalityScore = userTupleFullStats.groupBy('user_id).agg(sum('abnormalityScore)
    //  .as("userAbnormalityScore")).sort(desc("userAbnormalityScore"))


    // Distribution Stats
    //val userAbnormalityScore = MongoSpark.load(sparkSession)
    //println(userAbnormalityScore.count())
    //userAbnormalityScore.describe("userAbnormalityScore").show()

    //val tupleStats = MongoSpark.load(sparkSession)
    //tupleStats.describe("abnormalityScore").show()
  }

}
