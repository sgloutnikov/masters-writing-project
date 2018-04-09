package edu.sjsu.cs298

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ResultsValidation {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/yelp_reviews.userAbnormalityScore")
      .getOrCreate()
    import org.apache.spark.sql.functions._
    import sparkSession.sqlContext.implicits._

    val topLimit = 100
    val cnlpUserStats = MongoSpark.load(sparkSession).sort(desc("userAbnormalityScore")).limit(topLimit)

    val onReadConfig = ReadConfig(Map("collection" -> "nb_userAbnormalityScore"),
      Some(ReadConfig(sparkSession)))
    val nbUserStats = MongoSpark.load(sparkSession, onReadConfig).sort(desc("userAbnormalityScore")).limit(topLimit)

    val matchingStats = cnlpUserStats.join(nbUserStats, Seq("user_id"))
    println(matchingStats.count())
    matchingStats.show(false)

    /*
    // Distribution stats CoreNLP
    val cnlpStats = cnlpUserStats.describe("userAbnormalityScore").collectAsList()
    val mean = cnlpStats.get(1).getAs[String](1).toDouble
    val stddev = cnlpStats.get(2).getAs[String](1).toDouble

    val aboveMean = cnlpUserStats.filter('userAbnormalityScore.gt(mean))
    val oneStdAboveMean = cnlpUserStats.filter('userAbnormalityScore.gt(mean+stddev))
    val twoStdAboveMean = cnlpUserStats.filter('userAbnormalityScore.gt(mean+(2*stddev)))
    val belowOneStd = cnlpUserStats.filter('userAbnormalityScore.lt(mean+stddev))
    println("# Above Mean: " + aboveMean.count())
    println("# 1 STD Above Mean: " + oneStdAboveMean.count())
    println("# 2 STD Above Mean: " + twoStdAboveMean.count())
    println("# Below 1 STD: " + belowOneStd.count())
    */

    /*
    // Distribution stats Naive Bayes
    val nbStats = nbUserStats.describe("userAbnormalityScore").collectAsList()
    val mean = nbStats.get(1).getAs[String](1).toDouble
    val stddev = nbStats.get(2).getAs[String](1).toDouble

    val aboveMean = nbUserStats.filter('userAbnormalityScore.gt(mean))
    val oneStdAboveMean = nbUserStats.filter('userAbnormalityScore.gt(mean+stddev))
    val twoStdAboveMean = nbUserStats.filter('userAbnormalityScore.gt(mean+(2*stddev)))
    val belowOneStd = nbUserStats.filter('userAbnormalityScore.lt(mean+stddev))
    println("# Above Mean: " + aboveMean.count())
    println("# 1 STD Above Mean: " + oneStdAboveMean.count())
    println("# 2 STD Above Mean: " + twoStdAboveMean.count())
    println("# Below 1 STD: " + belowOneStd.count())
    */
  }
}
