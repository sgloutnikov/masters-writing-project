package edu.sjsu.cs298

import java.util

import com.mongodb.spark.MongoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.util.ArrayList

import scala.collection.mutable.ListBuffer

object TupleGenerator {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/yelp_reviews.sentimentVectors")
      .getOrCreate()

    val vectors = MongoSpark.load(spark).limit(3)
    vectors.show()


  }

  def generateTuple(sVector: String): List[String] = {
    val tupleList = new ListBuffer[String]

    tupleList += "abc"
    tupleList += "def"

    tupleList.toList
  }



}
