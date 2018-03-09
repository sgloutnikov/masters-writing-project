package edu.sjsu.cs298

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._

object SparkCoreNLP {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("WritingProject")
      .master("local[*]")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val input = Seq(
      (1, "<xml>Stanford University is located in California. It is a great university.</xml>")
    ).toDF("id", "text")

    val output = input
      .select(cleanxml('text).as('doc))
      .select(explode(ssplit('doc)).as('sen))
      .select('sen, tokenize('sen).as('words), ner('sen).as('nerTags), sentiment('sen).as('sentiment))

    output.show(truncate = false)

  }

}
