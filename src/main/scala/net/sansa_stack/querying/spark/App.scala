package net.sansa_stack.querying.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File
import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import net.sansa_stack.querying.spark.sparklify.SparkSQLifyContext
import org.apache.spark.sql.SQLContext
import net.sansa_stack.querying.spark.utils.{ Logging, SparkUtils }
import net.sansa_stack.querying.spark.sparql2sql.Translator
import net.sansa_stack.querying.spark.model.DataStorageManager
import org.apache.spark.sql.SparkSession

object App extends Logging {

  def main(args: Array[String]): Unit = {

    val sparkMasterUrl = System.getenv("SPARK_MASTER_URL")

   val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    SparkUtils.setLogLevels(org.apache.log4j.Level.WARN, Seq("org.apache", "spark", "org.eclipse.jetty", "akka", "org"))

    logger.info("SparkSQLify running")
    val startTime = System.currentTimeMillis()

    SparkUtils.HDFSPath = "src/test/resources/dbpedia_sample.nt"

    val r = new DataStorageManager(List("vicePresident", "region", "termPeriod"))(sparkSession)
    r.createPT()

    val query = """
   SELECT ?vicePresident
      WHERE
       { <Abraham_Lincoln> <vicePresident> ?vcepresident.
       }
   """
    val translator = Translator(query)
    val sql = translator.getSQL
    val dataFrame = sparkSession.querySparkSQLify(sql, query)
    dataFrame.printSchema()
    dataFrame.show()

    println("finished SparkSQLify in " + (System.currentTimeMillis() - startTime) + "ms.")

  }
}

