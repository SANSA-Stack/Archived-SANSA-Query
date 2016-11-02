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

object App extends Logging {

  def main(args: Array[String]): Unit = {

    val sparkMasterUrl = System.getenv("SPARK_MASTER_URL")

    val conf = new SparkConf().setAppName("SparkSQLify").setMaster("local[*]")
    conf.setSparkHome("/opt/spark-1.5.1")
    conf.setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    SparkUtils.setLogLevels(org.apache.log4j.Level.WARN, Seq("org.apache", "spark", "org.eclipse.jetty", "akka", "org"))

    logger.info("SparkSQLify running")
    val startTime = System.currentTimeMillis()

    SparkUtils.HDFSPath = "src/test/resources/dbpedia_sample.nt"

    val query = """
   SELECT ?vicePresident
      WHERE
       { <Abraham_Lincoln> <vicePresident> ?vicepresident.
       }
   """
    val translator = Translator(query)
    val sql = translator.getSQL

    val r = new DataStorageManager(List("vicePresident", "region", "termPeriod"))(sc, sqlContext)
    r.createPT()

    val dataFrame = sqlContext.querySparkSQLify(sql, query)
    dataFrame.printSchema()
    dataFrame.show()

    println("finished SparkSQLify in " + (System.currentTimeMillis() - startTime) + "ms.")

  }
}

