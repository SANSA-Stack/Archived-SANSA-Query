package net.sansa_stack.querying.spark.sparklify.HBase

import org.apache.spark.sql.sources.{ BaseRelation, TableScan }
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import java.sql.{ ResultSet, DriverManager, Connection }
import org.apache.jena.jdbc.mem.MemDriver
import scala.collection.mutable.ArrayBuffer
import net.sansa_stack.querying.spark.utils.SparkUtils
import org.apache.spark.input.PortableDataStream
import net.sansa_stack.querying.spark.sparklify.SPARQLTranslator
import org.apache.hadoop.hbase.HBaseConfiguration

case class HBaseRelation(sqlQuery: String, query: String, userSchema: StructType)(@transient val sqlContext: SQLContext)
    extends BaseRelation with TableScan with Serializable {

  override def schema: StructType = {
    if (this.userSchema != null) {
      return this.userSchema
    } else {
      val varNames = SPARQLTranslator.ResultSet(query)
      val schemaFields = varNames.map { fieldName =>
        StructField(fieldName, StringType, nullable = true)
      }
      StructType(schemaFields)
    }
  }

  override def buildScan(): RDD[Row] = {
    tripleRows
  }

  private def tripleRows(): RDD[Row] = {
    val schemaFields = schema.fields
    val conf = HBaseConfiguration.create()

   // val hbaseContext = new HBaseContext(sc, conf)

    println("Not implemented yet")
    null
  }
}  