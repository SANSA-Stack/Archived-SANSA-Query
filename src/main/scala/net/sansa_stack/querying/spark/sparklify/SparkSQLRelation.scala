package net.sansa_stack.querying.spark.sparklify

import org.apache.spark.sql.sources.{ BaseRelation, TableScan }
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import java.sql.{ ResultSet, DriverManager, Connection }
import org.apache.jena.jdbc.mem.MemDriver
import scala.collection.mutable.ArrayBuffer
import net.sansa_stack.querying.spark.utils.SparkUtils
import org.apache.spark.input.PortableDataStream

/**
 *
 * @author Gezim Sejdiu
 *
 */
case class SparkSQLRelation(sqlQuery: String, query: String, userSchema: StructType)(@transient val sqlContext: SQLContext)
    extends BaseRelation with TableScan with Serializable {

  override def schema: StructType = {
    if (this.userSchema != null) {
      return this.userSchema
    } else {
      val varNames = SPARQLTranslator.ResultSet(query)
      // By default fields are assumed to be StringType
      val schemaFields = varNames.map { fieldName =>
        StructField(fieldName, StringType, nullable = true)
      }
      StructType(schemaFields)
    }
  }

  private val TripleSchema: StructType = StructType(Seq(
    StructField("Subject", StringType, nullable = false, Metadata.empty),
    StructField("Predicate", StringType, nullable = false, Metadata.empty),
    StructField("Object", StringType, nullable = false, Metadata.empty)))

  override def buildScan(): RDD[Row] = {
    //val filesRDD = sqlContext.sparkContext.binaryFiles(location)
    //filesRDD.flatMap(tripleRows)
    tripleRows
  }

  private def tripleRows(): RDD[Row] = {
    val schemaFields = schema.fields
    /*    val filesRDD = sqlContext.sparkContext.binaryFiles(SparkUtils.HDFSPath)
    */
    val temQuery = """
        SELECT DISTINCT sub  FROM PropertyTable
       WHERE sub is not null
       AND vicePresident is not null
       AND vicePresident  ='Andrew_Johnson'
      """
    val rs_sql = sqlContext.sql(temQuery)
    println(rs_sql.queryExecution.executedPlan)

    val rows = ArrayBuffer[Row]()

    /*  rs_sql.foreach { rs=>
    //val values = schemaFields.map(castValue(_, rs))
          rows += Row.fromSeq(values)
      //    println("values:" + values.toString())
        }*/
    //sqlContext.sparkContext.makeRDD(rs_sql.collect().toSeq)
    rs_sql.rdd
  }

  private def castValue(field: StructField, rs: ResultSet): AnyRef = field.dataType match {
    case _: ByteType =>
      val v = rs.getByte(field.name)
      if (rs.wasNull()) null else byte2Byte(v)
    case _: IntegerType =>
      val v = rs.getInt(field.name)
      if (rs.wasNull()) null else int2Integer(v)
    case _: DoubleType =>
      val v = rs.getDouble(field.name)
      if (rs.wasNull()) null else double2Double(v)
    case _: StringType => rs.getString(field.name)
    case _: BooleanType =>
      val v = rs.getBoolean(field.name)
      if (rs.wasNull()) null else boolean2Boolean(v)
    case _: TimestampType =>
      rs.getTimestamp(field.name)
    case _ =>
      rs.getString(field.name)
  }

}  