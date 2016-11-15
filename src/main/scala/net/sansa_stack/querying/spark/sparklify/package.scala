package net.sansa_stack.querying.spark

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.types.StructType

package object sparklify {

  implicit class SparkSQLifyContext(sparkSession: SparkSession) extends Serializable {

    def querySparkSQLify(
      location: String,
      query: String,
      userSchema: StructType = null): DataFrame = {

      val sparqlRelation = SparkSQLRelation(location, query, userSchema)(sparkSession.sqlContext)
      println(sparqlRelation)
      sparkSession.sqlContext.baseRelationToDataFrame(sparqlRelation)
    }
  }
}