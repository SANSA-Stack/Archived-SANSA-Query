package net.sansa_stack.querying.spark

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.types.StructType

package object sparklify {

  implicit class SparkSQLifyContext(sqlContext: SQLContext) extends Serializable {

    def querySparkSQLify(
      location: String,
      query: String,
      userSchema: StructType = null): DataFrame = {
      
      val sparqlRelation = SparkSQLRelation(location, query, userSchema)(sqlContext)
      println(sparqlRelation)
      sqlContext.baseRelationToDataFrame(sparqlRelation)
    }
  }
}