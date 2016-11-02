package net.sansa_stack.querying.spark.sparklify

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.types.StructType

package object HBase {

  implicit class SparkSQLifyHBaseContext(sqlContext: SQLContext) extends Serializable {

    def querySparkSQLifyHBase(
      location: String,
      query: String,
      userSchema: StructType = null): DataFrame = {
      
      val sparqlRelation = HBaseRelation(location, query, userSchema)(sqlContext)
      sqlContext.baseRelationToDataFrame(sparqlRelation)
    }
  }
}