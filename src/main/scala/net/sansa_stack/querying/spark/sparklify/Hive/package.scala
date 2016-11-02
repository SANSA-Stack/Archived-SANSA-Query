package net.sansa_stack.querying.spark.sparklify

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.types.StructType

package object Hive {

  implicit class SparkSQLifyHiveContext(sqlContext: SQLContext) extends Serializable {

    def querySparkSQLifyHive(
      location: String,
      query: String,
      userSchema: StructType = null): DataFrame = {
      
      val sparqlRelation = HiveRelation(location, query, userSchema)(sqlContext)
      sqlContext.baseRelationToDataFrame(sparqlRelation)
    }
  }
}