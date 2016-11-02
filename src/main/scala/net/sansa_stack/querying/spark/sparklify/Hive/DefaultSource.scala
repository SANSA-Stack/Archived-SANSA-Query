package net.sansa_stack.querying.spark.sparklify.Hive

import org.apache.spark.sql.sources.{ SchemaRelationProvider, RelationProvider, DataSourceRegister }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType

class DefaultSource extends RelationProvider with SchemaRelationProvider with DataSourceRegister {

  override def shortName(): String = "SparkSQLifyHive"

  /*
   * Creates a new relation for data store.
   * @parameters: should include location of the data and sparql query.
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): HiveRelation= {
    createRelation(sqlContext, parameters,null)
  }

  /*
   * Creates a new relation for data store.
   * @parameters: should include location of the data and sparql query.
   * @schema - user defined schema.
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): HiveRelation= {
    HiveRelation(checkPath(parameters), checkQuery(parameters), schema)(sqlContext)
  }
  

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' is required!"))
  }

  private def checkQuery(parameters: Map[String, String]): String = {
    parameters.getOrElse("query", sys.error("'query' is required!"))
  }

}