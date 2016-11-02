package net.sansa_stack.querying.spark.sparklify

import org.apache.jena.query.QueryFactory
import scala.collection.JavaConversions._

object SPARQLTranslator {

  def ResultSet(queryString: String): Vector[String] = {
    val query = QueryFactory.create(queryString)
    if (query.isSelectType()) {
      query.getResultVars.toVector
    } else if (query.isConstructType() || query.isDescribeType()) {
      Vector("Subject", "Predicate", "Object")
    } else if (query.isAskType()) {
      Vector("ASK")
    } else {
      query.getResultVars.toVector
    }
  }

}