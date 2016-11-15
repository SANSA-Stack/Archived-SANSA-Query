package net.sansa_stack.querying.spark.sparql2sql

import org.apache.jena.graph.Node
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.{ OpWalker, Algebra }
//import org.mp.base.SparqlQuery

import scala.collection.JavaConversions._
import net.sansa_stack.querying.spark.sparklify.SPARQLTranslator
import net.sansa_stack.querying.spark.utils.SparkUtils

class Translator(queryStr: String) {

  private val query = QueryFactory.create(queryStr)
  private val op = Algebra.compile(query);
  println("op:" + op)
  private val visitor = SparkSQLOpVisitor("PropertyTable")
  OpWalker.walk(op, visitor)

  SparkUtils.uniquePredicatesList = visitor.uniquePredicatesList

  val getSQL = visitor.getSQL()
  val prefixStr = query.getPrefixMapping()
    .getNsPrefixMap
    .toList
    .map(prefix => String.format("PREFIX %s: %s", prefix._1, prefix._2))
    .mkString("\n")

  val queries = visitor.queries
}

object Translator {
  def apply(queryStr: String) = new Translator(queryStr)
}

