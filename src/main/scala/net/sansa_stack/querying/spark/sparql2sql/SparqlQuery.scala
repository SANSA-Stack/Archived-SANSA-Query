package net.sansa_stack.querying.spark.sparql2sql

import org.apache.jena.graph.{ Node, Triple }
import net.sansa_stack.querying.spark.utils.SparkUtils

class SparqlQuery(val vars: List[String], val query: String) {

  override def toString = s"$query"
}

object SparqlQuery {
  def apply(triple: Triple): SparqlQuery = {
    val nodeList = List(triple.getSubject, triple.getPredicate, triple.getObject)
    val vars = nodeList.filter(_.isVariable).map(_.getName)
      
    val nodeStrList = nodeList.map(nodeToString)
    val tripleStr = nodeStrList.mkString(" ")
    val varStr = nodeStrList.filter(_.startsWith("?")).mkString(" ")
    val query = s"SELECT $varStr { $tripleStr }"

    new SparqlQuery(vars, query)
  }

  private def nodeToString(node: Node): String = {
    if (node.isURI) {
      return "<" + node.toString + ">"
    } else if (node.isLiteral) {
      var literal = "\"" + node.getLiteralValue() + "\""
      if (node.getLiteralDatatypeURI() != null) {
        literal += "^^<" + node.getLiteralDatatypeURI() + ">";
      }
      return literal;
    } else {
      var nodeStr = node.toString()
      if (nodeStr.startsWith("??")) {
        nodeStr = nodeStr.replace("??", "?bn")
      }
      return nodeStr
    }
    node.toString
  }

}