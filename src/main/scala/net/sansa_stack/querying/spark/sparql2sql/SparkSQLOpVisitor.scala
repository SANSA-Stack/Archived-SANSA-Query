package net.sansa_stack.querying.spark.sparql2sql

import org.apache.jena.graph.{ Node, Triple }
import org.apache.jena.sparql.algebra.OpVisitor
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.expr._
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.graph

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
import org.apache.jena.query.QueryFactory
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.shared.PrefixMapping

class SparkSQLOpVisitor(tableName: String) extends OpVisitor {

  var selectClause = "select "
  var fromClause = s"from $tableName "
  var whereClause = "WHERE ";
  var groupClause = "GROUP BY ";
  var havingClause = "HAVING ";
  var currentQueryStr = "";

  var bgpStarted: Boolean = false
  var queries: List[SparqlQuery] = null
  var filterList = List[String]()

  var previousSelect = List[String]()
  var bgpBindings = List[List[Binding]]();
  
  var uniquePredicatesList  = List[String]()

  override def visit(opTable: OpTable) {}

  override def visit(opNull: OpNull) {}

  override def visit(opProc: OpProcedure) {}

  override def visit(opTop: OpTopN) {}

  override def visit(opGroup: OpGroup) {}

  override def visit(opSlice: OpSlice) {
    //    var previousSelect = previousSelects.remove(previousSelects.size()-1);
    //		previousSelect += "LIMIT " + opSlice.getLength();
    //		previousSelects.add(previousSelect);

    //    selectClause += "LIMIT " + opSlice.getLength();
  }

  override def visit(opDistinct: OpDistinct) {

    //selectClause = selectClause.replaceFirst("select", "SELECT DISTINCT")

  }

  override def visit(opReduced: OpReduced) {}

  override def visit(opProject: OpProject) {
    val varsStr = opProject.getVars.toList.map(_.getName).mkString(", ")
    selectClause += varsStr

    for (filterStr <- filterList) {
      if (!filterStr.trim().equals("")) {
        var modifier = "";
        if (!whereClause.equals("WHERE ")) {
          modifier = " AND ";
        }
        whereClause += modifier + filterStr;
      }
    }
    filterList.clear();

    if (bgpStarted) {
      bgpStarted = false;
    }
  }

  override def visit(opPropFunc: OpPropFunc) {}

  override def visit(opFilter: OpFilter) {}

  override def visit(opGraph: OpGraph) {}

  override def visit(opService: OpService) {}

  override def visit(opPath: OpPath) {}

  override def visit(opQuad: OpQuad) {}

  override def visit(opTriple: OpTriple) {}

  override def visit(quadBlock: OpQuadBlock) {}

  override def visit(quadPattern: OpQuadPattern) {}

  override def visit(opBGP: OpBGP) {
    bgpStarted = true
    queries = opBGP.getPattern.getList.toList.map(SparqlQuery(_))
    
    val patterns = opBGP.getPattern().getList();
    var queryStr = "SELECT * WHERE {\n";
    for (pattern <- patterns) {
      queryStr += "\t" + nodeToString(pattern.getSubject()) + " " + nodeToString(pattern.getPredicate()) + " " + nodeToString(pattern.getObject()) + ".\n";
      uniquePredicatesList ::= nodeToString(pattern.getPredicate())
    }
    queryStr += "}";
    currentQueryStr = queryStr;

    var triples = opBGP.getPattern().getList()
    val prefixes = PrefixMapping.Factory.create();
    //var tripleGroups = new HashMap[Node, TripleGroup]();

  }
  override def visit(dsNames: OpDatasetNames) {}

  override def visit(opLabel: OpLabel) {}

  override def visit(opAssign: OpAssign) {}

  override def visit(opExtend: OpExtend) {}

  override def visit(opJoin: OpJoin) {}

  override def visit(opLeftJoin: OpLeftJoin) {}

  override def visit(opUnion: OpUnion) {}

  override def visit(opDiff: OpDiff) {}

  override def visit(opMinus: OpMinus) {}

  override def visit(opCondition: OpConditional) {}

  override def visit(opSequence: OpSequence) {}

  override def visit(opDisjunction: OpDisjunction) {}

  override def visit(opExt: OpExt) {}

  override def visit(opList: OpList) {}

  override def visit(opOrder: OpOrder) {}

  def getSQL(): String = s"$selectClause $fromClause"
  //  {
  //		if(previousSelects.size()>0) {
  //			return previousSelects.get(0);
  //		}
  //		null;
  //  }

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

object SparkSQLOpVisitor {
  def apply(tableName: String) = new SparkSQLOpVisitor(tableName)
}