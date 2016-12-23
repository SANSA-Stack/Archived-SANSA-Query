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
import scala.collection.mutable.Stack
import org.apache.calcite.sql.SqlOperator
import scala.collection.mutable.LinkedList
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Map
import java.util.Arrays

class SparkSQLOpVisitor(tableName: String) extends OpVisitor {

  var selectClause = "select "
  var fromClause = s"from $tableName "
  var whereClause = "WHERE ";
  var groupClause = "GROUP BY ";
  var havingClause = "HAVING ";
  var currentQueryStr = "";
  var bgpStarted: Boolean = false
  var queries: List[SparqlQuery] = null
  var filterList = ListBuffer[String]()

  var previousSelect = List[String]()
  var bgpBindings = List[List[Binding]]();

  var uniquePredicatesList = List[String]()
  var _vars = List[String]()
  var _joinVars = List[String]()
  var sqlOpStack = Stack[String]()

  var varToSet = new HashMap[String, String]()

  var varMapping = new HashMap[String, String]();

  override def visit(opTable: OpTable) {}

  override def visit(opNull: OpNull) {}

  override def visit(opProc: OpProcedure) {}

  override def visit(opTop: OpTopN) {}

  override def visit(opGroup: OpGroup) {
    for (e <- opGroup.getGroupVars().getExprs().entrySet()) {
      add(processVar(e.getKey()))
      for (v <- e.getValue().getVarsMentioned()) {
        add(processVar(v))
      }
    }
    for (e <- opGroup.getAggregators()) {
      add(processVar(e.getAggVar().asVar()));
      if (e.getExpr() != null) {
        for (v <- e.getExpr().getVarsMentioned()) {
          add(processVar(v));
        }
      }
      if (e.getAggregator().getExprList != null) {
        for (v <- e.getAggregator().getExprList.getVarsMentioned()) {
          add(processVar(v));
        }
      }
    }
  }

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
    for (v <- opProject.getVars()) {
      println("v: " + v)
      add(processVar(v))
    }

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

  override def visit(opFilter: OpFilter) {
    for (e <- opFilter.getExprs()) {
      processExpr(e)
    }

    var filterStr = "";
    for (filter <- opFilter.getExprs().getList()) {
      val v = new FilterExprVisitor();
      v.setMapping(varMapping);
      ExprWalker.walk(v, filter);
      v.finishVisit();
      var modifier = "";
      if (!v.getExpression().equals("")) {
        if (!filterStr.equals("")) {
          modifier = " AND ";
        }
        filterStr += modifier + v.getExpression();
        //				whereClause += modifier + v.getExpression();
      } else if (!v.getHavingExpression().equals("")) {
        if (!havingClause.equals("HAVING ")) {
          modifier = " AND ";
        }
        havingClause += modifier + v.getHavingExpression();
      }
    }
    println("filterStr: " + filterStr)
    filterList.add(filterStr);
  }

  override def visit(opGraph: OpGraph) {
    process(opGraph.getNode());
  }

  override def visit(opService: OpService) {}

  override def visit(opPath: OpPath) {}

  override def visit(opQuad: OpQuad) {}

  override def visit(opTriple: OpTriple) {
    process(opTriple.getTriple().getSubject())
    process(opTriple.getTriple().getPredicate())
    process(opTriple.getTriple().getObject())
  }

  override def visit(quadBlock: OpQuadBlock) {}

  override def visit(quadPattern: OpQuadPattern) {

  }

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
    for (node <- opBGP.getPattern) {
      val subValue = node.getSubject
    }
    for (t <- opBGP.getPattern.getList) {
      println("t: " + t)
      process(t.getSubject());
      process(t.getPredicate());
      process(t.getObject());
    }

  }
  override def visit(dsNames: OpDatasetNames) {}

  override def visit(opLabel: OpLabel) {}

  override def visit(opAssign: OpAssign) {}

  override def visit(opExtend: OpExtend) {
    for (v <- opExtend.getVarExprList().getExprs().entrySet()) {
      add(processVar(v.getKey()))
      processExpr(v.getValue())
    }
  }

  override def visit(opJoin: OpJoin) {}

  override def visit(opLeftJoin: OpLeftJoin) {
    if (opLeftJoin.getExprs() != null) {
      for (e <- opLeftJoin.getExprs()) {
        processExpr(e)
      }
    }
  }

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

  def process(n: Node) {
    if (n.isVariable())
      add(processVar(n))
  }
  def processVar(v: Node) = v.getName

  def processExpr(e: Expr) {
    for (v <- e.getVarsMentioned()) {
      add(processVar(v))
    }
  }

  def add(x: Any) {
    println("x: " + x)
    if (!result.contains(x))
      result.add(x)
  }
  val result = new ListBuffer[Any]()

  def getResult() = result

}

object SparkSQLOpVisitor {
  def apply(tableName: String) = new SparkSQLOpVisitor(tableName)
}

class FilterExprVisitor extends ExprVisitor {
  var expression = ""
  var havingExpression = ""
  var currentPart = ""
  var combinePart = ""
  val functionList = List("<", ">", "=", "!=", "<=", ">=")
  var havingParts = ListBuffer[String]()
  var exprParts = ListBuffer[String]()

  var varMapping: Map[String, String] = null

  def setMapping(varMapping: Map[String, String]) {
    this.varMapping = varMapping
  }
  def getExpression() = expression
  def getHavingExpression() = havingExpression
  def finishVisit() {}
  def startVisit() {}
  def visit(arg0: ExprFunction0) {}
  def visit(arg0: ExprFunction1) {}
  def visit(args: ExprFunction2) {
    println(functionList)
    if (functionList.contains(args.getOpName())) {
      val leftSide = args.getArg1();
      val rightSide = args.getArg2();
      currentPart += handleExpr(leftSide) + args.getOpName() + handleExpr(rightSide)
      val t = leftSide.getVarName()//.startsWith(".")
      println("t:here: " +t)
      if (leftSide.getVarName().startsWith(".")) {
        havingParts.add(currentPart);
      } else {
        exprParts.add(currentPart);
      }
      currentPart = "";
    } else if (args.getOpName().equals("&&"))
      combinePart = " AND ";
    else if (args.getOpName().equals("||"))
      combinePart = " OR ";
    
  }
  def visit(arg0: ExprFunction3) {}

  def visit(arg0: ExprFunctionN) {}

  def visit(arg0: ExprFunctionOp) {}

  def visit(arg0: NodeValue) {}

  def visit(exprVar: ExprVar) {}

  def visit(arg0: ExprAggregator) {}
  
  
  def parseNodeValue(node: NodeValue) {
    if (node.isDateTime()) {
      "'" + node.getDateTime().toString() + "'";
    } else if (node.isInteger()) {
      node.getInteger().toString();
    } else if (node.isFloat()) {
      Float.toString().format(node.getFloat())
    } else {
       "'" + node.getString() + "'";
    }
  }
  def handleExpr(expr:Expr) {
		if(expr.isVariable()) {
			expr.getVarName()
		} else if(expr.isConstant()) {
			parseNodeValue(expr.getConstant());
		}
		expr.toString();
  }
}
