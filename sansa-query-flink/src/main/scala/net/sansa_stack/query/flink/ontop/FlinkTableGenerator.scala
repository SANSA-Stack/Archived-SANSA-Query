package net.sansa_stack.query.flink.ontop

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import scala.reflect.runtime.universe.typeOf

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment

import net.sansa_stack.query.common.ontop.{BlankNodeStrategy, SQLUtils}
import net.sansa_stack.rdf.common.partition.core.RdfPartitionComplex
import org.apache.flink.table.api._
import org.apache.flink.types.Row
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.api.scala._

import net.sansa_stack.rdf.common.partition.schema.{SchemaStringDate, SchemaStringDouble, SchemaStringLong, SchemaStringString, SchemaStringStringLang, SchemaStringStringType}

/**
 * @author Lorenz Buehmann
 */
class FlinkTableGenerator(env: BatchTableEnvironment,
                          blankNodeStrategy: BlankNodeStrategy.Value = BlankNodeStrategy.Table) {

  val logger = com.typesafe.scalalogging.Logger(classOf[FlinkTableGenerator])

  /**
   * Creates and registers a Flink table p(s,o) for each partition.
   *
   * Note: partitions with string literals as object will be kept into a single table per property and a 3rd column for the
   * (optional) language tag is used instead.
   *
   * @param partitions the partitions
   */
  def createAndRegisterFlinkTables(partitions: Map[RdfPartitionComplex, DataSet[Product]]): Unit = {

    // register the lang tagged RDDs as a single table:
    // we have to merge the RDDs of all languages per property first, otherwise we would always replace it by another
    // language
    partitions
      .filter(_._1.lang.nonEmpty)
      .map { case (p, ds) => (p.predicate, p, ds) }
      .groupBy(_._1)
      .map { case (k, v) =>
        val ds = v.map(_._3).reduce((a, b) => a.union(b))
        val p = v.head._2
        (p, ds)
      }

      .map { case (p, ds) => (SQLUtils.createTableName(p, blankNodeStrategy), p, ds) }
      .groupBy(_._1)
      .map(map => map._2.head)
      .map(e => (e._2, e._3))
      .foreach { case (p, ds) => createFlinkTable(p, ds) }

    // register the non-lang tagged RDDs as table
    partitions
      .filter(_._1.lang.isEmpty)
      .map { case (p, ds) => (SQLUtils.createTableName(p, blankNodeStrategy), p, ds) }
      .groupBy(_._1)
      .map(map => map._2.head)
      .map(e => (e._2, e._3))

      .foreach {
        case (p, ds) => createFlinkTable(p, ds)
      }
  }

  /**
   * creates a Flink table for each RDF partition
   */
  private def createFlinkTable(p: RdfPartitionComplex, ds: DataSet[Product]) = {

    var tableName = SQLUtils.createTableName(p, blankNodeStrategy)
    logger.debug(s"creating Spark table ${escapeTablename(tableName)}")
    tableName = escapeTablename(tableName)

//    dataSet.map(p => Row.of(p.productIterator.toSeq)).print()
//    env.fromDataSet(dataSet, $"s", $"o").as(escapeTablename(tableName)) // createTemporaryView(escapeTablename(name), dataSet)

    val q = p.layout.schema
    q match {
      case q if q =:= typeOf[SchemaStringLong] =>
        var fn = (r: Product) => r.asInstanceOf[SchemaStringLong]
        env.createTemporaryView(tableName, ds.map (fn))
      case q if q =:= typeOf[SchemaStringString] =>
        var fn = (r: Product) => r.asInstanceOf[SchemaStringString]
        env.createTemporaryView(tableName, ds.map (fn))
      case q if q =:= typeOf[SchemaStringStringType] =>
        var fn = (r: Product) => r.asInstanceOf[SchemaStringStringType]
        env.createTemporaryView(tableName, ds.map (fn))
      case q if q =:= typeOf[SchemaStringDouble] =>
        var fn = (r: Product) => r.asInstanceOf[SchemaStringDouble]
        env.createTemporaryView(tableName, ds.map (fn))
      case q if q =:= typeOf[SchemaStringStringLang] =>
        var fn = (r: Product) => r.asInstanceOf[SchemaStringStringLang]
        env.createTemporaryView(tableName, ds.map (fn))
      case q if q =:= typeOf[SchemaStringDate] =>
        var fn = (r: Product) => r.asInstanceOf[SchemaStringDate]
        env.createTemporaryView(tableName, ds.map (fn))
      case _ =>
        throw new RuntimeException("Unhandled schema type: " + q)
    }
  }

  private def escapeTablename(path: String): String =
    URLEncoder.encode(path, StandardCharsets.UTF_8.toString)
      .toLowerCase
      .replace('%', 'P')
      .replace('.', 'C')
      .replace("-", "dash")

}

object FlinkTableGenerator {
  def apply(env: BatchTableEnvironment): FlinkTableGenerator = new FlinkTableGenerator(env)

  def apply(env: BatchTableEnvironment,
            blankNodeStrategy: BlankNodeStrategy.Value): FlinkTableGenerator =
    new FlinkTableGenerator(env, blankNodeStrategy)
}


