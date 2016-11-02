package net.sansa_stack.querying.spark.model

import net.sansa_stack.querying.spark.io.TripleReader
import net.sansa_stack.querying.spark.utils.SparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/*
 * DataStorageManager manages PropertyTable and other backend strategies for SparkSQLify. 
 * @author Gezim Sejdiu
 */
class DataStorageManager(uniquePredicatesList: List[String])(@transient val sc: SparkContext, @transient val sql_c: SQLContext) extends Serializable {

  import sql_c.implicits._

  val columns = Seq("sub", "pred", "obj")
  val u_predicates = sc.broadcast(sc.parallelize(uniquePredicatesList).distinct().collect().toList)

  /*
 * createPTT - creates TripleTable with (s,p,o) where p in on uniquePredicatesList.
 */
  def createPTT() = {
       val ptt_df = TripleReader.loadFromFile(SparkUtils.HDFSPath, sc)
      .filter {
        case f: Triples => u_predicates.value.contains(f.pred.toString())
      }
      .map(f => (f.subj.toString(), f.pred.toString(), f.obj.toString()))
      // .filter(f=>f.pred.getLiteralLexicalForm in uniquePredicatesList.map(_)))
      .toDF(columns: _*)
    ptt_df.registerTempTable("Triples")
    sql_c.cacheTable("Triples")
  }

  def createTT() = {
    val ptt_df = TripleReader.loadFromFile(SparkUtils.HDFSPath, sc)
      .filter {
        case f: Triples => u_predicates.value.contains(f.pred.toString())
      }
      .map(f => (f.subj.toString(), f.pred.toString(), f.obj.toString()))
      // .filter(f=>f.pred.getLiteralLexicalForm in uniquePredicatesList.map(_)))
      .toDF(columns: _*)
    ptt_df.registerTempTable("TriplesTable")
    sql_c.cacheTable("TriplesTable")
  }

  def transpose(df: DataFrame, compositeId: List[String], key: String, value: String) = {

    val distinctCols = df.select(key).distinct.map { r => r(0) }.collect().toList
    sc.parallelize(compositeId)

    val rdd = df.map { row =>
      (compositeId.collect { case id => row.getAs(id).asInstanceOf[Any] },
        scala.collection.mutable.Map(row.getAs(key).asInstanceOf[Any] -> row.getAs(value).asInstanceOf[Any]))
    }
    val pairRdd = rdd.reduceByKey(_ ++ _)
    val rowRdd = pairRdd.map(r => dynamicRow(r, distinctCols))
    sql_c.createDataFrame(rowRdd, getSchema(df.schema, compositeId, (key, distinctCols)))

  }

  private def dynamicRow(r: (List[Any], scala.collection.mutable.Map[Any, Any]), colNames: List[Any]) = {
    val cols = colNames.collect { case col => r._2.getOrElse(col.toString(), null) }
    val array = r._1 ++ cols
    Row(array: _*)
  }

  private def getSchema(srcSchema: StructType, idCols: List[String], distinctCols: (String, List[Any])): StructType = {
    val idSchema = idCols.map { idCol => srcSchema.apply(idCol) }
    val colSchema = srcSchema.apply(distinctCols._1)
    val colsSchema = distinctCols._2.map { col => StructField(col.asInstanceOf[String], colSchema.dataType, colSchema.nullable) }
    StructType(idSchema ++ colsSchema)
  }

  /*
 * createPT - creates PropertyTable with (s,<List<uniquePredicatesList>>) generated from PTT..
 */
  def createPT() = {
    createTT()
    var _tt = sql_c.sql("select * "
      + "from TriplesTable")
     _tt.show()


    val dfOutput = transpose(_tt, List("sub"), "pred", "obj")
     dfOutput.registerTempTable("PropertyTable")
    sql_c.cacheTable("PropertyTable")
    
    dfOutput.show
    dfOutput.printSchema()

/*
    for (predicate <- uniquePredicatesList) {
      var p_Table = sql_c.sql("select sub, obj "
        + "from triples where pred='" + predicate + "'")
    }*/

  }

  object DataStorageManager {
    def apply(uniquePredicatesList: List[String])(sc: SparkContext, sql_c: SQLContext) = new DataStorageManager(uniquePredicatesList)(sc, sql_c)
  }
}