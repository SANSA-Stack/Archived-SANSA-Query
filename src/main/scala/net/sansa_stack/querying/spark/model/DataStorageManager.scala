package net.sansa_stack.querying.spark.model

import net.sansa_stack.querying.spark.io.TripleReader
import net.sansa_stack.querying.spark.utils.SparkUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders

/*
 * DataStorageManager manages PropertyTable and other backend strategies for SparkSQLify. 
 * @author Gezim Sejdiu
 */
class DataStorageManager(uniquePredicatesList: List[String])(@transient val sparkSession: SparkSession) extends Serializable {

  import sparkSession.sqlContext.implicits._
  val columns = Seq("sub", "pred", "obj")
  val u_predicates = sparkSession.sparkContext.broadcast(sparkSession.sparkContext.parallelize(uniquePredicatesList).distinct().collect().toList)

  /*
 * createPTT - creates TripleTable with (s,p,o) where p in on uniquePredicatesList.
 */
  def createPTT() = {
    val ptt_df = TripleReader.loadFromFile(SparkUtils.HDFSPath, sparkSession)
      .filter {
        case f: Triples => u_predicates.value.contains(f.pred.toString())
      }
      .map(f => (f.subj.toString(), f.pred.toString(), f.obj.toString()))
      // .filter(f=>f.pred.getLiteralLexicalForm in uniquePredicatesList.map(_)))
      .toDF(columns: _*);

    ptt_df.createOrReplaceTempView("Triples")
    sparkSession.catalog.cacheTable("Triples")
  }

  def createTT() = {
    val ptt_df = TripleReader.loadFromFile(SparkUtils.HDFSPath, sparkSession)
      .filter {
        case f: Triples => u_predicates.value.contains(f.pred.toString())
      }
      .map(f => (f.subj.toString(), f.pred.toString(), f.obj.toString()))
      .toDF(columns: _*)
    ptt_df.createOrReplaceTempView("TriplesTable")
    sparkSession.catalog.cacheTable("TriplesTable")
  }

  def transpose(df: DataFrame, compositeId: List[String], key: String, value: String) = {
     val distinctCols = df.select(key).distinct.map { r => r(0).toString().substring(r(0).toString().lastIndexOf("/") + 1) }.collect().toList

    sparkSession.sparkContext.parallelize(compositeId)
    type Encoder =(List[Any], scala.collection.mutable.Map[Any, Any])
    
    implicit val reverseKryoEncoder = Encoders.kryo[Encoder]

    val rdd = df.map { row =>
      (compositeId.collect { case id => row.getAs(id).asInstanceOf[Any] },
        scala.collection.mutable.Map(row.getAs(key).asInstanceOf[Any] -> row.getAs(value).asInstanceOf[Any]))
    }.rdd
    
    val pairRdd = rdd.reduceByKey(_ ++ _)
    val rowRdd = pairRdd.map(r => dynamicRow(r, distinctCols))
    sparkSession.createDataFrame(rowRdd, getSchema(df.schema, compositeId, (key, distinctCols)))

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
    var _tt = sparkSession.sql("select * "
      + "from TriplesTable")
    _tt.show()

    val dfOutput = transpose(_tt, List("sub"), "pred", "obj")
    dfOutput.registerTempTable("PropertyTable")
    sparkSession.catalog.cacheTable("PropertyTable")

    dfOutput.show
    dfOutput.printSchema()

    /*
    for (predicate <- uniquePredicatesList) {
      var p_Table = sql_c.sql("select sub, obj "
        + "from triples where pred='" + predicate + "'")
    }*/

  }

  object DataStorageManager {
    def apply(uniquePredicatesList: List[String])(sparkSession: SparkSession) = new DataStorageManager(uniquePredicatesList)(sparkSession)
  }
}