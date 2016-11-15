package net.sansa_stack.querying.spark.io

/*import org.openjena.riot.RiotReader
import org.openjena.riot.Lang*/
import java.io.InputStream
import org.apache.spark.rdd.RDD
import org.apache.jena.riot.{ Lang, RDFDataMgr }
import net.sansa_stack.querying.spark.utils.Logging
import net.sansa_stack.querying.spark.model.Triples
import org.apache.spark.sql.SparkSession

/**
 * Reads triples.
 *
 * @author Gezim Sejdiu
 *
 */
object TripleReader extends Logging {

  def parseTriples(fn: String) = {
    val triples = RDFDataMgr.createIteratorTriples(new StringInputStream(fn), Lang.NTRIPLES, "http://example/base").next

    Triples(triples.getSubject(), triples.getPredicate(), triples.getObject())
  }

  def loadFromFile(path: String, sparkSession: SparkSession, minPartitions: Int = 2): RDD[Triples] = {

    val triples =
      sparkSession.sparkContext.textFile(path)
        .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
        .map(parseTriples)
    triples
  }
  
}

class StringInputStream(s: String) extends InputStream {
  private val bytes = s.getBytes
  private var pos = 0

  override def read(): Int = if (pos >= bytes.length) {
    -1
  } else {
    val r = bytes(pos)
    pos += 1
    r.toInt
  }
}
