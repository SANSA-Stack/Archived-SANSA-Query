package net.sansa_stack.querying.spark.io

import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.io.File
import net.sansa_stack.querying.spark.utils.Logging
import net.sansa_stack.querying.spark.model.Triples

/**
 * Writes triples to disk.
 *
 * @author Gezim Sejdiu
 *
 */

object TripleWriter extends Logging {

  def writeToFile(triples: RDD[Triples], path: String) = {
    val startTime = System.currentTimeMillis()

    triples
      .map(t => "<" + t.subj + "> <" + t.pred + "> <" + t.obj + "> .")
      .saveAsTextFile(path)
  }

  def writeToFile(triples: RDD[Triples], path: String, partition: Int) = {
    val startTime = System.currentTimeMillis()

    triples
      .map(t => "<" + t.subj + "> <" + t.pred + "> <" + t.obj + "> .")
      .coalesce(partition)
      .saveAsTextFile(path)
  }
}