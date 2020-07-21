package net.sansa_stack.query.common.ontop

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import org.apache.jena.graph.NodeFactory

import net.sansa_stack.rdf.common.partition.core.RdfPartitionComplex

/**
 * Some utilities for working with SQL objects.
 *
 * @author Lorenz Buehmann
 */
object SQLUtils {

  def escapeTablename(name: String, quoted: Boolean = true, quotChar: Char = '"'): String = {
    val s = URLEncoder.encode(name, StandardCharsets.UTF_8.toString)
      .toLowerCase
      .replace('%', 'P')
      .replace('.', 'C')
      .replace("-", "dash")
    if (quoted) s"""$quotChar$s$quotChar""" else s
  }


  def createTableName(p: RdfPartitionComplex, blankNodeStrategy: BlankNodeStrategy.Value): String = {
    val pred = p.predicate

    // For now let's just use the full predicate as the uri
    // val predPart = pred.substring(pred.lastIndexOf("/") + 1)
    val predPart = pred
    val pn = NodeFactory.createURI(p.predicate)

    val dt = p.datatype
    val dtPart = if (dt != null && !dt.isEmpty) "_" + dt.substring(dt.lastIndexOf("/") + 1) else ""
    val langPart = if (p.langTagPresent) "_lang" else ""
    val blankPart = if (blankNodeStrategy == BlankNodeStrategy.Table) {
      var tmp = ""
      if (p.subjectType == 0) tmp += "_s_blank"
      if (p.objectType == 0) tmp += "_o_blank"
      tmp
    } else ""

    val tableName = predPart + dtPart + langPart + blankPart// .replace("#", "__").replace("-", "_")

    tableName
  }


}
