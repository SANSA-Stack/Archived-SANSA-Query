package net.sansa_stack.querying.spark.model

/*import com.hp.hpl.jena.graph.{ Triple => JTriple }
import com.hp.hpl.jena.graph.{ Node => JNode }*/
import org.apache.jena.graph.{Node=>JNode,Triple=>JTriple, Node_URI, Node_Concrete}
/**
 * A data structure for a set of triples.
 *
 * @author Gezim Sejdiu
 *
 */

case class Triples(subj: JNode, pred: JNode, obj: JNode) extends JTriple(subj, pred, obj) with Serializable {

  def dataType(literal: String): String = {
    val index = literal.indexOf("^^")
    var res = "";
    if (index > -1)
      res = literal.substring(index + 2)
    res
  }

  def languageTag(literal: String): String = {
    val index = literal.indexOf("@")
    var res = "";
    if (index > -1)
      res = literal.substring(index + 1)
    res
  }
}
