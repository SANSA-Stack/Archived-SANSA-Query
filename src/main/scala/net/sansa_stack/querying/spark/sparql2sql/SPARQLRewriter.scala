package net.sansa_stack.querying.spark.sparql2sql

import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.QueryParserFactory;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParserFactory
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery
import org.eclipse.rdf4j.queryrender.sparql.SPARQLQueryRenderer

object SPARQLRewriter {

  def rewrite(selectQuery: String) = {
    val fac = new SPARQLParserFactory()
    val parser = fac.getParser()
    val query = parser.parseQuery(selectQuery, null)
    println(query)

    val renderer = new SPARQLQueryRenderer()
    val renderedQuery = renderer.render(query)
    renderedQuery
  }

}