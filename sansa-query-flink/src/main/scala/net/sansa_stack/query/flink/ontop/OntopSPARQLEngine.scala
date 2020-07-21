package net.sansa_stack.query.flink.ontop

import java.sql.{Connection, DriverManager, SQLException}
import java.{lang, util}
import java.util.Properties

import com.google.common.collect.{ImmutableMap, ImmutableSortedSet, Sets}
import it.unibz.inf.ontop.answering.reformulation.input.SPARQLQuery
import it.unibz.inf.ontop.answering.resultset.OBDAResultSet
import it.unibz.inf.ontop.exception.{OBDASpecificationException, OntopReformulationException}
import it.unibz.inf.ontop.iq.exception.EmptyQueryException
import it.unibz.inf.ontop.iq.node.ConstructionNode
import it.unibz.inf.ontop.model.`type`.{DBTermType, TypeFactory}
import it.unibz.inf.ontop.model.atom.DistinctVariableOnlyDataAtom
import it.unibz.inf.ontop.model.term._
import it.unibz.inf.ontop.substitution.{ImmutableSubstitution, SubstitutionFactory}

import net.sansa_stack.rdf.common.partition.core.RdfPartitionComplex
import org.apache.jena.graph.Triple
import org.apache.jena.query.{QueryFactory, QueryType}
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.vocabulary.RDF
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{IRI, OWLAxiom, OWLOntology}
import scala.collection.JavaConverters._

import org.apache.flink.api.common.functions.{MapPartitionFunction, RichMapPartitionFunction}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import net.sansa_stack.query.common.ontop.{BlankNodeStrategy, JDBCDatabaseGenerator, OntopMappingGenerator, OntopUtils, SQLUtils}



trait SPARQL2SQLRewriter[T <: QueryRewrite] {
  def createSQLQuery(sparqlQuery: String): T
}

abstract class QueryRewrite(sparqlQuery: String, sqlQuery: String)
/**
 * Wraps the result of query rewriting of Ontop.
 */
case class OntopQueryRewrite(sparqlQuery: String,
                             inputQuery: SPARQLQuery[_ <: OBDAResultSet],
                             sqlQuery: String,
                             sqlSignature: ImmutableSortedSet[Variable],
                             sqlTypeMap: ImmutableMap[Variable, DBTermType],
                             constructionNode: ConstructionNode,
                             answerAtom: DistinctVariableOnlyDataAtom,
                             sparqlVar2Term: ImmutableSubstitution[ImmutableTerm],
                             termFactory: TermFactory,
                             typeFactory: TypeFactory,
                             substitutionFactory: SubstitutionFactory
                            ) extends QueryRewrite(sparqlQuery, sqlQuery) {}

/**
 * A SPARQL to SQL rewriter based on Ontop.
 *
 * RDF partitions will be taken into account to generate Ontop mappings to an in-memory H2 database.
 *
 * @constructor create a new Ontop SPARQL to SQL rewriter based on RDF partitions.
 * @param partitions the RDF partitions
 */
class OntopSPARQL2SQLRewriter(val partitions: Set[RdfPartitionComplex],
                              blankNodeStrategy: BlankNodeStrategy.Value,
                              ontology: Option[OWLOntology] = None)
  extends SPARQL2SQLRewriter[OntopQueryRewrite]
    with Serializable {

  private val logger = com.typesafe.scalalogging.Logger(classOf[OntopSPARQL2SQLRewriter])

  // load Ontop properties
  val ontopProperties = new Properties()
  ontopProperties.load(getClass.getClassLoader.getResourceAsStream("ontop-spark.properties"))

  // create the tmp DB needed for Ontop
  private val JDBC_URL = "jdbc:h2:mem:sansaontopdb;DATABASE_TO_UPPER=FALSE"
  private val JDBC_USER = "sa"
  private val JDBC_PASSWORD = ""

  private lazy val connection: Connection = try {
    Class.forName("org.h2.Driver")
    DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)
  } catch {
    case e: SQLException =>
      logger.error("Error occurred when creating in-memory H2 database", e)
      throw e
  }
  JDBCDatabaseGenerator.generateTables(connection, partitions, blankNodeStrategy)

  // create OBDA mappings
  val mappings = OntopMappingGenerator.createOBDAMappingsForPartitions(partitions, ontology)
  logger.debug(s"Ontop mappings:\n$mappings")

  // the Ontop core
  val reformulationConfiguration = OntopUtils.createReformulationConfig(mappings, ontopProperties, ontology)
  val termFactory = reformulationConfiguration.getTermFactory
  val typeFactory = reformulationConfiguration.getTypeFactory
  val queryReformulator = reformulationConfiguration.loadQueryReformulator
  val substitutionFactory = reformulationConfiguration.getInjector.getInstance(classOf[SubstitutionFactory])
  val inputQueryFactory = queryReformulator.getInputQueryFactory


  @throws[OBDASpecificationException]
  @throws[OntopReformulationException]
  def createSQLQuery(sparqlQuery: String): OntopQueryRewrite = {
    val inputQuery = inputQueryFactory.createSPARQLQuery(sparqlQuery)

    val executableQuery = queryReformulator.reformulateIntoNativeQuery(inputQuery, queryReformulator.getQueryLoggerFactory.create())

    val sqlQuery = OntopUtils.extractSQLQuery(executableQuery)
    val constructionNode = OntopUtils.extractRootConstructionNode(executableQuery)
    val nativeNode = OntopUtils.extractNativeNode(executableQuery)
    val signature = nativeNode.getVariables
    val typeMap = nativeNode.getTypeMap

    OntopQueryRewrite(sparqlQuery, inputQuery, sqlQuery, signature, typeMap, constructionNode,
      executableQuery.getProjectionAtom, constructionNode.getSubstitution, termFactory, typeFactory, substitutionFactory)
  }

  def close(): Unit = connection.close()
}

/**
 * A SPARQL to SQL rewriter based on Ontop.
 *
 * RDF partitions will be taken into account to generate Ontop mappings to an in-memory H2 database.
 *
 */
object OntopSPARQL2SQLRewriter {
  /**
   * Creates a new SPARQL to SQL rewriter based on RDF partitions.
   *
   * @param partitions the RDF partitions
   * @return a new SPARQL to SQL rewriter
   */
  def apply(partitions: Set[RdfPartitionComplex], blankNodeStrategy: BlankNodeStrategy.Value, ontology: Option[OWLOntology])
  : OntopSPARQL2SQLRewriter = new OntopSPARQL2SQLRewriter(partitions, blankNodeStrategy, ontology)

  def apply(partitions: Set[RdfPartitionComplex], blankNodeStrategy: BlankNodeStrategy.Value)
  : OntopSPARQL2SQLRewriter = new OntopSPARQL2SQLRewriter(partitions, blankNodeStrategy)

}

/**
 * A SPARQL engine based on Ontop as SPARQL-to-SQL rewriter.
 *
 * @param env the Flink table environment
 * @param databaseName an existing Spark database that contains the tables for the RDF partitions
 * @param partitions the RDF partitions
 * @param ontology an (optional) ontology that will be used for query optimization and rewriting
 */
class OntopSPARQLEngine(val env: BatchTableEnvironment,
                        val databaseName: String,
                        val partitions: Set[RdfPartitionComplex],
                        var ontology: Option[OWLOntology]) {

  private val logger = com.typesafe.scalalogging.Logger[OntopSPARQLEngine]

  // if no ontology has been provided, we try to extract it from the dataset
  if (ontology.isEmpty) {
    ontology = createOntology()
  }

  val blankNodeStrategy: BlankNodeStrategy.Value = BlankNodeStrategy.Table

  private val sparql2sql = OntopSPARQL2SQLRewriter(partitions, blankNodeStrategy, ontology)

  val typeFactory = sparql2sql.typeFactory

  // mapping from RDF datatype to Spark SQL datatype
  val rdfDatatype2SQLCastName = DatatypeMappings(typeFactory)

  if (databaseName != null && databaseName.trim.nonEmpty) {
    env.useDatabase(databaseName)
  }

  private def createOntology(): Option[OWLOntology] = {
    logger.debug("extracting ontology from dataset")
    // get the partitions that contains the rdf:type triples
    val typePartitions = partitions.filter(_.predicate == RDF.`type`.getURI)

    if (typePartitions.nonEmpty) {
      val names = typePartitions.map(p => SQLUtils.escapeTablename(SQLUtils.createTableName(p, blankNodeStrategy), quotChar = '`'))

      val sql = names.map(name => s"SELECT DISTINCT o FROM $name").mkString(" UNION ")

      val ds = env.sqlQuery(sql).toDataSet[Row]

      val classes = ds.collect().map(_.get(0).toString)

      val dataFactory = OWLManager.getOWLDataFactory
      val axioms: Set[OWLAxiom] = classes.map(cls =>
            dataFactory.getOWLDeclarationAxiom(dataFactory.getOWLClass(IRI.create(cls)))).toSet
      val ontology = OWLManager.createOWLOntologyManager().createOntology(axioms.asJava)

      Some(ontology)
    } else {
      None
    }
  }


  /**
   * Shutdown of the engine, i.e. all open resource will be closed.
   */
  def stop(): Unit = {
    sparql2sql.close()
  }

  /**
   * Free resources, e.g. unregister Spark tables.
   */
  def clear(): Unit = {

  }

  private def postProcess(table: Table, queryRewrite: OntopQueryRewrite): Table = {
    var result = table

    // all projected variables
    val signature = queryRewrite.answerAtom.getArguments

    // mapping from SPARQL variable to term, i.e. to either SQL var or other SPARQL 1.1 bindings (BIND ...)
    val sparqlVar2Term = queryRewrite.constructionNode.getSubstitution

    // we rename the columns of the SQL projected vars
    val columnMappings = signature.asScala
      .map(v => (v, sparqlVar2Term.get(v)))
      .filterNot(_._2.isInstanceOf[RDFConstant]) // skip RDF constants which will be added later
      .map { case (v, term) => (v, term.getVariableStream.findFirst().get()) }
      .toMap
    columnMappings.foreach {
      case (sparqlVar, sqlVar) => result = result.renameColumns($"${sqlVar.getName}" as s"${sparqlVar.getName}")
    }

    // append the lang tags
    // todo other post processing stuff?
//    signature.asScala
//      .map(v => (v, sparqlVar2Term.get(v)))
//      .foreach {case (v, term) =>
//        if (term.isInstanceOf[NonGroundFunctionalTerm]) {
//          if (term.asInstanceOf[NonGroundFunctionalTerm].getFunctionSymbol.isInstanceOf[RDFTermFunctionSymbol]) {
//            val t = term.asInstanceOf[NonGroundFunctionalTerm]
//            if (t.getArity == 2 && t.getTerm(2).asInstanceOf[NonGroundFunctionalTerm].getFunctionSymbol.isInstanceOf[RDFTermTypeFunctionSymbol]) {
//              val map = t.getTerm(2).asInstanceOf[NonGroundFunctionalTerm].getFunctionSymbol.asInstanceOf[RDFTermTypeFunctionSymbol].getConversionMap
// //              map.get(new DBConstantImpl())
//            }
//          }
//        }
//      }


    // and we also add columns for literal bindings which are not already returned by the converted SQL query but
    // are the result of static bindings, e.g. BIND(1 as ?z)
    Sets.difference(new util.HashSet[Variable](signature), columnMappings.keySet.asJava).asScala.foreach(v => {
      val simplifiedTerm = sparqlVar2Term.apply(v).simplify()
      simplifiedTerm match {
        case constant: Constant =>
          if (simplifiedTerm.isInstanceOf[RDFConstant]) { // the only case we cover
            simplifiedTerm match {
              case iri: IRIConstant => // IRI will be String in Spark
                result = result.addOrReplaceColumns(lit(iri.getIRI.getIRIString) as v.getName)
              case l: RDFLiteralConstant => // literals casted to its corresponding type, otherwise String
                val lexicalValue = l.getValue
                val castType = rdfDatatype2SQLCastName.getOrElse(l.getType, DataTypes.STRING())
                result = result.addOrReplaceColumns(lit(lexicalValue).cast(castType) as v.getName)
              case _ =>
            }
          } else {
            if (constant.isNull) {

            }
            if (constant.isInstanceOf[DBConstant]) {
              //                throw new SQLOntopBindingSet.InvalidConstantTypeInResultException(constant + "is a DB constant. But a binding cannot have a DB constant as value")
            }
            //              throw new InvalidConstantTypeInResultException("Unexpected constant type for " + constant);
          }
        case _ =>
        //            throw new SQLOntopBindingSet.InvalidTermAsResultException(simplifiedTerm)
      }
    })

    // and finally, we also have to ensure the original order of the projection vars
    result = result.select(signature.asScala.map(v => v.getName).map(col): _*)

    result
  }

  /**
   * Executes the given SPARQL query on the provided dataset partitions.
   *
   * @param query the SPARQL query
   * @return a DataFrame with the raw result of the SQL query execution and the query rewrite object for
   *         processing the intermediate SQL rows
   *         (None if the SQL query was empty)
   * @throws org.apache.spark.sql.AnalysisException if the query execution fails
   */
  def executeDebug(query: String): (Table, Option[OntopQueryRewrite]) = {
    logger.info(s"SPARQL query:\n$query")

    try {
      // translate to SQL query
      val queryRewrite = sparql2sql.createSQLQuery(query)
      val sql = queryRewrite.sqlQuery.replace("\"", "`")
        .replace("`PUBLIC`.", "")
      logger.info(s"SQL query:\n$sql")

      // execute SQL query
      val resultRaw = env.sqlQuery(sql)
      //    result.show(false)
      //    result.printSchema()

      (resultRaw, Some(queryRewrite))
    } catch {
      case e: EmptyQueryException =>
        logger.warn(s"Empty SQL query generated by Ontop. Returning empty DataFrame for SPARQL query\n$query")
        (spark.emptyDataFrame, None)
      case e: org.apache.spark.sql.AnalysisException =>
        logger.error(s"Spark failed to execute translated SQL query\n$query", e)
        throw e
      case e: Exception => throw e
    }
  }

  /**
   * Executes the given SPARQL query on the provided dataset partitions.
   *
   * @param query the SPARQL query
   * @return a DataFrame with the resulting bindings as columns
   * @throws org.apache.spark.sql.AnalysisException if the query execution fails
   */
  def execute(query: String): Table = {
    val (resultRaw, queryRewrite) = executeDebug(query)
    var result = resultRaw

    if (queryRewrite.nonEmpty) {
      result = postProcess(resultRaw, queryRewrite.get)
    }

    result
  }

  /**
   * Executes a SELECT query on the provided dataset partitions and returns a DataFrame.
   *
   * @param query the SPARQL query
   * @return an RDD of solution bindings
   * @throws org.apache.spark.sql.AnalysisException if the query execution fails
   */
  def execSelect(query: String): DataSet[Binding] = {
    checkQueryType(query, QueryType.SELECT)

    val df = executeDebug(query)._1

    df.mapPartition(new MapRowsToBindingsFunction(query, sparql2sql.mappings,sparql2sql.ontopProperties, partitions, ontology))
  }

  /**
   * Executes an ASK query on the provided dataset partitions.
   *
   * @param query the SPARQL query
   * @return `true` or `false` depending on the result of the ASK query execution
   * @throws org.apache.spark.sql.AnalysisException if the query execution fails
   */
  def execAsk(query: String): Boolean = {
    checkQueryType(query, QueryType.ASK)
    val df = executeDebug(query)._1
    !df.
  }

  /**
   * Executes a CONSTRUCT query on the provided dataset partitions.
   *
   * @param query the SPARQL query
   * @return a DataSet of triples
   * @throws org.apache.spark.sql.AnalysisException if the query execution fails
   */
  def execConstruct(query: String): DataSet[org.apache.jena.graph.Triple] = {
    checkQueryType(query, QueryType.CONSTRUCT)

    val ds = executeDebug(query)._1

    ds.mapPartition(new MapRowsToTriplesFunction(query, sparql2sql.mappings,sparql2sql.ontopProperties, partitions, ontology))
  }

  private def checkQueryType(query: String, queryType: QueryType) = {
    val q = QueryFactory.create(query)
    if (q.queryType() != queryType) throw new RuntimeException(s"Wrong query type. Expected ${queryType.toString} query," +
      s" got ${q.queryType().toString}")
  }


}

class MapRowsToTriplesFunction(query: String,
                               mappings: String,
                               properties: Properties,
                               partitions: Set[RdfPartitionComplex],
                               ontology: Option[OWLOntology])
  extends MapPartitionFunction[Row, Triple] {

  override def mapPartition(iterable: lang.Iterable[Row], collector: Collector[Triple]): Unit = {
    val mapper = new OntopRowMapper(mappings, properties, partitions, query, ontology)
    mapper.toTriples(iterable.asScala).foreach(collector.collect)
    mapper.close()
  }
}

class MapRowsToBindingsFunction(query: String,
                               mappings: String,
                               properties: Properties,
                               partitions: Set[RdfPartitionComplex],
                               ontology: Option[OWLOntology])
  extends MapPartitionFunction[Row, Binding] {

  override def mapPartition(iterable: lang.Iterable[Row], collector: Collector[Binding]): Unit = {
    val mapper = new OntopRowMapper(mappings, properties, partitions, query, ontology)
    iterable.forEach(row => collector.collect(mapper.map(row)))
    mapper.close()
  }
}



object OntopSPARQLEngine {

  def main(args: Array[String]): Unit = {
    new OntopCLI().run(args)
  }

  def apply(env: TableEnvironment, databaseName: String, partitions: Set[RdfPartitionComplex], ontology: Option[OWLOntology]): OntopSPARQLEngine
  = new OntopSPARQLEngine(env, databaseName, partitions, ontology)

  def apply(env: BatchTableEnvironment, partitions: Map[RdfPartitionComplex, DataSet[Product]], ontology: Option[OWLOntology]): OntopSPARQLEngine = {
    // create and register Spark tables
    FlinkTableGenerator(env).createAndRegisterFlinkTables(partitions)

    new OntopSPARQLEngine(env, null, partitions.keySet, ontology)
  }

}
