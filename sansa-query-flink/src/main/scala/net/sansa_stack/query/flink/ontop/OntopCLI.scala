package net.sansa_stack.query.flink.ontop

import java.io.File
import java.net.URI
import java.nio.file.Paths

import scala.reflect.ClassTag

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.jena.graph.Triple
import org.apache.jena.query.{QueryFactory, QueryType}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.OWLOntology
import scopt.OParser

import net.sansa_stack.query.common.ontop.PartitionSerDe
import net.sansa_stack.rdf.common.partition.core.{RdfPartition, RdfPartitionComplex, RdfPartitioner, RdfPartitionerComplex}
import net.sansa_stack.rdf.flink.io.ntriples.NTriplesReader
import net.sansa_stack.rdf.flink.partition.core.RdfPartitionUtilsFlink
import org.apache.flink.api.scala._

import net.sansa_stack.rdf.common.partition.core

/**
 * @author Lorenz Buehmann
 */
class OntopCLI {

  val logger = com.typesafe.scalalogging.Logger(classOf[OntopCLI])

  def run(args: Array[String]): Unit = {
    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        run(config)
      case _ =>
      // arguments are bad, error message will have been displayed
    }
  }

  def computePartitions[RdfPartitionComplex: TypeInformation: ClassTag](triplesDS: DataSet[Triple]): Map[core.RdfPartitionComplex, DataSet[Product]] = {
    val partitioner = RdfPartitionerComplex(false)
    val partitions = triplesDS.map(x => partitioner.fromTriple(x)).distinct(_.hashCode()).collect
    val array = partitions.map { p =>
      (
        p,
        triplesDS
          .filter(t => p.matches(t))
          .map(t => p.layout.fromTriple(t))
      )
    }
    Map(array: _*)
  }

  private def run(config: Config): Unit = {

    val fbEnv = ExecutionEnvironment.getExecutionEnvironment
    val env = BatchTableEnvironment.create(fbEnv)

    val sparqlEngine =
      if (config.inputPath != null) {
        // read triples as RDD[Triple]

        val triplesDS = NTriplesReader.load(fbEnv, URI.create(config.inputPath.toString))

        // load optional schema file and filter properties used for VP
        val ont: OWLOntology =
        if (config.schemaPath != null) OWLManager.createOWLOntologyManager().loadOntologyFromOntologyDocument(new File(config.schemaPath))
        else null

        // do partitioning here
        logger.info("computing partitions ...")

//        val partitions: Map[RdfPartitionComplex, DataSet[Product]] =
//          RdfPartitionUtilsFlink.partitionGraph(triplesDS, partitioner = RdfPartitionerComplex(false))
        val partitions = computePartitions(triplesDS)

        logger.info(s"got ${partitions.keySet.size} partitions ...")

        // create the SPARQL engine
        OntopSPARQLEngine(env, partitions, Option(ont))
      } else {
        // load partitioning metadata
        val partitions = PartitionSerDe.deserializeFrom(Paths.get(config.metaDataPath))
        // create the SPARQL engine
        OntopSPARQLEngine(env, config.databaseName, partitions, None)
      }

    var input = config.initialQuery

    def runQuery(query: String): Unit = {
      try {
        val q = QueryFactory.create(query)

        q.queryType() match {
          case QueryType.SELECT =>
            val res = sparqlEngine.execSelect(query)
            println(res.collect().mkString("\n"))
          case QueryType.ASK =>
            val res = sparqlEngine.execAsk(query)
            println(res)
          case QueryType.CONSTRUCT =>
            val res = sparqlEngine.execConstruct(query)
            println(res.collect().mkString("\n"))
          case _ => throw new RuntimeException(s"unsupported query type: ${q.queryType()}")
        }
      } catch {
        case e: Exception => Console.err.println("failed to execute query")
          e.printStackTrace()
      }
    }

    if (input != null) {
      runQuery(input)
    }
    println("enter SPARQL query (press 'q' to quit): ")
    input = scala.io.StdIn.readLine()
    while (input != "q") {
      runQuery(input)
      println("enter SPARQL query (press 'q' to quit): ")
      input = scala.io.StdIn.readLine()
    }

    sparqlEngine.stop()
  }

  case class Config(
                     inputPath: URI = null,
                     metaDataPath: URI = null,
                     schemaPath: URI = null,
                     databaseName: String = null,
                     initialQuery: String = null)
  val builder = OParser.builder[Config]
  import builder._
  val parser = {
    OParser.sequence(
      programName("Ontop SPARQL Engine"),
      head("Ontop SPARQL Engine", "0.1.0"),
      opt[URI]('i', "input")
        .action((x, c) => c.copy(inputPath = x))
        .text("path to input data"),
      opt[URI]('m', "metadata")
        .action((x, c) => c.copy(metaDataPath = x))
        .text("path to partitioning metadata"),
      opt[String]("database")
        .abbr("db")
        .action((x, c) => c.copy(databaseName = x))
        .text("the name of the Spark databases used as KB"),
      opt[URI]('s', "schema")
        .optional()
        .action((x, c) => c.copy(schemaPath = x))
        .text("an optional file containing the OWL schema to process only object and data properties"),
      opt[String]('q', "query")
        .optional()
        .action((x, c) => c.copy(initialQuery = x))
        .text("an initial SPARQL query that will be executed"),
      checkConfig( c =>
        if (c.databaseName != null && c.inputPath != null) failure("either specify path to data or an already created database")
        else success ),
      checkConfig( c =>
        if (c.databaseName != null && c.metaDataPath == null) failure("If database is used the path to the partitioning " +
          "metadata has to be provided as well")
        else success )
    )
  }

}
