// package
package net.sansa_stack.query.spark.semantic

// imports
import scala.collection.mutable.ArrayBuffer
import java.util.Scanner
import java.io.File
import java.util.concurrent.TimeUnit
import java.io._
import com.google.common.collect.ArrayListMultimap
import scala.collection.JavaConversions._
import java.util.StringTokenizer
import org.apache.spark.rdd._

/*
 * QuerySystem - query on semantic partition data
 *
 * @symbol - list of symbols.
 * @partitionData - a RDD of n-triples (formatted).
 * @queryInputPath - query file path.
 * @queryResultPath - path for output result.
 * @numOfFilesPartition - total number of files to save the partition data.
 */
class QuerySystem(
                     symbol: Map[String, String],
                     partitionData: RDD[String],
                     queryInputPath: String,
                     queryResultPath: String,
                     numOfFilesPartition: Int
                 ) extends Serializable {
    var _selectVariables: ArrayBuffer[ArrayBuffer[String]] = ArrayBuffer()
    var _whereVariables: ArrayBuffer[ArrayBuffer[String]] = ArrayBuffer()
    var _WhereTriples: ArrayBuffer[ArrayBuffer[String]] = ArrayBuffer()
    var _numOfWhereClauseTriples: ArrayBuffer[Int] = ArrayBuffer()
    var _queriesLimit: Map[Int, Int] = Map()
    var _queriesProcessTime: ArrayBuffer[Long] = ArrayBuffer()

    var outputRDD: RDD[(String, List[String])] = _
    var workingTripleRDD: RDD[(String, List[String])] = _
    var workingPartialRDD: RDD[(String, List[String])] = _

    def run(): Unit = {
        // parse queries
        for (qID <- this.fetchQueries.indices) {
            // parse query
            this.queryParser(this.fetchQueries(qID), qID)

            // start process time
            val startTime = System.nanoTime()

            // query engine
            this.queryEngine(qID)

            // end process time
            this.queryTime(System.nanoTime() - startTime)
        }

        // overall process time
        this.overallQueriesTime()
    }

    /********************************
    * Parse Queries & Store Variables
    * *******************************/

    // fetch queries from input file
    def fetchQueries: ArrayBuffer[ArrayBuffer[String]] = {
        val file = new File(queryInputPath)
        val fileScanner = new Scanner(file)
        var queryList: ArrayBuffer[ArrayBuffer[String]] = ArrayBuffer()

        // scan lines
        while (fileScanner.hasNext) {
            var line = fileScanner.nextLine.trim()

            // ignore empty lines
            if (line.nonEmpty) {
                // query should start with SELECT
                if (line.toUpperCase.startsWith("SELECT")) {
                    var singleQuery: ArrayBuffer[String] = ArrayBuffer()
                    singleQuery += line

                    // add elements until "}" found
                    while (fileScanner.hasNext) {
                        line = fileScanner.nextLine.trim()

                        // ignore empty lines
                        if (line.nonEmpty) {
                            // if reach at the end
                            if (!line.toUpperCase.startsWith("SELECT")) {
                                singleQuery += line
                            } else {
                                // append query to the list
                                queryList += singleQuery

                                // re-initialize query
                                singleQuery = ArrayBuffer()

                                // next query SELECT
                                singleQuery += line
                            }
                        }
                    }

                    // append query to the list
                    queryList += singleQuery
                }
            }
        }

        queryList
    }

    // parse queries
    def queryParser(query: ArrayBuffer[String], qID: Int): Unit = {
        var selectLine: String = ""
        var whereLines: ArrayBuffer[String] = ArrayBuffer()
        var queryLimit: Int = 0
        var isEnd = false

        // validate query and fetch SELECT and WHERE clauses
        for (i <- 0 to query.length if !isEnd) {
            var j = i
            var line = query(i)

            // clause: SELECT
            if (line.toUpperCase.startsWith("SELECT")) {
                // exception: more than one SELECT line
                if (selectLine.nonEmpty) {
                    throw new IllegalStateException("Multiple SELECT lines detected: " + line)
                }

                selectLine = line
            } else {
                // clause: WHERE
                if (line.toUpperCase.startsWith("WHERE")) {
                    whereLines += line

                    // store all WHERE lines
                    while (!line.endsWith(this.symbol("bracket-right"))) {
                        j += 1
                        line = query(j)

                        // exception: more than one WHERE line
                        if (line.toUpperCase.startsWith("WHERE")) {
                            throw new IllegalStateException("Multiple WHERE lines detected: " + line)
                        }

                        whereLines += line
                    }

                    // check for other operators
                    for(k <- j+1 until query.size) {
                        line = query(k)

                        // store LIMIT value
                        if (line.toUpperCase.startsWith("LIMIT")) {
                            val locationPoint = line.lastIndexOf(this.symbol("blank")) // split line at location: LIMIT
                            val newLine = line.substring(locationPoint) // skip: LIMIT
                            queryLimit = newLine.substring(1, newLine.length).toInt
                            _queriesLimit = Map(qID -> queryLimit)
                        }

                        // increment
                        j += 1
                    }

                    isEnd = true
                } else {
                    throw new IllegalStateException("WHERE Clause not found!")
                }
            }
        }

        // set values to variables
        val selectVariables = this.lineParser(selectLine).filter(_.nonEmpty)
        val whereVariables = this.fetchWhereVariables(whereLines).filter(_.nonEmpty)
        val WhereTriples = this.fetchWhereTriples(whereLines, whereVariables)

        // append variables: SELECT clause
        _selectVariables.append(selectVariables)

        // append variables: WHERE clause
        _whereVariables.append(whereVariables)

        // validate SELECT clause variables
        this.validateSelectVariables(selectVariables, qID)

        // append WHERE triples
        _WhereTriples.append(WhereTriples)

        // append number of clause in a query
        _numOfWhereClauseTriples.append(WhereTriples.size)
    }

    // parse line and store SELECT and WHERE clause variables
    def lineParser(lineParse: String): ArrayBuffer[String] = {
        var line: String = lineParse
        var varList: ArrayBuffer[String] = ArrayBuffer()

        // split line at location: ?
        var locationPoint = line.indexOf(this.symbol("question-mark"))

        // one or more SELECT variables
        while (locationPoint >= 0) {
            // skip: SELECT (left with all variables)
            line = line.substring(locationPoint)

            // split line at location: blank space
            locationPoint = line.indexOf(this.symbol("blank"))

            // when there is no more variables found after split
            if (locationPoint == -1) {
                // set location point to end of line
                locationPoint = line.length()
            }

            // set value to a variable
            var variable = line.substring(0, locationPoint)

            // validate variable (WHERE clause)
            while (variable.endsWith(this.symbol("bracket-right"))) {
                variable = variable.substring(0, variable.length() - 1)
            }

            // add variable to the list
            varList += variable

            // set next location point
            line = line.substring(locationPoint)
            locationPoint = line.indexOf(this.symbol("question-mark"))
        }

        // filter out duplicates
        varList = this.removeDuplicates(varList)

        varList
    }

    // fetch variables: WHERE clause
    def fetchWhereVariables(linesParse: ArrayBuffer[String]): ArrayBuffer[String] = {
        var varList: ArrayBuffer[String] = ArrayBuffer()

        // parse lines
        linesParse.foreach({ line =>
            val variableList = this.lineParser(line)

            // append variables to the list
            variableList.foreach(variable => {
                varList.append(variable)
            })
        })

        // filter out duplicates
        varList = this.removeDuplicates(varList)

        varList
    }

    // fetch triples: WHERE clause
    def fetchWhereTriples(whereLines: ArrayBuffer[String], whereVariables: ArrayBuffer[String]): ArrayBuffer[String] = {
        var varList: ArrayBuffer[String] = ArrayBuffer()

        // iterate WHERE lines
        whereLines.foreach(whereLine => {
            if (!whereLine.isEmpty) {
                var line = whereLine

                // remove WHERE
                if (line.toUpperCase.startsWith("WHERE")) {
                    line = line.substring(5).trim()
                }

                // remove "{"
                if (line.startsWith(this.symbol("bracket-left"))) {
                    line = line.substring(1).trim()
                }

                // remove "}"
                if (line.startsWith(this.symbol("bracket-right"))) {
                    line = line.substring(1).trim()
                }

                // left with triples
                if (line.length() > 0) {
                    // remove spaces
                    line = line.substring(0, line.length()).trim()

                    // remove "." at the end of triple line
                    if (line.substring(line.length() - 1).contains(this.symbol("dot"))) {
                        line = line.substring(0, line.length() - 1).trim()
                    }

                    // append triple to the list
                    varList += line
                }
            }
        })

        // filter out duplicates
        varList = this.removeDuplicates(varList)

        varList
    }

    // remove duplicates
    def removeDuplicates(list: ArrayBuffer[String]): ArrayBuffer[String] = {
        var varList: ArrayBuffer[String] = ArrayBuffer()

        // filter out duplicates from the list
        list.foreach(item => {
            if (!varList.contains(item)) {
                varList += item
            }
        })

        varList
    }

    // validate SELECT clause variables
    def validateSelectVariables(list: ArrayBuffer[String], qID: Int): Unit = {
        // SELECT clause variables must be in WHERE clause
        list.foreach(variable => {
            if (!_whereVariables(qID).contains(variable)) {
                // exception: SELECT variable is not found in WHERE clause
                throw new IllegalStateException("Query No. " + qID + ": SELECT clause variables must be in WHERE clause: " + variable)
            }
        })
    }

    /****************
    * Process Queries
    * ***************/

    // query engine
    def queryEngine(qID: Int): Unit = {
        println("Query No: " + (qID + 1))

        // validate number of WHERE clause triples
        if (_numOfWhereClauseTriples(qID) == 1) {
            println("No. of WHERE clause Triples: 1")

            // process first triple
            this.runFirstTriple(qID)
        } else {
            println("No. of WHERE clause Triples: " + _numOfWhereClauseTriples(qID))

            // process all triples of a query
            this.runAllTriplesOfQuery(qID)
        }
    }

    /****************************************************
    * Process Query: with only one triple in WHERE clause
    * ***************************************************/

    // process first triple
    def runFirstTriple(qID: Int, clauseNum: Int = 0, isRemainingTriples: Boolean = false, varJoinList: ArrayBuffer[String] = null): Unit = {
        // number of WHERE clause triples
        val numOfWhereClauseTriples = _numOfWhereClauseTriples(qID)

        // fetch WHERE clause
        val triple = _WhereTriples(qID)(clauseNum)

        // fetch SUBJECT, PREDICATE and OBJECT
        val tripleData = this.fetchTripleSPO(triple)
        val tripleSubject = tripleData(0)
        val triplePredicate = tripleData(1)
        val tripleObject = tripleData(2)

        // process partition data
        outputRDD = partitionData
            .flatMap(line => {
                val lineArray = line.split(this.symbol("space"))
                var firstTime: Boolean = true

                val output = for (i <- 1 until (lineArray.length - 1)) yield {
                    var line: String = ""

                    // odd numbers
                    if (i % 2 != 0) {
                        if (lineArray(i) == triplePredicate) {
                            if (firstTime) {
                                firstTime = false
                            }
                            line = lineArray(0) + this.symbol("space") + lineArray(i) + this.symbol("space") + lineArray(i + 1)
                        }
                    }

                    line
                }

                output
            })
            .filter(_.nonEmpty)
            .map(line => {
                val splitPartitionData = line.split(this.symbol("space"))
                val i: Int = 0
                var key: String = ""
                var value: List[String] = List()

                // SUBJECT
                val lineSubject = splitPartitionData(i)

                // check: SUBJECT
                if (tripleSubject.startsWith(this.symbol("question-mark")) || tripleSubject.equals(lineSubject)) {
                    // OBJECT
                    val lineObject = splitPartitionData(i + 2)

                    // check: OBJECT
                    if (tripleObject.startsWith(this.symbol("question-mark")) || tripleObject.equals(lineObject)) {
                        if (numOfWhereClauseTriples == 1) {
                            // set output: query with only one WHERE clause triple
                            key = this.setOnlyTripleOutput(
                                qID,
                                tripleSubject,
                                tripleObject,
                                lineSubject,
                                lineObject
                            )

                            value = List(null)
                        } else {
                            if (!isRemainingTriples) {
                                // set output: query with more than one WHERE clause triples
                                key = this.setFirstTripleOutput(
                                    tripleSubject,
                                    tripleObject,
                                    lineSubject,
                                    lineObject
                                )

                                value = List(null)
                            } else {
                                // set output: query with more than one WHERE clause triples
                                val keyValue = this.setRemainingTriplesOutput(
                                    tripleSubject,
                                    tripleObject,
                                    lineSubject,
                                    lineObject,
                                    varJoinList
                                )

                                // assign values
                                for (k <- keyValue.keySet()) {
                                    key = k
                                    value = keyValue.get(k).toList
                                }
                            }
                        }
                    }
                }

                // (K, V) pair
                (key, value)
            })
            .filter(_._1.nonEmpty)

        // only triple query: display output
        if (outputRDD.partitions.nonEmpty && numOfWhereClauseTriples == 1) {
            val resultPath = this.queryResultPath + "/" + qID + "/"

            // check limit
            if (_queriesLimit.get(qID).isDefined) {
                outputRDD
                    .map(key => key._1) // key is the output in case of just one triple query
                    .repartition(this.numOfFilesPartition)
                    .mapPartitions(_.take(_queriesLimit(qID)))
                    .saveAsTextFile(resultPath)
            } else {
                outputRDD
                    .map(key => key._1) // key is the output in case of just one triple query
                    .repartition(this.numOfFilesPartition)
                    .saveAsTextFile(resultPath)
            }
        }

        // multi triples query: set RDD
        if (numOfWhereClauseTriples > 1 && !isRemainingTriples) workingPartialRDD = outputRDD
        if (numOfWhereClauseTriples > 1 && isRemainingTriples) workingTripleRDD = outputRDD
    }

    // set output: query with only one WHERE clause triple
    def setOnlyTripleOutput(qID: Int, tripleSubject: String, tripleObject: String, lineSubject: String, lineObject: String): String = {
        var key: String = ""

        // set output result
        _selectVariables(qID).foreach(selectVariable => {
            // equal variables: SUBJECT and OBJECT with SELECT variables
            if (tripleSubject.equals(selectVariable) && tripleObject.equals(selectVariable)) {
                // equal variables: SUBJECT and OBJECT
                if (tripleSubject.equals(tripleObject)) {
                    val subjectURI = new String(lineSubject.getBytes(), 0, lineSubject.getBytes().length)
                    if (key.nonEmpty) {
                        key += this.symbol("space") + subjectURI
                    } else {
                        key = subjectURI
                    }
                }
            } else {
                // equal variable: SUBJECT with SELECT variables
                if (tripleSubject.equals(selectVariable)) {
                    val subjectURI = new String(lineSubject.getBytes(), 0, lineSubject.getBytes().length)
                    if (key.nonEmpty) {
                        key += this.symbol("space") + subjectURI
                    } else {
                        key = subjectURI
                    }
                } else {
                    // equal variable: OBJECT with SELECT variables
                    if (tripleObject.equals(selectVariable)) {
                        val objectURI = new String(lineObject.getBytes(), 0, lineObject.getBytes().length)
                        if (key.nonEmpty) {
                            key += this.symbol("space") + objectURI
                        } else {
                            key = objectURI
                        }
                    }
                }
            }
        })

        key
    }

    /**********************************************************
    * Process Query: with only multiple triples in WHERE clause
    * *********************************************************/

    // process all triples of a query
    def runAllTriplesOfQuery(qID: Int): Unit = {
        // process first triple
        this.runFirstTriple(qID)

        // iterate the remaining clauses
        for (i <- 1 until _WhereTriples(qID).length) {
            val clauseNum = i

            // fetch variable join list
            val varJoinList: ArrayBuffer[String] = fetchVarJoinList(qID, clauseNum)

            // process remaining triples
            this.runRemainingTriples(qID, clauseNum, varJoinList)
        }

        // display multi triples output
        this.displayMultiTriplesOutput(_selectVariables(qID), qID)
    }

    // set output: query with more than one WHERE clause triples
    def setFirstTripleOutput(tripleSubject: String, tripleObject: String, lineSubject: String, lineObject: String): String = {
        var key: String = ""

        // equal variables: SUBJECT and OBJECT with "?"
        if (tripleSubject.startsWith(this.symbol("question-mark")) && tripleObject.startsWith(this.symbol("question-mark"))) {
            if (tripleSubject.equals(tripleObject)) {
                key = tripleSubject + this.symbol("blank") + lineSubject
            } else {
                key = tripleSubject + this.symbol("blank") + lineSubject + this.symbol("blank") + tripleObject + this.symbol("blank") + lineObject
            }
        } else {
            // equal variable: SUBJECT with "?"
            if (tripleSubject.startsWith(this.symbol("question-mark"))) {
                key = tripleSubject + this.symbol("blank") + lineSubject
            }

            // equal variable: OBJECT with "?"
            if (tripleObject.startsWith(this.symbol("question-mark"))) {
                key = tripleObject + this.symbol("blank") + lineObject
            }
        }

        key
    }

    // fetch variable join list
    def fetchVarJoinList(qID: Int, clauseNum: Int): ArrayBuffer[String] = {
        val foundVarListFromTriples: ArrayBuffer[String] = ArrayBuffer()
        val varJoinList: ArrayBuffer[String] = ArrayBuffer()

        // fetch variables from triples (until processing triple)
        for (i <- 0 until clauseNum) {
            val triple = _WhereTriples(qID)(i)

            // fetch SUBJECT and OBJECT
            val tripleData = this.fetchTripleSPO(triple)
            val tripleSubject = tripleData(0)
            val tripleObject = tripleData(2)

            // SUBJECT
            if (tripleSubject.startsWith(this.symbol("question-mark"))) {
                if (!foundVarListFromTriples.contains(tripleSubject)) {
                    foundVarListFromTriples.append(tripleSubject)
                }
            }

            // OBJECT
            if (tripleObject.startsWith(this.symbol("question-mark"))) {
                if (!foundVarListFromTriples.contains(tripleObject)) {
                    foundVarListFromTriples.append(tripleObject)
                }
            }
        }

        // current triple
        val triple = _WhereTriples(qID)(clauseNum)

        // fetch SUBJECT and OBJECT
        val tripleData = this.fetchTripleSPO(triple)
        val tripleSubject = tripleData(0)
        val tripleObject = tripleData(2)

        // SUBJECT
        if (tripleSubject.startsWith(this.symbol("question-mark"))) {
            if (foundVarListFromTriples.contains(tripleSubject)) {
                varJoinList.append(tripleSubject)
            }
        }

        // OBJECT
        if (tripleObject.startsWith(this.symbol("question-mark"))) {
            if (foundVarListFromTriples.contains(tripleObject)) {
                if (!varJoinList.contains(tripleObject)) {
                    varJoinList.append(tripleObject)
                }
            }
        }

        varJoinList
    }

    // process remaining triples
    def runRemainingTriples(qID: Int, clauseNum: Int, varJoinList: ArrayBuffer[String]): Unit = {
        // process remaining triple
        this.runFirstTriple(qID, clauseNum, isRemainingTriples = true, varJoinList)

        // set output: work on processed triples
        this.setRemainingPartialOutput(varJoinList)

        // join two RDD
        val tempRDD: RDD[(String, (List[String], List[String]))] = workingTripleRDD.join(workingPartialRDD)

        // iterate output keys
        workingPartialRDD = tempRDD
            .flatMap(line => {
                val key2 = line._1
                val valuesList = line._2._1 ::: line._2._2 // concatenate two Lists
                val triplesList: ArrayBuffer[String] = ArrayBuffer()
                val partialsList: ArrayBuffer[String] = ArrayBuffer()

                // iterate key values
                valuesList.foreach(value => {
                    val str = value.substring(0, 1)

                    // check triple
                    if (str.equals("t")) {
                        triplesList.append(value)
                    } else {
                        partialsList.append(value)
                    }
                })

                // set output
                val output = for {
                    i <- triplesList.indices
                    j <- partialsList.indices
                } yield {
                    val key1 = partialsList(j).substring(1) + this.symbol("blank") + triplesList(i).substring(1)
                    val key = key1 + this.symbol("blank") + key2

                    // (K, V) pair
                    (key, null)
                }
                output
            })
    }

    // set output: work on processed triples
    def setRemainingPartialOutput(varJoinList: ArrayBuffer[String]): Unit = {
        workingPartialRDD = workingPartialRDD
            .map(line => {
                var key: String = ""
                var value: String = "p"
                val processLine = line._1
                val itr = new StringTokenizer(processLine)

                while (itr.hasMoreTokens) {
                    val variable = itr.nextToken

                    // check variable in variable join list
                    if (varJoinList.contains(variable)) {
                        if (key.length() != 0) {
                            key = key + this.symbol("blank")
                        }

                        key = key + variable
                        key = key + this.symbol("blank")
                        key = key + itr.nextToken
                    } else {
                        if (value.length() != 0) {
                            value = value + this.symbol("blank")
                        }

                        value = value + this.symbol("blank") + variable
                        value = value + this.symbol("blank")
                        value = value + itr.nextToken
                    }
                }

                // (K, V) pair
                (key, List(value))
            })
    }

    // set output: query with more than one WHERE clause triples
    def setRemainingTriplesOutput(tripleSubject: String, tripleObject: String, lineSubject: String, lineObject: String, varJoinList: ArrayBuffer[String]): ArrayListMultimap[String, String] = {
        val keyValue: ArrayListMultimap[String, String] = ArrayListMultimap.create[String, String]()

        // equal variables: SUBJECT and OBJECT with "?"
        if (tripleSubject.startsWith(this.symbol("question-mark")) && tripleObject.startsWith(this.symbol("question-mark"))) {
            if (tripleSubject.equals(tripleObject)) {
                keyValue.put(tripleSubject + this.symbol("blank") + lineSubject, "t")
            } else {
                if (varJoinList.size > 1) {
                    keyValue.put(tripleSubject + this.symbol("blank") + lineSubject + this.symbol("blank") + tripleObject + this.symbol("blank") + lineObject, "t")
                } else {
                    if (varJoinList(0).equals(tripleSubject)) {
                        keyValue.put(tripleSubject + this.symbol("blank") + lineSubject, "t " + tripleObject + this.symbol("blank") + lineObject)
                    } else {
                        keyValue.put(tripleObject + this.symbol("blank") + lineObject, "t " + tripleSubject + this.symbol("blank") + lineSubject)
                    }
                }
            }
        } else {
            // equal variable: SUBJECT with "?"
            if (tripleSubject.startsWith(this.symbol("question-mark"))) {
                keyValue.put(tripleSubject + this.symbol("blank") + lineSubject, "t")
            }

            // equal variable: OBJECT with "?"
            if (tripleObject.startsWith(this.symbol("question-mark"))) {
                keyValue.put(tripleObject + this.symbol("blank") + lineObject, "t")
            }
        }

        keyValue
    }

    // display multi triples output
    def displayMultiTriplesOutput(selectVariables: ArrayBuffer[String], qID: Int): Unit = {
        val tmpRDD = workingPartialRDD.map(line => {
            val processLine = line._1
            val itr = new StringTokenizer(processLine)
            var outputResult: String = ""

            // check tokens
            while (itr.hasMoreTokens) {
                val variable = itr.nextToken

                // if variable exists in SELECT variables
                if (selectVariables.contains(variable)) {
                    val next = itr.nextToken()
                    if (outputResult.length() > 0) {
                        outputResult = outputResult.concat(this.symbol("space"))
                    }
                    outputResult = outputResult.concat(next)
                } else {
                    itr.nextToken()
                }
            }

            outputResult
        })

        // output result to file
        if (tmpRDD.partitions.nonEmpty) {
            val resultPath = this.queryResultPath + "/" + qID + "/"

            // check limit
            if (_queriesLimit.get(qID).isDefined) {
                tmpRDD
                    .repartition(this.numOfFilesPartition)
                    .mapPartitions(_.take(_queriesLimit(qID)))
                    .saveAsTextFile(resultPath)
            } else {
                tmpRDD
                    .repartition(this.numOfFilesPartition)
                    .saveAsTextFile(resultPath)
            }
        }
    }

    /********
    * Helpers
    * *******/

    // fetch SUBJECT, PREDICATE and OBJECT
    def fetchTripleSPO(triple: String): ArrayBuffer[String] = {
        // return list
        val tripleData: ArrayBuffer[String] = ArrayBuffer()

        // fetch indices
        val locationPoint1 = triple.indexOf(this.symbol("blank"))
        val locationPoint2 = triple.lastIndexOf(this.symbol("blank"))

        // WHERE clause: SUBJECT, PREDICATE and OBJECT
        val tripleSubject = triple.substring(0, locationPoint1).trim()
        val triplePredicate = triple.substring(locationPoint1, locationPoint2).trim()
        val tripleObject = triple.substring(locationPoint2, triple.length()).trim()

        // append data
        tripleData.append(tripleSubject)
        tripleData.append(triplePredicate)
        tripleData.append(tripleObject)

        tripleData
    }

    // total query process time
    def queryTime(processedTime: Long): Unit = {
        val milliseconds = TimeUnit.MILLISECONDS.convert(processedTime, TimeUnit.NANOSECONDS)
        val seconds = Math.floor(milliseconds/1000d + .5d).toInt
        val minutes = TimeUnit.MINUTES.convert(processedTime, TimeUnit.NANOSECONDS)

        if (milliseconds >= 0) {
            println("Processed Time (MILLISECONDS): " + milliseconds)

            if (seconds > 0) {
                println("Processed Time (SECONDS): " + seconds + " approx.")

                if (minutes > 0) {
                    println("Processed Time (MINUTES): " + minutes)
                }
            }
        }

        // append query time
        _queriesProcessTime.append(milliseconds)

        println(this.symbol("newline"))
    }

    // overall queries process time
    def overallQueriesTime(): Unit = {
        val milliseconds: Long = _queriesProcessTime.sum
        val seconds = Math.floor(milliseconds/1000d + .5d).toInt

        if (milliseconds >= 1000) {
            println("--> Overall Process Time: " + milliseconds + "ms (" + seconds + "secs approx.)")
        } else {
            println("--> Overall Process Time: " + milliseconds + "ms")
        }
    }
}
