package net.sansa_stack.querying.spark.utils

import org.apache.spark.serializer.{ KryoRegistrator => SparkKryoRegistrator }
import com.esotericsoftware.kryo.Kryo
import net.sansa_stack.querying.spark.sparklify.SparkSQLRelation

/*
 * Class for serialization by the Kryo serializer.
 */
class Registrator extends SparkKryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    // model
    kryo.register(classOf[SparkSQLRelation])
    //kryo.register(classOf[TripleRDD])
  }
}