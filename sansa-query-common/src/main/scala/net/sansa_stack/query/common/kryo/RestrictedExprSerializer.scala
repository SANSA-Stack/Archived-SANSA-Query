package net.sansa_stack.query.common.kryo

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.aksw.jena_sparql_api.views.RestrictedExpr
import org.apache.jena.sparql.expr.Expr

class RestrictedExprSerializer extends Serializer[RestrictedExpr] {
  override def write(kryo: Kryo, output: Output, obj: RestrictedExpr): Unit =
    kryo.writeClassAndObject(output, obj.getExpr)

  override def read(kryo: Kryo, input: Input, aClass: Class[RestrictedExpr]): RestrictedExpr = {
    val expr = kryo.readClassAndObject(input).asInstanceOf[Expr]
    new RestrictedExpr(expr);
  }
}
