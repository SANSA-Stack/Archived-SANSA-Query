package net.sansa_stack.query.flink.ontop

import it.unibz.inf.ontop.model.`type`.{RDFDatatype, TypeFactory}
import it.unibz.inf.ontop.model.vocabulary.XSD
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.types.DataType


/**
 * Mapping from Ontop RDF datatype to Flink SQL datatype
 *
 * @author Lorenz Buehmann
 */
object DatatypeMappings {
  def apply(typeFactory: TypeFactory): Map[RDFDatatype, DataType] = Map(
    typeFactory.getXsdStringDatatype -> DataTypes.STRING(),
    typeFactory.getXsdIntegerDatatype -> DataTypes.INT(),
    typeFactory.getXsdDecimalDatatype -> DataTypes.DECIMAL(10, 0),
    typeFactory.getXsdDoubleDatatype -> DataTypes.DOUBLE(),
    typeFactory.getXsdBooleanDatatype -> DataTypes.BOOLEAN(),
    typeFactory.getXsdFloatDatatype -> DataTypes.FLOAT(),
    typeFactory.getDatatype(XSD.SHORT) -> DataTypes.SMALLINT(),
    typeFactory.getDatatype(XSD.DATE) -> DataTypes.DATE(),
    typeFactory.getDatatype(XSD.BYTE) -> DataTypes.BYTES(),
    typeFactory.getDatatype(XSD.LONG) -> DataTypes.BIGINT()
  )
}
