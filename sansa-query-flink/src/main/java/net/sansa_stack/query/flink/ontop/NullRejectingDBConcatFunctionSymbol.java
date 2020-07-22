package net.sansa_stack.query.flink.ontop;

import com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.model.term.ImmutableTerm;
import it.unibz.inf.ontop.model.term.functionsymbol.db.DBFunctionSymbolSerializer;
import it.unibz.inf.ontop.model.term.functionsymbol.db.impl.AbstractDBConcatFunctionSymbol;
import it.unibz.inf.ontop.model.term.functionsymbol.db.impl.Serializers;
import it.unibz.inf.ontop.model.type.DBTermType;

public class NullRejectingDBConcatFunctionSymbol extends AbstractDBConcatFunctionSymbol {


    protected NullRejectingDBConcatFunctionSymbol(String nameInDialect, int arity, DBTermType dbStringType,
                                                  DBTermType rootDBTermType, boolean isOperator) {
        super(nameInDialect, arity, dbStringType, rootDBTermType,
                isOperator
                        ? Serializers.getOperatorSerializer(nameInDialect)
                        : Serializers.getRegularSerializer(nameInDialect));
    }

    protected NullRejectingDBConcatFunctionSymbol(String nameInDialect, int arity, DBTermType dbStringType,
                                                  DBTermType rootDBTermType, DBFunctionSymbolSerializer serializer) {
        super(nameInDialect, arity, dbStringType, rootDBTermType, serializer);
    }

    @Override
    public boolean isAlwaysInjectiveInTheAbsenceOfNonInjectiveFunctionalTerms() {
        return false;
    }

    /**
     * TODO: allow post-processing
     */
    @Override
    public boolean canBePostProcessed(ImmutableList<? extends ImmutableTerm> arguments) {
        return false;
    }
}