package abracad.avro;

import java.io.IOException;

import clojure.lang.Keyword;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

public class ClojureDatumWriter extends GenericDatumWriter<Object> {

    private static class Vars {
        private static final String NS = "abracad.avro.write";

        private static final Var writeRecord = RT.var(NS, "write-record");
        private static final Var writeEnum = RT.var(NS, "write-enum");
        private static final Var writeArray = RT.var(NS, "write-array");
        private static final Var resolveUnion = RT.var(NS, "resolve-union");
        private static final Var writeBytes = RT.var(NS, "write-bytes");
        private static final Var writeFixed = RT.var(NS, "write-fixed");

        static {
            RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
        }
    }

    public ClojureDatumWriter(Schema schema, ClojureData data) {
        super(schema, data);
    }

    @Override
    public void write(Schema schema, Object datum, Encoder out) throws IOException {
        Object datumCast = castDatum(schema, datum);
        super.write(schema, datumCast, out);
    }

    private Object castDatum(Schema schema, Object datum) {
        if (schema.getLogicalType() != null) {
            return datum;
        } else {
            switch (schema.getType()) {
                case INT:
                    return RT.intCast(datum);
                case LONG:
                    return RT.longCast(datum);
                case FLOAT:
                    return RT.floatCast(datum);
                case DOUBLE:
                    return RT.doubleCast(datum);
                case BOOLEAN:
                    return RT.booleanCast(datum);
                default:
                    return datum;
            }
        }
    }

    @Override
    protected void writeRecord(Schema schema, Object datum, Encoder out) {
        Vars.writeRecord.invoke(this, schema, datum, out);
    }

    @Override
    protected void writeEnum(Schema schema, Object datum, Encoder out) {
        Vars.writeEnum.invoke(this, schema, datum, out);
    }

    @Override
    protected void writeArray(Schema schema, Object datum, Encoder out) {
        Vars.writeArray.invoke(this, schema, datum, out);
    }

    @Override
    protected int resolveUnion(Schema union, Object datum) {
        // Logical type cases will be resolved by the underlying clojure implementation
        // TODO maybe move logical type checking into clojure? Java implementation seems to work fine though.
        Object i = Vars.resolveUnion.invoke(this, union, datum);
        if (i == null) {
            return super.resolveUnion(union, datum);
        }
        return RT.intCast(i);
    }

    @Override
    protected void writeBytes(Object datum, Encoder out) {
        Vars.writeBytes.invoke(this, datum, out);
    }

    @Override
    protected void writeFixed(Schema schema, Object datum, Encoder out) {
        Vars.writeFixed.invoke(this, schema, datum, out);
    }

    @Override
    protected void writeString(Schema schema, Object datum, Encoder out) throws IOException {
        if ("keyword".equals(schema.getProp(ClojureData.CLOJURE_TYPE_PROP))) {
            super.writeString(schema, ((Keyword) datum).getName(), out);
        } else {
            super.writeString(schema, datum, out);
        }
    }
}
