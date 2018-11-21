package abracad.avro;

import java.util.Collections;
import java.util.List;

import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

public class ClojureData extends ReflectData {

    public static String CLOJURE_TYPE_PROP = "clojureType";

    private static class Vars {
        private static final String NS = "abracad.avro.compare";
        private static final Var compare = RT.var(NS, "compare");

        static {
            RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
        }
    }

    private static final ClojureData NO_CONVERSIONS = new ClojureData(Collections.emptyList());

    public ClojureData(List<Conversion<?>> conversions) {
        super();
        for (Conversion<?> conversion : conversions) {
            addLogicalTypeConversion(conversion);
        }
    }

    public static ClojureData withNoConversions() {
        return NO_CONVERSIONS;
    }

    @Override
    public DatumReader createDatumReader(Schema schema) {
        return createDatumReader(schema, schema);
    }

    @Override
    public DatumReader createDatumReader(Schema writer, Schema reader) {
        return new ClojureDatumReader(writer, reader, this);
    }

    @Override
    public DatumWriter createDatumWriter(Schema schema) {
        return new ClojureDatumWriter(schema, this);
    }

    @Override
    public int compare(Object o1, Object o2, Schema s, boolean equals) {
        return (int) ((IFn.OOOOL) Vars.compare.get()).invokePrim(o1, o2, s, equals);
    }

    public int _supercompare(Object o1, Object o2, Schema s, boolean equals) {
        return super.compare(o1, o2, s, equals);
    }

}
