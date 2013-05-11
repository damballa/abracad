package abracad.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

public class ClojureDatumWriter extends GenericDatumWriter<Object> {

private static final String NS = "abracad.avro.write";

static {
    RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
}

private static final Var writeRecord = RT.var(NS, "write-record");
private static final Var writeEnum = RT.var(NS, "write-enum");
private static final Var resolveUnion = RT.var(NS, "resolve-union");
private static final Var writeFixed = RT.var(NS, "write-fixed");

public
ClojureDatumWriter() {
    super();
}

public
ClojureDatumWriter(Schema schema) {
    super(schema);
}

@Override
public void
write(Schema schema, Object datum, Encoder out)
        throws IOException {
    super.write(schema, datum, out);
}

@Override
protected void
writeRecord(Schema schema, Object datum, Encoder out)
        throws IOException {
    writeRecord.invoke(this, schema, datum, out);
}

@Override
protected void
writeEnum(Schema schema, Object datum, Encoder out)
        throws IOException {
    writeEnum.invoke(this, schema, datum, out);
}  

@Override
protected int
resolveUnion(Schema union, Object datum) {
    Object i = resolveUnion.invoke(this, union, datum);
    if (i == null) throw new UnresolvedUnionException(union, datum);
    return RT.intCast(i);
}

@Override
protected void
writeFixed(Schema schema, Object datum, Encoder out)
        throws IOException {
    writeFixed.invoke(this, schema, datum, out);
}

}
