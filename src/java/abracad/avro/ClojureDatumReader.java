package abracad.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;

import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

public class ClojureDatumReader<T> extends GenericDatumReader<T> {

private static final String NS = "abracad.avro";

static {
    RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
}

private static final Var readRecord = RT.var(NS, "read-record");
private static final Var readEnum = RT.var(NS, "read-enum");
private static final Var readArray = RT.var(NS, "read-array");
private static final Var readMap = RT.var(NS, "read-map");
private static final Var readFixed = RT.var(NS, "read-fixed");
private static final Var readBytes = RT.var(NS, "read-fixed");

public
ClojureDatumReader() {
    this(null, null, ClojureData.get());
}

public
ClojureDatumReader(Schema schema) {
    this(schema, schema, ClojureData.get());
}

public
ClojureDatumReader(Schema writer, Schema reader) {
    this(writer, reader, ClojureData.get());
}

public
ClojureDatumReader(Schema writer, Schema reader, ClojureData data) {
    super(writer, reader, data);
}

@Override
public Object
read(Object old, Schema expected, ResolvingDecoder in)
        throws IOException {
    return super.read(old, expected, in);
}

@Override
protected Object
readRecord(Object old, Schema expected, ResolvingDecoder in)
        throws IOException {
    return readRecord.invoke(this, expected, in);
}

@Override
protected Object
readEnum(Schema expected, Decoder in) throws IOException {
    return readEnum.invoke(this, expected, in);
}

@Override
protected Object
readArray(Object old, Schema expected, ResolvingDecoder in)
        throws IOException {
    return readArray.invoke(this, expected, in);
}

@Override
protected Object
readMap(Object old, Schema expected, ResolvingDecoder in)
        throws IOException {
    return readMap.invoke(this, expected, in);
}

@Override
protected Object
readString(Object old, Schema expected, Decoder in)
        throws IOException {
    return in.readString();
}


@Override
protected Object
readFixed(Object old, Schema expected, Decoder in)
        throws IOException {
    return readFixed.invoke(this, expected, in);
}

@Override
protected Object
readBytes(Object old, Schema expected, Decoder in)
        throws IOException {
    return readBytes.invoke(this, expected, in);
}

}
