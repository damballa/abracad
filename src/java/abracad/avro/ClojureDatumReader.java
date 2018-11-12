package abracad.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;

import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

public class ClojureDatumReader extends GenericDatumReader<Object> {

private static class Vars {
    private static final String NS = "abracad.avro.read";

    private static final Var readRecord = RT.var(NS, "read-record");
    private static final Var readEnum = RT.var(NS, "read-enum");
    private static final Var readArray = RT.var(NS, "read-array");
    private static final Var readMap = RT.var(NS, "read-map");
    private static final Var readFixed = RT.var(NS, "read-fixed");
    private static final Var readBytes = RT.var(NS, "read-bytes");

    static {
        RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
    }
}


public
ClojureDatumReader() {
    super(null, null);
}

public
ClojureDatumReader(Schema schema) {
    super(schema, schema);
}

public
ClojureDatumReader(Schema writer, Schema reader) {
    super(writer, reader);
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
    return Vars.readRecord.invoke(this, expected, in);
}

@Override
protected Object
readEnum(Schema expected, Decoder in) throws IOException {
    return Vars.readEnum.invoke(this, expected, in);
}

@Override
protected Object
readArray(Object old, Schema expected, ResolvingDecoder in)
        throws IOException {
    return Vars.readArray.invoke(this, expected, in);
}

@Override
protected Object
readMap(Object old, Schema expected, ResolvingDecoder in)
        throws IOException {
    return Vars.readMap.invoke(this, expected, in);
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
    if(expected.getLogicalType() != null) {
        return super.readFixed(old, expected, in);
    }
    return Vars.readFixed.invoke(this, expected, in);
}

@Override
protected Object
readBytes(Object old, Schema expected, Decoder in)
        throws IOException {
    if(expected.getLogicalType() != null) {
        return super.readBytes(old, expected, in);
    }
    return Vars.readBytes.invoke(this, expected, in);
}

}
