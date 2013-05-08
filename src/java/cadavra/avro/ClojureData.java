package cadavra.avro;

import org.apache.avro.generic.GenericData;

public class ClojureData extends GenericData {

private static final ClojureData INSTANCE = new ClojureData();

protected
ClojureData() {
}

public static
ClojureData get() {
    return INSTANCE;
}

}

