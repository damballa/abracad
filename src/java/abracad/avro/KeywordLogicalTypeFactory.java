package abracad.avro;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;

public class KeywordLogicalTypeFactory implements LogicalTypeFactory {
    public static final String TYPE = "keyword";

    @Override
    public LogicalType fromSchema(Schema schema) {
        if(TYPE.equals(schema.getProp(LogicalType.LOGICAL_TYPE_PROP))) {
            return new KeywordLogicalType();
        }
        return null;
    }

    public static class KeywordLogicalType extends LogicalType {
        public KeywordLogicalType() {
            super(TYPE);
        }
    }
}
