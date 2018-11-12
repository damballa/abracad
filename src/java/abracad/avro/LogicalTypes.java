package abracad.avro;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import java.time.Instant;
import java.time.LocalDate;

public class LogicalTypes {
    // TODO avro exposes it's own implementations using Joda time. Should we use those? I prefer java.time.
    // TODO move into clojure land and make converters data driven. Provide default, default-java7 and ways to extend
    public static final ClojureData DEFAULT_LOGICAL_TYPES;
    static {
        ClojureData d = new ClojureData();
        d.addLogicalTypeConversion(new LocalDateConversion());
        d.addLogicalTypeConversion(new InstantConversion());
        d.addLogicalTypeConversion(new Conversions.DecimalConversion());
        d.addLogicalTypeConversion(new Conversions.UUIDConversion());
        DEFAULT_LOGICAL_TYPES = d;
    }

    private static class LocalDateConversion extends Conversion<LocalDate>{

        @Override
        public Class<LocalDate> getConvertedType() {
            return LocalDate.class;
        }

        @Override
        public String getLogicalTypeName() {
            return "date";
        }

        @Override
        public LocalDate fromInt(Integer value, Schema schema, LogicalType type) {
            return LocalDate.ofEpochDay(value);
        }

        @Override
        public Integer toInt(LocalDate value, Schema schema, LogicalType type) {
            return Math.toIntExact(value.toEpochDay());
        }
    }

    private static class InstantConversion extends Conversion<Instant>{

        @Override
        public Class<Instant> getConvertedType() {
            return Instant.class;
        }

        @Override
        public String getLogicalTypeName() {
            return "timestamp-millis";
        }

        @Override
        public Instant fromLong(Long value, Schema schema, LogicalType type) {
            return Instant.ofEpochMilli(value);
        }

        @Override
        public Long toLong(Instant value, Schema schema, LogicalType type) {
            return value.toEpochMilli();
        }
    }
}
