package abracad.avro;

import org.apache.avro.*;
import org.apache.avro.generic.GenericFixed;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

public class Java8LogicalTypes {

    private Java8LogicalTypes() {}

    public static class DateConversion extends Conversion<LocalDate> {

        @Override
        public Class<LocalDate> getConvertedType() {
            return LocalDate.class;
        }

        @Override
        public String getLogicalTypeName() {
            return "date";
        }

        @Override
        public LocalDate fromInt(Integer daysFromEpoch, Schema schema, LogicalType type) {
            return LocalDate.ofEpochDay(daysFromEpoch);
        }

        @Override
        public Integer toInt(LocalDate date, Schema schema, LogicalType type) {
            return Math.toIntExact(date.toEpochDay());
        }

        @Override
        public Schema getRecommendedSchema() {
            return LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        }
    }

    public static class TimeMillisConversion extends Conversion<LocalTime> {
        @Override
        public Class<LocalTime> getConvertedType() {
            return LocalTime.class;
        }

        @Override
        public String getLogicalTypeName() {
            return "time-millis";
        }

        @Override
        public LocalTime fromInt(Integer millisFromMidnight, Schema schema, LogicalType type) {
            return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(millisFromMidnight));
        }

        @Override
        public Integer toInt(LocalTime time, Schema schema, LogicalType type) {
            return Math.toIntExact(TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay()));
        }

        @Override
        public Schema getRecommendedSchema() {
            return LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
        }
    }

    public static class TimeMicrosConversion extends Conversion<LocalTime> {
        @Override
        public Class<LocalTime> getConvertedType() {
            return LocalTime.class;
        }

        @Override
        public String getLogicalTypeName() {
            return "time-micros";
        }

        @Override
        public LocalTime fromLong(Long microsFromMidnight, Schema schema, LogicalType type) {
            return LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(microsFromMidnight));
        }

        @Override
        public Long toLong(LocalTime time, Schema schema, LogicalType type) {
            return TimeUnit.NANOSECONDS.toMicros(time.toNanoOfDay());
        }

        @Override
        public Schema getRecommendedSchema() {
            return LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
        }
    }

    public static class TimestampMillisConversion extends Conversion<Instant> {
        @Override
        public Class<Instant> getConvertedType() {
            return Instant.class;
        }

        @Override
        public String getLogicalTypeName() {
            return "timestamp-millis";
        }

        @Override
        public Instant fromLong(Long millisFromEpoch, Schema schema, LogicalType type) {
            return Instant.ofEpochMilli(millisFromEpoch);
        }

        @Override
        public Long toLong(Instant timestamp, Schema schema, LogicalType type) {
            return timestamp.toEpochMilli();
        }

        @Override
        public Schema getRecommendedSchema() {
            return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        }
    }

    public static class TimestampMicrosConversion extends Conversion<Instant> {
        @Override
        public Class<Instant> getConvertedType() {
            return Instant.class;
        }

        @Override
        public String getLogicalTypeName() {
            return "timestamp-micros";
        }

        @Override
        public Instant fromLong(Long microsFromEpoch, Schema schema, LogicalType type) {
            long epochSeconds = microsFromEpoch / (1_000_000);
            long nanoAdjustment = (microsFromEpoch % (1_000_000)) * 1_000;

            return Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
        }

        @Override
        public Long toLong(Instant instant, Schema schema, LogicalType type) {
            long seconds = instant.getEpochSecond();
            int nanos = instant.getNano();

            if (seconds < 0 && nanos > 0) {
                long micros = Math.multiplyExact(seconds + 1, 1_000_000);
                long adjustment = (nanos / 1_000L) - 1_000_000;

                return Math.addExact(micros, adjustment);
            } else {
                long micros = Math.multiplyExact(seconds, 1_000_000);

                return Math.addExact(micros, nanos / 1_000);
            }
        }

        @Override
        public Schema getRecommendedSchema() {
            return LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        }
    }

    public static class RoundingDecimalConversion extends Conversion<BigDecimal> {
        private static final Conversion<BigDecimal> DELEGATE = new Conversions.DecimalConversion();
        private final RoundingMode roundingMode;

        public RoundingDecimalConversion(RoundingMode roundingMode) {
            this.roundingMode = roundingMode;
        }

        @Override
        public Class<BigDecimal> getConvertedType() {
            return DELEGATE.getConvertedType();
        }

        @Override
        public String getLogicalTypeName() {
            return DELEGATE.getLogicalTypeName();
        }

        @Override
        public Schema getRecommendedSchema() {
            return DELEGATE.getRecommendedSchema();
        }

        @Override
        public BigDecimal fromFixed(GenericFixed value, Schema schema, LogicalType type) {
            return DELEGATE.fromFixed(value, schema, type);
        }

        @Override
        public BigDecimal fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
            return DELEGATE.fromBytes(value, schema, type);
        }

        @Override
        public GenericFixed toFixed(BigDecimal value, Schema schema, LogicalType type) {
            int scale = ((LogicalTypes.Decimal) type).getScale();
            return DELEGATE.toFixed(value.setScale(scale, roundingMode), schema, type);
        }

        @Override
        public ByteBuffer toBytes(BigDecimal value, Schema schema, LogicalType type) {
            int scale = ((LogicalTypes.Decimal) type).getScale();
            return DELEGATE.toBytes(value.setScale(scale, roundingMode), schema, type);
        }
    }
}
