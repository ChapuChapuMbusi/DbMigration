package dbMigration.utils;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Base64;

public final class JdbcCsvValueFormatter {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
            .toFormatter();
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
            .toFormatter();

    private JdbcCsvValueFormatter() {
    }

    public static String format(ResultSet rs, ResultSetMetaData meta, int columnIndex) throws SQLException, IOException {
        Object value = rs.getObject(columnIndex);
        if (value == null) {
            return null;
        }

        return switch (meta.getColumnType(columnIndex)) {
            case Types.DATE -> formatDate(value);
            case Types.TIME, Types.TIME_WITH_TIMEZONE -> formatTime(value);
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> formatTimestamp(value);
            case Types.BOOLEAN, Types.BIT -> formatBoolean(value);
            case Types.DECIMAL, Types.NUMERIC -> formatDecimal(value);
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> formatBinary(rs, columnIndex, value);
            case Types.CLOB, Types.NCLOB, Types.LONGVARCHAR, Types.LONGNVARCHAR, Types.NVARCHAR, Types.NCHAR,
                    Types.CHAR, Types.VARCHAR -> formatCharacter(value);
            case Types.SQLXML -> formatSqlXml(value);
            default -> formatFallback(value);
        };
    }

    private static String formatDate(Object value) {
        if (value instanceof LocalDate localDate) {
            return DATE_FORMATTER.format(localDate);
        }
        if (value instanceof java.sql.Date sqlDate) {
            return DATE_FORMATTER.format(sqlDate.toLocalDate());
        }
        return value.toString();
    }

    private static String formatTime(Object value) {
        if (value instanceof OffsetTime offsetTime) {
            return TIME_FORMATTER.format(offsetTime.withOffsetSameInstant(ZoneOffset.UTC).toLocalTime());
        }
        if (value instanceof LocalTime localTime) {
            return TIME_FORMATTER.format(localTime);
        }
        if (value instanceof Time sqlTime) {
            return TIME_FORMATTER.format(sqlTime.toLocalTime());
        }
        return value.toString();
    }

    private static String formatTimestamp(Object value) {
        if (value instanceof OffsetDateTime offsetDateTime) {
            return TIMESTAMP_FORMATTER.format(offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime());
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return TIMESTAMP_FORMATTER.format(zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime());
        }
        if (value instanceof Instant instant) {
            return TIMESTAMP_FORMATTER.format(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
        }
        if (value instanceof LocalDateTime localDateTime) {
            return TIMESTAMP_FORMATTER.format(localDateTime);
        }
        if (value instanceof Timestamp timestamp) {
            return TIMESTAMP_FORMATTER.format(timestamp.toLocalDateTime());
        }
        return value.toString();
    }

    private static String formatBoolean(Object value) {
        if (value instanceof Boolean bool) {
            return bool ? "1" : "0";
        }
        if (value instanceof Number number) {
            return number.intValue() == 0 ? "0" : "1";
        }
        return value.toString();
    }

    private static String formatDecimal(Object value) {
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        return value.toString();
    }

    private static String formatBinary(ResultSet rs, int columnIndex, Object value) throws SQLException {
        byte[] bytes;
        if (value instanceof byte[] data) {
            bytes = data;
        } else if (value instanceof Blob blob) {
            bytes = blob.getBytes(1, (int) blob.length());
        } else {
            bytes = rs.getBytes(columnIndex);
        }
        return bytes == null ? null : Base64.getEncoder().encodeToString(bytes);
    }

    private static String formatCharacter(Object value) throws SQLException, IOException {
        if (value instanceof NClob nClob) {
            return readClob(nClob);
        }
        if (value instanceof Clob clob) {
            return readClob(clob);
        }
        return value.toString();
    }

    private static String formatSqlXml(Object value) throws SQLException {
        if (value instanceof SQLXML sqlxml) {
            return sqlxml.getString();
        }
        return value.toString();
    }

    private static String formatFallback(Object value) throws SQLException, IOException {
        if (value instanceof NClob nClob) {
            return readClob(nClob);
        }
        if (value instanceof Clob clob) {
            return readClob(clob);
        }
        if (value instanceof Blob blob) {
            return Base64.getEncoder().encodeToString(blob.getBytes(1, (int) blob.length()));
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        return value.toString();
    }

    private static String readClob(Clob clob) throws SQLException, IOException {
        try (Reader reader = clob.getCharacterStream()) {
            StringBuilder builder = new StringBuilder();
            char[] buffer = new char[8192];
            int read;
            while ((read = reader.read(buffer)) != -1) {
                builder.append(buffer, 0, read);
            }
            return builder.toString();
        }
    }
}
