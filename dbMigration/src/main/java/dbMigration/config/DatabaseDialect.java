package dbMigration.config;

import java.util.Locale;

public enum DatabaseDialect {
    MYSQL,
    ORACLE,
    SQLSERVER;

    public static DatabaseDialect infer(DbConfig config, DatabaseDialect defaultDialect) {
        if (config == null) {
            return defaultDialect;
        }
        if (config.dialect != null && !config.dialect.isBlank()) {
            return parse(config.dialect);
        }
        if (config.jdbc_url != null && !config.jdbc_url.isBlank()) {
            return fromJdbcUrl(config.jdbc_url);
        }
        return defaultDialect;
    }

    public static DatabaseDialect parse(String value) {
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "mysql" -> MYSQL;
            case "oracle" -> ORACLE;
            case "sqlserver", "sql_server", "mssql" -> SQLSERVER;
            default -> throw new IllegalArgumentException("Unsupported database dialect: " + value);
        };
    }

    public static DatabaseDialect fromJdbcUrl(String jdbcUrl) {
        String normalized = jdbcUrl.trim().toLowerCase(Locale.ROOT);
        if (normalized.startsWith("jdbc:mysql:")) {
            return MYSQL;
        }
        if (normalized.startsWith("jdbc:oracle:")) {
            return ORACLE;
        }
        if (normalized.startsWith("jdbc:sqlserver:")) {
            return SQLSERVER;
        }
        throw new IllegalArgumentException("Unsupported JDBC URL for dialect inference: " + jdbcUrl);
    }
}
