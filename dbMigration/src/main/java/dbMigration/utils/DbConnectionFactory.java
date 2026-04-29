package dbMigration.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import dbMigration.config.Config;
import dbMigration.config.DatabaseDialect;
import dbMigration.config.DbConfig;

public final class DbConnectionFactory {

    private DbConnectionFactory() {
    }

    public static Connection openSourceConnection(Config config) throws SQLException {
        return openConnection(config.db_config_source, DatabaseDialect.ORACLE);
    }

    public static Connection openTargetConnection(Config config) throws SQLException {
        return openConnection(config.db_config_target, DatabaseDialect.MYSQL);
    }

    public static DatabaseDialect getSourceDialect(Config config) {
        return DatabaseDialect.infer(config.db_config_source, DatabaseDialect.ORACLE);
    }

    public static DatabaseDialect getTargetDialect(Config config) {
        return DatabaseDialect.infer(config.db_config_target, DatabaseDialect.MYSQL);
    }

    public static Connection openConnection(DbConfig dbConfig, DatabaseDialect defaultDialect) throws SQLException {
        DatabaseDialect dialect = DatabaseDialect.infer(dbConfig, defaultDialect);
        Properties props = new Properties();
        props.setProperty("user", dbConfig.user);
        props.setProperty("password", dbConfig.password);
        if (dialect == DatabaseDialect.MYSQL) {
            props.setProperty("allowLoadLocalInfile", String.valueOf(dbConfig.allow_local_infile));
            props.setProperty("zeroDateTimeBehavior", "CONVERT_TO_NULL");
        }
        return DriverManager.getConnection(resolveJdbcUrl(dbConfig, dialect), props);
    }

    private static String resolveJdbcUrl(DbConfig dbConfig, DatabaseDialect dialect) {
        if (dbConfig.jdbc_url != null && !dbConfig.jdbc_url.isBlank()) {
            return dbConfig.jdbc_url;
        }
        return switch (dialect) {
            case MYSQL -> String.format(
                    "jdbc:mysql://%s/%s?allowLoadLocalInfile=%s",
                    dbConfig.host,
                    dbConfig.database,
                    dbConfig.allow_local_infile);
            case ORACLE -> String.format(
                    "jdbc:oracle:thin:@//%s/%s",
                    appendDefaultOraclePort(dbConfig.host),
                    dbConfig.database);
            case SQLSERVER -> String.format(
                    "jdbc:sqlserver://%s;databaseName=%s;encrypt=true;trustServerCertificate=true",
                    appendDefaultSqlServerPort(dbConfig.host),
                    dbConfig.database);
        };
    }

    private static String appendDefaultOraclePort(String host) {
        if (host == null || host.isBlank()) {
            return host;
        }
        return host.contains(":") ? host : host + ":1521";
    }

    private static String appendDefaultSqlServerPort(String host) {
        if (host == null || host.isBlank()) {
            return host;
        }
        return host.contains(":") ? host : host + ":1433";
    }
}
