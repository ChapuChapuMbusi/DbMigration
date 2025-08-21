package dbMigration.utils;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import dbMigration.config.Config;
import dbMigration.config.DbConfig;

public class ConnectionHandler {


        public java.sql.Connection getConnectionTarget(Config config) throws SQLException {
        DbConfig db = config.db_config_target;
        String url = String.format(
                "jdbc:mysql://%s/%s?allowLoadLocalInfile=%s",
                db.host, db.database, db.allow_local_infile);
        Properties props = new Properties();
        props.setProperty("user", db.user);
        props.setProperty("password", db.password);
        props.setProperty("allowLoadLocalInfile", String.valueOf(db.allow_local_infile));
        return DriverManager.getConnection(url, props);
    }
}
