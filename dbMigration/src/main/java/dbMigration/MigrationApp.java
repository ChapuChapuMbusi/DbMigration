package dbMigration;

import com.google.gson.*;
import com.mysql.cj.jdbc.AbandonedConnectionCleanupThread;
import com.mysql.cj.xdevapi.Table;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.stream.*;

import dbMigration.config.*;
import dbMigration.entity.ForeignKey;
import dbMigration.entity.Index;
import dbMigration.entity.TableMeta;
import dbMigration.utils.FileManager;
import dbMigration.utils.Import;

public class MigrationApp {

    private static Import importUtils = new Import();

    static Config config;
    static String LOG_FILE = "src/resources/load_data_log.txt";

    private static final Logger logger = LogManager.getLogger(MigrationApp.class);

    public static void main(String[] args) throws Exception {

        // Hook Chiusura thread houseKeeping Driver Oracle/MySql
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                // MySQL cleanup
                AbandonedConnectionCleanupThread.checkedShutdown();
                logger.info("MySQL cleanup thread stopped.");

                // Deregister only non-Oracle drivers
                Enumeration<Driver> drivers = DriverManager.getDrivers();
                while (drivers.hasMoreElements()) {
                    Driver d = drivers.nextElement();
                    if (d.getClass().getClassLoader() == MigrationApp.class.getClassLoader()
                            && !d.getClass().getName().startsWith("oracle.jdbc")) {
                        try {
                            DriverManager.deregisterDriver(d);
                            logger.info("Deregistered driver: " + d);
                        } catch (SQLException e) {
                            logger.warn("Error deregistering driver: " + d, e);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error during shutdown cleanup", e);
            }
        }));

        Properties props = loadProperties();
        config = buildConfigFromProperties(props);

        if (!checkConnections(config)) {
            logger.error("Issues with DB connection");
            return;
        }

        // importUtils.importDataIntoCsv(config);

        FileManager.splitFiles(new File(config.data_dir), 10000000L);

        logger.info("files splitted");

        Map<String, String> csvTableMap = buildCsvTableMap(config.data_dir + "\\TMP");
        int totalFiles = csvTableMap.size();
        log("=== Starting parallel data load with " + config.max_workers + " workers ===");
        log("Total files to process: " + totalFiles);

        List<String> completed = Collections.synchronizedList(new ArrayList<>());
        Map<String, String> failed = Collections.synchronizedMap(new HashMap<>());
        List<String> skipped = Collections.synchronizedList(new ArrayList<>());
        Map<String, TableMeta> tableMetaMaps = Collections.synchronizedMap(new HashMap<>());
        // creare mappa di index che vengono eliminati dal DB e toglil controlli per FK
        // e Index
        try (Connection conn = getConnectionTarget()) {
            tableMetaMaps = csvTableMap.entrySet().stream().collect(Collectors.toMap(k -> k.getKey(), v -> {
                try {
                    removeForeignKeyChecks(conn);
                    return removeIndexes(v.getValue(), conn);

                } catch (SQLException e) {
                    logger.error("could not remove indexes");
                    e.printStackTrace();
                    return new TableMeta();
                }
            }));
        }

        logger.info("dropped indexes and fkeys");

        ExecutorService executor = Executors.newFixedThreadPool(config.max_workers);
        List<Future<String>> futures = new ArrayList<>();
        for (Map.Entry<String, String> entry : csvTableMap.entrySet()) {
            futures.add(executor.submit(() -> loadSingleCsv(entry, completed, failed, skipped)));
        }
        int done = 0;
        for (Future<String> future : futures) {
            future.get();
            printProgress(++done, totalFiles);
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
        // Remove lock files
        removeLocks(config.data_dir + "\\TMP");

        // Retry failed
        if (!failed.isEmpty()) {
            log("Retrying " + failed.size() + " failed files...");
            List<String> retrySuccess = new ArrayList<>();
            Map<String, String> retryStillFailed = new HashMap<>();
            for (Map.Entry<String, String> entry : failed.entrySet()) {
                String result = loadSingleCsv(entry, completed, retryStillFailed, skipped);
                log("[RETRY] " + result);
                if (result.startsWith("OK >>"))
                    retrySuccess.add(entry.getKey());
                else
                    retryStillFailed.put(entry.getKey(), entry.getValue());
            }
            completed.addAll(retrySuccess);
            failed.remove(retrySuccess);
        }
        try (Connection conn = getConnectionTarget()) {
            restoreIndexes(tableMetaMaps, conn);
            restoreForeignKeyChecks(conn);
        }
        // Final summary
        log("=== All data loads completed ===");
        log("Total successful: " + completed.size());
        log("Total failed: " + failed.size());
        // for (Map<String, String> item : failed) {
        // log(" ❌ " + item.get("table") + " failed permanently.");
        // }
        FileManager.eliminateTmpFiles(new File(config.data_dir));
        removeLocks(config.data_dir);
    }

    static Map<String, String> buildCsvTableMap(String dataDir) throws IOException {

        logger.info("building csv-table map");
        List<Map<String, String>> mapping = new ArrayList<>();
        Map<String, String> map = new HashMap<>();
        try (Stream<Path> paths = Files.list(Paths.get(dataDir))) {
            paths.filter(p -> p.toString().toLowerCase().endsWith(".csv")).forEach(p -> {
                String filename = p.getFileName().toString();
                String table = filename;

                // remove .csv
                if (table.toLowerCase().endsWith(".csv")) {
                    table = table.substring(0, table.length() - 4);
                }

                // remove .partN suffix if present
                table = table.replaceAll("\\.part\\d+$", "");

                map.put(p.toString(), table);
                mapping.add(map);
            });
        }
        return map;
    }

    static String loadSingleCsv(Map.Entry<String, String> item, List<String> completed, Map<String, String> failed,
            List<String> skipped) {
        String csvPath = item.getKey();
        String table = item.getValue();
        String lockPath = csvPath + ".lock";
        File lockFile = new File(lockPath);

        if (lockFile.exists()) {
            skipped.add(table);
            return table + ": Skipped (already locked)";
        }

        try (FileWriter lock = new FileWriter(lockFile)) {
            lock.write("locked");
        } catch (IOException e) {
            failed.put(item.getKey(), item.getValue());
            return table + ": ERROR - " + e.getMessage();
        }

        try (Connection conn = getConnectionTarget()) {
            try (Statement stmt = conn.createStatement()) {

                int before = countRows(stmt, table);

                removeForeignKeyChecks(conn);
                // Get columns
                List<String> columns = new ArrayList<>();
                ResultSet rs = stmt.executeQuery("SHOW COLUMNS FROM " + table);
                while (rs.next())
                    columns.add(rs.getString(1));

                // Build LOAD DATA SQL
                String colMapping = columns.stream().map(c -> "@" + c).collect(Collectors.joining(", "));
                String setClause = columns.stream()
                        .map(c -> "`" + c + "`" + " = NULLIF(NULLIF(@" + c + ", 'null'), '')")
                        .collect(Collectors.joining(", "));
                String sql = String.format(
                        "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES (%s) SET %s;",
                        csvPath.replace("\\", "\\\\"), table, colMapping, setClause);

                stmt.execute(sql);

                restoreForeignKeyChecks(conn);

                int after = countRows(stmt, table);
                int inserted = after - before;

                // stmt.execute("COMMIT;");

                completed.add(table);
                // log(String.format("INSERT %s in table %s", inserted, table));
                return "OK >> " + table + ": " + inserted + " rows inserted";
            }
        } catch (Exception e) {
            failed.put(item.getKey(), item.getValue());
            // log(String.format("ERROR : table %s, with message: %s", table,
            // e.getMessage()));
            return table + ": ERROR - " + e.getMessage();
        } finally {
            lockFile.delete();
        }
    }

    static Connection getConnectionTarget() throws SQLException {
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

    static int countRows(Statement stmt, String table) throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table);
        rs.next();
        return rs.getInt(1);
    }

    static void printProgress(int done, int total) {
        System.out.print(String.format("Progress: %d/%d files completed\r", done, total));
    }

    static void log(String message) {
        logger.info(message);
    }

    static void removeLocks(String dataDir) {
        File dir = new File(dataDir);
        for (File file : dir.listFiles()) {
            if (file.getName().toLowerCase().endsWith(".lock"))
                file.delete();
        }
    }

    static Config buildConfigFromProperties(Properties props) {

        Config cfg = new Config();
        DbConfig dbSource = new DbConfig();
        dbSource.user = props.getProperty("db_config_source.user");
        dbSource.password = props.getProperty("db_config_source.password");
        dbSource.host = props.getProperty("db_config_source.host");
        dbSource.database = props.getProperty("db_config_source.database");
        dbSource.allow_local_infile = Boolean
                .parseBoolean(props.getProperty("db_config_source.allow_local_infile", "false"));
        cfg.db_config_source = dbSource;
        DbConfig dbTarget = new DbConfig();
        dbTarget.user = props.getProperty("db_config_target.user");
        dbTarget.password = props.getProperty("db_config_target.password");
        dbTarget.host = props.getProperty("db_config_target.host");
        dbTarget.database = props.getProperty("db_config_target.database");
        dbTarget.allow_local_infile = Boolean
                .parseBoolean(props.getProperty("db_config_target.allow_local_infile", "false"));
        cfg.db_config_target = dbTarget;
        cfg.data_dir = props.getProperty("data_dir");
        cfg.max_workers = Integer.parseInt(props.getProperty("max_workers"));
        return cfg;
    }

    private static boolean checkConnections(Config conf) {
        try (Connection conn = getConnectionTarget()) {

            logger.info("-----------------------------------");
            logger.info("CONNECTION TARGET");
            logger.info("HOST: {}", conf.db_config_target.host);
            logger.info("USER: ".concat(conf.db_config_target.user));
            logger.info("DATABASE: ".concat(conn.getMetaData().getDatabaseProductName()));
            logger.info("-----------------------------------");

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        try (Connection conn = DriverManager.getConnection(conf.db_config_source.host, conf.db_config_source.user,
                conf.db_config_source.password)) {
            logger.info("-----------------------------------");
            logger.info("CONNECTION SOURCE");
            logger.info("HOST: ".concat(conf.db_config_source.host));
            logger.info("USER: ".concat(conf.db_config_source.user));
            logger.info("DATABASE: ".concat(conn.getMetaData().getDatabaseProductName()));
            logger.info("-----------------------------------");
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static Properties loadProperties() {
        Properties props = new Properties();

        try (InputStream input = MigrationApp.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {

            if (input == null) {
                throw new RuntimeException("application.properties not found in classpath!");
            }

            props.load(input);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }

        return props;
    }

    public static TableMeta removeIndexes(String table, Connection conn) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        List<Index> indexList = new ArrayList<>();
        List<ForeignKey> fkeys = new ArrayList<>();
        // disable indexes query and store index data in var
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet indexes = metaData.getIndexInfo(null, null, table, false, false)) {
                while (indexes.next()) {
                    String indexName = indexes.getString("INDEX_NAME");
                    String columnName = indexes.getString("COLUMN_NAME");
                    boolean nonUnique = indexes.getBoolean("NON_UNIQUE");
                    if (indexName.contains("PRIMARY")) {
                        continue;
                    } else if (indexName.contains("FK_")) {
                        continue;
                    } else {
                        stmt.addBatch(String.format("ALTER TABLE `%s` DROP INDEX `%s`", table, indexName));
                    }

                    indexList.stream()
                            .filter(i -> i.getName().equals(indexName))
                            .findFirst()
                            .ifPresentOrElse(
                                    i -> i.getColumns().add(columnName),
                                    () -> {
                                        Index i = new Index();
                                        i.setName(indexName);
                                        i.setUnique(!nonUnique);
                                        i.setColumns(new ArrayList<>());
                                        i.getColumns().add(columnName);
                                        indexList.add(i);
                                    });
                }
                stmt.executeBatch();
            }

            try (ResultSet fk = metaData.getImportedKeys(null, null, table)) {
                stmt.clearBatch();
                while (fk.next()) {
                    String fkName = fk.getString("FK_NAME");
                    String fkColumn = fk.getString("FKCOLUMN_NAME");
                    String pkTable = fk.getString("PKTABLE_NAME");
                    String pkColumn = fk.getString("PKCOLUMN_NAME");
                    short updateRule = fk.getShort("UPDATE_RULE");
                    short deleteRule = fk.getShort("DELETE_RULE");

                    stmt.addBatch(String.format("ALTER TABLE `%s` DROP FOREIGN KEY `%s`", table, fkName));

                    fkeys.stream()
                            .filter(f -> f.getFkName().equals(fkName))
                            .findFirst()
                            .ifPresentOrElse(
                                    f -> {
                                        f.getFkColumns().add(fkColumn);
                                        f.getFkReferenceColumns().add(pkColumn);
                                    },
                                    () -> {
                                        ForeignKey f = new ForeignKey();
                                        f.setFkName(fkName);
                                        f.setFkColumns(new ArrayList<>());
                                        f.getFkColumns().add(fkColumn);
                                        f.setFkReferenceColumns(new ArrayList<>());
                                        f.getFkReferenceColumns().add(pkColumn);
                                        f.setDeleteRule(deleteRule);
                                        f.setUpdateRule(updateRule);
                                        f.setFkTable(pkTable);
                                        fkeys.add(f);
                                    });
                }

                stmt.executeBatch();

            } catch (SQLException e) {
                return new TableMeta();
            }
        } catch (SQLException e) {
            return new TableMeta();
        }
        return new TableMeta(fkeys, indexList);
    }

    public static void restoreIndexes(Map<String, TableMeta> map, Connection conn) throws SQLException {
        try (Statement stm = conn.createStatement()) {
            map.entrySet().stream().forEach(item -> {
                String table = item.getKey();
                TableMeta meta = item.getValue();
                List<Index> indexList = meta.getI();
                if (indexList != null && !indexList.isEmpty()) {
                    indexList.stream().forEach(index -> {

                        try {
                            stm.addBatch(
                                    String.format("CREATE `%s` INDEX `%s` ON `%s` (`%s`)",
                                            index.isUnique() ? "UNIQUE" : "",
                                            index.getName(), table, index.getColumns().stream()
                                                    .collect(Collectors.joining(","))));
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    });
                }

                List<ForeignKey> fkeys = meta.getfK();
                if (fkeys != null && !fkeys.isEmpty()) {
                    fkeys.stream().forEach(fk -> {
                        try {
                            stm.addBatch(
                                    String.format(
                                            "ALTER TABLE `%s` ADD FOREIGN KEY (`%s`) REFERENCES `%s`(`%s`), ON UPDATE=%d, ON DELETE=%d%n\"",
                                            table,
                                            fk.getFkColumns().stream()
                                                    .collect(Collectors.joining(",")),
                                            fk.getFkTable(),
                                            fk.getFkReferenceColumns().stream().collect(Collectors.joining(",")),
                                            fk.getUpdateRule(),
                                            fk.getDeleteRule()));
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    });
                }

            });
            stm.executeBatch();
            logger.info("restored indexes and fkeys");
        }
    }

    public static void removeForeignKeyChecks(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Disabled checks for FK errors and performance
            stmt.execute("SET FOREIGN_KEY_CHECKS=0");
            stmt.execute("SET GLOBAL local_infile = 'ON'");
            stmt.execute("SET unique_checks=0 ");
            stmt.execute("SET autocommit=1");
        }
    }

    public static void restoreForeignKeyChecks(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Disabled checks for FK errors and performance
            stmt.execute("SET FOREIGN_KEY_CHECKS=1");
            stmt.execute("SET unique_checks=1 ");
            // stmt.execute("SET autocommit=1");
        }
    }
}
