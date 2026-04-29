package dbMigration;

import com.mysql.cj.jdbc.AbandonedConnectionCleanupThread;

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
import dbMigration.utils.DbConnectionFactory;
import dbMigration.utils.FileManager;
import dbMigration.utils.Import;
import dbMigration.utils.OutputPathResolver;

public class MigrationApp {

    private static Import importUtils = new Import();
    private static final Set<String> AGREEMENT_COLUMNS = Set.of("COD_ACC", "CODICE_ACCORDO");
    private static final String COD_ACC_PLACEHOLDER = ":cod_acc";

    static Config config;
    static String LOG_FILE = "src/resources/load_data_log.txt";

    private static final Logger logger =    LogManager.getLogger(MigrationApp.class);

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

        Path outputDir = OutputPathResolver.resolveOutputDirectory(config.data_dir);
        importUtils.importDataIntoCsv(config);

        FileManager.splitFiles(outputDir.toFile(), 10000000L);

        logger.info("files splitted");

        Path tmpDir = outputDir.resolve("TMP");
        removeLocks(outputDir.toString());
        removeLocks(tmpDir.toString());
        Map<String, String> csvTableMap = buildCsvTableMap(tmpDir.toString());
        try (Connection conn = DbConnectionFactory.openTargetConnection(config)) {
            csvTableMap = filterCsvTableMapByTargetTables(csvTableMap, conn);
        }
        Map<String, List<String>> tableCsvMap = groupCsvPathsByTable(csvTableMap);
        int totalTables = tableCsvMap.size();
        log("=== Starting parallel data load with " + config.max_workers + " workers ===");
        log("Total tables to process: " + totalTables);

        List<String> completed = Collections.synchronizedList(new ArrayList<>());
        Map<String, String> failed = Collections.synchronizedMap(new HashMap<>());
        List<String> skipped = Collections.synchronizedList(new ArrayList<>());
        Map<String, TableMeta> tableMetaMaps = Collections.synchronizedMap(new HashMap<>());
        // creare mappa di index che vengono eliminati dal DB e toglil controlli per FK
        // e Index
        try (Connection conn = DbConnectionFactory.openTargetConnection(config)) {
            tableMetaMaps = tableCsvMap.keySet().stream().collect(Collectors.toMap(table -> table, table -> {
                try {
                    removeForeignKeyChecks(conn);
                    return removeIndexes(table, conn);

                } catch (SQLException e) {
                    logger.error("could not remove indexes");
                    e.printStackTrace();
                    return new TableMeta();
                }
            }, (left, right) -> left, LinkedHashMap::new));
        }

        logger.info("dropped indexes and fkeys");

        ExecutorService executor = Executors.newFixedThreadPool(config.max_workers);
        List<Future<String>> futures = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : tableCsvMap.entrySet()) {
            futures.add(executor.submit(() -> loadTableCsvs(entry.getKey(), entry.getValue(), completed, failed,
                    skipped)));
        }
        int done = 0;
        for (Future<String> future : futures) {
            String result = future.get();
            if (!result.startsWith("OK >>")) {
                log(result);
            }
            printProgress(++done, totalTables);
        }
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);
        // Remove lock files
        removeLocks(tmpDir.toString());

        // Retry failed
        if (!failed.isEmpty()) {
            log("Retrying " + failed.size() + " failed tables...");
            List<String> retrySuccess = new ArrayList<>();
            Map<String, String> retryStillFailed = new HashMap<>();
            for (String table : failed.keySet()) {
                String result = loadTableCsvs(table, tableCsvMap.getOrDefault(table, List.of()), completed, retryStillFailed, skipped);
                log("[RETRY] " + result);
                if (result.startsWith("OK >>"))
                    retrySuccess.add(table);
                else
                    retryStillFailed.put(table, failed.get(table));
            }
            failed.clear();
            failed.putAll(retryStillFailed);
        }
        try (Connection conn = DbConnectionFactory.openTargetConnection(config)) {
            restoreIndexes(tableMetaMaps, conn);
            restoreForeignKeyChecks(conn);
        }
        // Final summary
        log("=== All data loads completed ===");
        log("Total successful: " + completed.size());
        log("Total skipped: " + skipped.size());
        log("Total failed: " + failed.size());
        // for (Map<String, String> item : failed) {
        // log(" ❌ " + item.get("table") + " failed permanently.");
        // }
        FileManager.eliminateTmpFiles(outputDir.toFile());
        removeLocks(outputDir.toString());
    }

    static Map<String, String> buildCsvTableMap(String dataDir) throws IOException {

        logger.info("building csv-table map");
        Map<String, String> map = new HashMap<>();
        try (Stream<Path> paths = Files.list(Paths.get(dataDir))) {
            paths.filter(p -> p.toString().endsWith(".csv")).forEach(p -> {
                String filename = p.getFileName().toString();
                String table = filename;

                // remove .csv
                if (table.endsWith(".csv")) {
                    table = table.substring(0, table.length() - 4);
                }

                // remove .partN suffix if present
                table = table.replaceAll("\\.part\\d+$", "");
                table = normalizeTableName(table);

                map.put(p.toString(), table);
            });
        }
        return map;
    }

    static Map<String, List<String>> groupCsvPathsByTable(Map<String, String> csvTableMap) {
        Map<String, List<String>> grouped = new LinkedHashMap<>();
        csvTableMap.entrySet().stream()
                .sorted(Comparator
                        .comparing(Map.Entry<String, String>::getValue)
                        .thenComparingInt(entry -> extractPartNumber(entry.getKey()))
                        .thenComparing(Map.Entry::getKey))
                .forEach(entry -> grouped
                        .computeIfAbsent(entry.getValue(), ignored -> new ArrayList<>())
                        .add(entry.getKey()));
        return grouped;
    }

    static Map<String, String> filterCsvTableMapByTargetTables(Map<String, String> csvTableMap, Connection conn)
            throws SQLException {
        Set<String> targetTables = getTargetTableNames(conn);
        Map<String, String> filtered = new LinkedHashMap<>();
        List<String> skipped = new ArrayList<>();

        for (Map.Entry<String, String> entry : csvTableMap.entrySet()) {
            String table = entry.getValue();
            if (targetTables.contains(normalizeTableName(table))) {
                filtered.put(entry.getKey(), table);
            } else {
                skipped.add(table + " <- " + entry.getKey());
            }
        }

        if (!skipped.isEmpty()) {
            log("Skipping " + skipped.size() + " CSV files with no matching target table");
            for (String item : skipped) {
                log("[SKIP] Missing target table for CSV: " + item);
            }
        }

        return filtered;
    }

    static Set<String> getTargetTableNames(Connection conn) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        Set<String> tableNames = new HashSet<>();
        String catalog = config.db_config_target.database;
        String schema = config.db_config_target.schema;

        collectTableNames(metaData, catalog, schema, tableNames);
        if (tableNames.isEmpty() && catalog != null) {
            collectTableNames(metaData, catalog, null, tableNames);
        }
        if (tableNames.isEmpty() && schema != null) {
            collectTableNames(metaData, null, schema, tableNames);
        }
        if (tableNames.isEmpty()) {
            collectTableNames(metaData, null, null, tableNames);
        }

        return tableNames;
    }

    static void collectTableNames(DatabaseMetaData metaData, String catalog, String schema, Set<String> tableNames)
            throws SQLException {
        try (ResultSet rs = metaData.getTables(catalog, schema, "%", null)) {
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                if (tableName != null && !tableName.isBlank()) {
                    tableNames.add(normalizeTableName(tableName));
                }
            }
        }
    }

    static String normalizeTableName(String tableName) {
        return tableName == null ? "" : tableName.trim().toUpperCase(Locale.ROOT);
    }

    static String loadTableCsvs(String table, List<String> csvPaths, List<String> completed, Map<String, String> failed,
            List<String> skipped) {
        table = normalizeTableName(table);
        if (csvPaths == null || csvPaths.isEmpty()) {
            failed.put(table, "No CSV files found");
            return table + ": ERROR - No CSV files found";
        }
        String lockPath = csvPaths.get(0) + "." + table + ".lock";
        File lockFile = new File(lockPath);

        if (lockFile.exists()) {
            skipped.add(table);
            return table + ": Skipped (already locked)";
        }

        try (FileWriter lock = new FileWriter(lockFile)) {
            lock.write("locked");
        } catch (IOException e) {
            failed.put(table, e.getMessage());
            return table + ": ERROR - " + e.getMessage();
        }

        try (Connection conn = DbConnectionFactory.openTargetConnection(config)) {
            try (Statement stmt = conn.createStatement()) {

                int before = countRows(stmt, table);

                removeForeignKeyChecks(conn);
                List<TargetColumn> columns = getTargetColumns(conn, table);
                int deleted = maybeDeleteFilteredRows(conn, table, columns);
                int afterDelete = deleted > 0 ? countRows(stmt, table) : before;
                for (String csvPath : csvPaths) {
                    loadCsvPart(stmt, table, csvPath, columns);
                }

                int after = countRows(stmt, table);
                int inserted = after - afterDelete;

                // stmt.execute("COMMIT;");

                completed.add(table);
                if (deleted > 0) {
                    log(String.format("DELETE %s filtered rows from table %s", deleted, table));
                }
                log(String.format("INSERT %s in table %s", inserted, table));
                return "OK >> " + table + ": " + inserted + " rows inserted";
            } finally {
                restoreForeignKeyChecks(conn);
            }
        } catch (Exception e) {
            failed.put(table, e.getMessage());
            // log(String.format("ERROR : table %s, with message: %s", table,
            // e.getMessage()));
            return table + ": ERROR - " + e.getMessage();
        } finally {
            lockFile.delete();
        }
    }

    static int countRows(Statement stmt, String table) throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM `" + table + "`");
        rs.next();
        return rs.getInt(1);
    }

    private static void loadCsvPart(Statement stmt, String table, String csvPath, List<TargetColumn> columns) throws SQLException {
        String colMapping = columns.stream()
                .map(TargetColumn::userVariable)
                .collect(Collectors.joining(", "));
        String setClause = columns.stream()
                .map(MigrationApp::buildLoadAssignment)
                .collect(Collectors.joining(", "));
        String sql = String.format(
                "LOAD DATA LOCAL INFILE '%s' INTO TABLE `%s` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES (%s) SET %s;",
                csvPath.replace("\\", "\\\\"), table, colMapping, setClause);

        stmt.execute(sql);
    }

    static void printProgress(int done, int total) {
        System.out.print(String.format("Progress: %d/%d tables completed\r", done, total));
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
        dbSource.schema = props.getProperty("db_config_source.schema");
        dbSource.jdbc_url = props.getProperty("db_config_source.jdbc_url");
        dbSource.dialect = props.getProperty("db_config_source.dialect");
        dbSource.allow_local_infile = Boolean
                .parseBoolean(props.getProperty("db_config_source.allow_local_infile", "false"));
        cfg.db_config_source = dbSource;
        DbConfig dbTarget = new DbConfig();
        dbTarget.user = props.getProperty("db_config_target.user");
        dbTarget.password = props.getProperty("db_config_target.password");
        dbTarget.host = props.getProperty("db_config_target.host");
        dbTarget.database = props.getProperty("db_config_target.database");
        dbTarget.schema = props.getProperty("db_config_target.schema");
        dbTarget.jdbc_url = props.getProperty("db_config_target.jdbc_url");
        dbTarget.dialect = props.getProperty("db_config_target.dialect");
        dbTarget.allow_local_infile = Boolean
                .parseBoolean(props.getProperty("db_config_target.allow_local_infile", "false"));
        cfg.db_config_target = dbTarget;
        LightModeConfig lightMode = new LightModeConfig();
        lightMode.enabled = Boolean.parseBoolean(props.getProperty("light_mode.enabled", "false"));
        lightMode.dry_run = Boolean.parseBoolean(props.getProperty("light_mode.dry_run", "false"));
        lightMode.report_enabled = Boolean.parseBoolean(props.getProperty("light_mode.report_enabled", "true"));
        lightMode.report_file = props.getProperty("light_mode.report_file", "light_export_report.csv");
        lightMode.cod_acc = props.getProperty("light_mode.cod_acc");
        lightMode.max_rows_per_table = Integer.parseInt(props.getProperty("light_mode.max_rows_per_table", "50"));
        lightMode.relation_search_depth = Integer.parseInt(props.getProperty("light_mode.relation_search_depth", "6"));
        lightMode.uncapped_tables.addAll(parseUppercaseCsvSet(props.getProperty("light_mode.uncapped_tables", "")));
        lightMode.replace_filtered_tables.addAll(parseUppercaseCsvSet(props.getProperty("light_mode.replace_filtered_tables", "")));
        props.stringPropertyNames().stream()
                .filter(name -> name.startsWith("light_mode.manual_filter."))
                .forEach(name -> lightMode.manual_filters.put(
                        name.substring("light_mode.manual_filter.".length()).toUpperCase(Locale.ROOT),
                        props.getProperty(name)));
        props.stringPropertyNames().stream()
                .filter(name -> name.startsWith("light_mode.manual_delete_filter."))
                .forEach(name -> lightMode.manual_delete_filters.put(
                        name.substring("light_mode.manual_delete_filter.".length()).toUpperCase(Locale.ROOT),
                        props.getProperty(name)));
        props.stringPropertyNames().stream()
                .filter(name -> name.startsWith("light_mode.manual_order_by."))
                .forEach(name -> lightMode.manual_order_by.put(
                        name.substring("light_mode.manual_order_by.".length()).toUpperCase(Locale.ROOT),
                        props.getProperty(name)));
        cfg.light_mode = lightMode;
        cfg.include_tables = parseUppercaseCsvSet(props.getProperty("include_tables", ""));
        cfg.data_dir = props.getProperty("data_dir");
        cfg.max_workers = Integer.parseInt(props.getProperty("max_workers"));
        return cfg;
    }

    private static Set<String> parseUppercaseCsvSet(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return new LinkedHashSet<>();
        }
        return Arrays.stream(rawValue.split(","))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .map(value -> value.toUpperCase(Locale.ROOT))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static boolean checkConnections(Config conf) {
        try (Connection conn = DbConnectionFactory.openTargetConnection(conf)) {

            logger.info("-----------------------------------");
            logger.info("CONNECTION TARGET");
            logger.info("HOST: {}", conf.db_config_target.host);
            logger.info("USER: ".concat(conf.db_config_target.user));
            logger.info("DIALECT: {}", DbConnectionFactory.getTargetDialect(conf));
            logger.info("DATABASE: ".concat(conn.getMetaData().getDatabaseProductName()));
            logger.info("-----------------------------------");

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
       try (Connection conn = DbConnectionFactory.openSourceConnection(conf)){
            logger.info("-----------------------------------");
            logger.info("CONNECTION SOURCE");
            logger.info("HOST: ".concat(conf.db_config_source.host));
            logger.info("USER: ".concat(conf.db_config_source.user));
            logger.info("DIALECT: {}", DbConnectionFactory.getSourceDialect(conf));
            logger.info("DATABASE: ".concat(conn.getMetaData().getDatabaseProductName()));
            logger.info("-----------------------------------");
        }catch (SQLException e) {
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
                        stmt.addBatch(String.format("ALTER TABLE `%s` DROP INDEX `%s`", table, indexName));
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
                    String updateRule = fkRuleToString(fk.getShort("UPDATE_RULE"));
                    String deleteRule = fkRuleToString(fk.getShort("DELETE_RULE"));

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
                                            "ALTER TABLE `%s` ADD FOREIGN KEY (`%s`) REFERENCES `%s`(`%s`), ON UPDATE=%s, ON DELETE=%s%n\"",
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
            stmt.execute("SET sql_log_bin = 0");
            stmt.execute("SET GLOBAL local_infile = 'ON'");
            stmt.execute("SET unique_checks=0 ");
            stmt.execute("SET autocommit=1");
            stmt.execute("SET time_zone = '+00:00'");
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

    private static String fkRuleToString(short rule) {
        return switch (rule) {
            case DatabaseMetaData.importedKeyCascade -> "CASCADE";
            case DatabaseMetaData.importedKeySetNull -> "SET NULL";
            case DatabaseMetaData.importedKeySetDefault -> "SET DEFAULT";
            case DatabaseMetaData.importedKeyRestrict -> "RESTRICT";
            case DatabaseMetaData.importedKeyNoAction -> "NO ACTION";
            default -> "";
        };
    }

    private static List<TargetColumn> getTargetColumns(Connection conn, String table) throws SQLException {
        List<TargetColumn> columns = new ArrayList<>();
        DatabaseMetaData metaData = conn.getMetaData();
        try (ResultSet rs = metaData.getColumns(config.db_config_target.database, null, table, null)) {
            while (rs.next()) {
                columns.add(new TargetColumn(
                        rs.getInt("ORDINAL_POSITION"),
                        rs.getString("COLUMN_NAME"),
                        rs.getInt("DATA_TYPE")));
            }
        }
        columns.sort(Comparator.comparingInt(TargetColumn::ordinalPosition));
        if (columns.isEmpty()) {
            throw new SQLException("No target columns found for table " + table);
        }
        return columns;
    }

    private static int maybeDeleteFilteredRows(Connection conn, String table, List<TargetColumn> columns) throws SQLException {
        DeletePlan deletePlan = resolveDeletePlan(table, columns, config.light_mode);
        if (deletePlan == null) {
            return 0;
        }
        String sql = String.format("DELETE FROM `%s` WHERE %s", table, deletePlan.predicate());
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            bindDeleteParameters(stmt, deletePlan.parameterCount(), config.light_mode);
            return stmt.executeUpdate();
        }
    }

    private static DeletePlan resolveDeletePlan(String table, List<TargetColumn> columns, LightModeConfig lightMode) {
        if (!isLightModeEnabled(lightMode)) {
            return null;
        }

        String manualFilter = lightMode.manual_delete_filters.get(table);
        if (manualFilter != null && !manualFilter.isBlank()) {
            int parameterCount = countOccurrences(manualFilter, COD_ACC_PLACEHOLDER);
            return new DeletePlan(manualFilter.replace(COD_ACC_PLACEHOLDER, "?"), parameterCount);
        }

        if (!lightMode.replace_filtered_tables.contains(table)) {
            return null;
        }

        List<String> agreementColumns = columns.stream()
                .map(TargetColumn::name)
                .filter(MigrationApp::isAgreementLikeColumn)
                .toList();
        if (agreementColumns.isEmpty()) {
            logger.warn("Light-mode replace is enabled for {}, but no COD_ACC-like target column was found", table);
            return null;
        }

        String predicate = agreementColumns.stream()
                .map(column -> String.format("`%s` = ?", column))
                .collect(Collectors.joining(" OR "));
        if (agreementColumns.size() > 1) {
            predicate = "(" + predicate + ")";
        }
        return new DeletePlan(predicate, agreementColumns.size());
    }

    private static void bindDeleteParameters(PreparedStatement stmt, int parameterCount, LightModeConfig lightMode)
            throws SQLException {
        DatabaseDialect targetDialect = DbConnectionFactory.getTargetDialect(config);
        String codAcc = normalizeCodAcc(lightMode.cod_acc, targetDialect);
        for (int index = 1; index <= parameterCount; index++) {
            stmt.setString(index, codAcc);
        }
    }

    private static boolean isLightModeEnabled(LightModeConfig lightMode) {
        return lightMode != null
                && lightMode.enabled
                && lightMode.cod_acc != null
                && !lightMode.cod_acc.isBlank();
    }

    private static String normalizeCodAcc(String codAcc, DatabaseDialect dialect) {
        return dialect == DatabaseDialect.ORACLE ? codAcc.toUpperCase(Locale.ROOT) : codAcc;
    }

    private static boolean isAgreementLikeColumn(String columnName) {
        String normalized = normalizeTableName(columnName);
        return AGREEMENT_COLUMNS.contains(normalized)
                || normalized.startsWith("COD_ACC_")
                || normalized.startsWith("CODICE_ACCORDO_");
    }

    private static int countOccurrences(String text, String token) {
        int count = 0;
        int fromIndex = 0;
        while ((fromIndex = text.indexOf(token, fromIndex)) >= 0) {
            count++;
            fromIndex += token.length();
        }
        return count;
    }

    private static int extractPartNumber(String csvPath) {
        int markerIndex = csvPath.lastIndexOf(".part");
        int suffixIndex = csvPath.lastIndexOf(".csv");
        if (markerIndex < 0 || suffixIndex <= markerIndex + 5) {
            return 0;
        }
        try {
            return Integer.parseInt(csvPath.substring(markerIndex + 5, suffixIndex));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static String buildLoadAssignment(TargetColumn column) {
        String rawValue = normalizeNullableValue(column.userVariable());
        String expression = isBinaryType(column.dataType())
                ? String.format(
                        "IF(%s IS NULL OR %s = '' OR LOWER(%s) = 'null', NULL, FROM_BASE64(%s))",
                        column.userVariable(),
                        column.userVariable(),
                        column.userVariable(),
                        column.userVariable())
                : rawValue;
        return String.format("`%s` = %s", column.name(), expression);
    }

    private static String normalizeNullableValue(String userVariable) {
        return String.format("NULLIF(NULLIF(%s, 'null'), '')", userVariable);
    }

    private static boolean isBinaryType(int dataType) {
        return switch (dataType) {
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> true;
            default -> false;
        };
    }

    private record TargetColumn(int ordinalPosition, String name, int dataType) {
        private String userVariable() {
            return "@v" + ordinalPosition;
        }
    }

    private record DeletePlan(String predicate, int parameterCount) {
    }
}
