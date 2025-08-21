package dbMigration;

import com.google.gson.*;
import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

import dbMigration.config.*;
import dbMigration.utils.Import;

public class MigrationApp {

    private static Import importUtils = new Import();
    

    static Config config;
    static String LOG_FILE = "load_data_log.txt";

    public static void main(String[] args) throws Exception {
        Properties props = loadProperties("src/main/resources/application.properties");
        config = buildConfigFromProperties(props);

        importUtils.importDataIntoCsv(config);

        List<Map<String, String>> csvTableMap = buildCsvTableMap(config.data_dir);
        int totalFiles = csvTableMap.size();
        log("=== Starting parallel data load with " + config.max_workers + " workers ===");
        log("Total files to process: " + totalFiles);

        List<String> completed = Collections.synchronizedList(new ArrayList<>());
        List<Map<String, String>> failed = Collections.synchronizedList(new ArrayList<>());
        List<String> skipped = Collections.synchronizedList(new ArrayList<>());

        ExecutorService executor = Executors.newFixedThreadPool(config.max_workers);
        List<Future<String>> futures = new ArrayList<>();
        for (Map<String, String> item : csvTableMap) {
            final Map<String, String> currentItem = item;
            futures.add(executor.submit(() -> loadSingleCsv(currentItem, completed, failed, skipped)));
        }
        int done = 0;
        for (Future<String> future : futures) {
            future.get();
            printProgress(++done, totalFiles);
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
        // Remove lock files
        removeLocks(config.data_dir);

        // Retry failed
        if (!failed.isEmpty()) {
            log("Retrying " + failed.size() + " failed files...");
            List<String> retrySuccess = new ArrayList<>();
            List<Map<String, String>> retryStillFailed = new ArrayList<>();
            for (Map<String, String> item : failed) {
                String result = loadSingleCsv(item, completed, retryStillFailed, skipped);
                log("[RETRY] " + result);
                if (result.startsWith("✅"))
                    retrySuccess.add(item.get("table"));
                else
                    retryStillFailed.add(item);
            }
            completed.addAll(retrySuccess);
            failed.addAll(retryStillFailed);
        }

        // Final summary
        log("=== All data loads completed ===");
        log("✅ Total successful: " + completed.size());
        log("❌ Total failed: " + failed.size());
        // for (Map<String, String> item : failed) {
        // log(" ❌ " + item.get("table") + " failed permanently.");
        // }
        removeLocks(config.data_dir);
    }

    static List<Map<String, String>> buildCsvTableMap(String dataDir) throws IOException {
        List<Map<String, String>> mapping = new ArrayList<>();
        try (Stream<Path> paths = Files.list(Paths.get(dataDir))) {
            paths.filter(p -> p.toString().toLowerCase().endsWith(".csv")).forEach(p -> {
                String filename = p.getFileName().toString();
                String[] parts = filename.replace(".csv", "").split("_");
                String table;
                if (parts.length > 2) {
                    table = String.join("_", Arrays.copyOf(parts, parts.length - 2));
                } else {
                    table = parts[0];
                }
                Map<String, String> map = new HashMap<>();
                map.put("csv", p.toString());
                map.put("table", table);
                mapping.add(map);
            });
        }
        return mapping;
    }

    static String loadSingleCsv(Map<String, String> item, List<String> completed, List<Map<String, String>> failed,
            List<String> skipped) {
        String csvPath = item.get("csv");
        String table = item.get("table");
        String lockPath = csvPath + ".lock";
        File lockFile = new File(lockPath);

        if (lockFile.exists()) {
            skipped.add(table);
            return "⏭️ " + table + ": Skipped (already locked)";
        }

        try (FileWriter lock = new FileWriter(lockFile)) {
            lock.write("locked");
        } catch (IOException e) {
            failed.add(item);
            return "❌ " + table + ": ERROR - " + e.getMessage();
        }

        try (Connection conn = getConnectionTarget()) {
            Statement stmt = conn.createStatement();
            stmt.execute("SET FOREIGN_KEY_CHECKS=0");
            stmt.execute("SET GLOBAL local_infile = 'ON'");
            int before = countRows(stmt, table);

            // Get columns
            List<String> columns = new ArrayList<>();
            ResultSet rs = stmt.executeQuery("SHOW COLUMNS FROM " + table);
            while (rs.next())
                columns.add(rs.getString(1));

            // Build LOAD DATA SQL
            String colMapping = columns.stream().map(c -> "@" + c).collect(Collectors.joining(", "));
            String setClause = columns.stream()
                    .map(c -> c + " = NULLIF(NULLIF(@" + c + ", 'null'), '')")
                    .collect(Collectors.joining(", "));
            String sql = String.format(
                    "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES (%s) SET %s;",
                    csvPath.replace("\\", "\\\\"), table, colMapping, setClause);

            stmt.execute(sql);
            int after = countRows(stmt, table);
            int inserted = after - before;

            stmt.execute("SET FOREIGN_KEY_CHECKS=1");
            completed.add(table);
            log(String.format("INSERT %s in table %s", inserted, table));
            return "✅ " + table + ": " + inserted + " rows inserted";
        } catch (Exception e) {
            failed.add(item);
            log(String.format("ERROR : table  %s, with message: %s", table, e.getMessage()));
            return "❌ " + table + ": ERROR - " + e.getMessage();
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
        System.out.print(String.format("Progress: ✅ %d/%d files completed\r", done, total));
    }

    static void log(String message) {
        String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
        String line = "[" + timestamp + "] " + message;
        System.out.println(line);
        try (FileWriter fw = new FileWriter(LOG_FILE, true)) {
            fw.write(line + "\n");
        } catch (IOException ignored) {
        }
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
        dbSource.user = props.getProperty("db_config_target.user");
        dbSource.password = props.getProperty("db_config_target.password");
        dbSource.host = props.getProperty("db_config_target.host");
        dbSource.database = props.getProperty("db_config_target.database");
        dbSource.allow_local_infile = Boolean.parseBoolean(props.getProperty("db_config_target.allow_local_infile", "false"));
        cfg.db_config_source = dbSource;
        DbConfig dbTarget = new DbConfig();
        dbTarget.user = props.getProperty("db_config_source.user");
        dbTarget.password = props.getProperty("db_config_source.password");
        dbTarget.host = props.getProperty("db_config_source.host");
        dbTarget.database = props.getProperty("db_config_source.database");
        dbTarget.allow_local_infile = Boolean.parseBoolean(props.getProperty("db_config_source.allow_local_infile", "false"));
        cfg.db_config_target= dbTarget;
        cfg.data_dir = props.getProperty("data_dir");
        cfg.max_workers = Integer.parseInt(props.getProperty("max_workers"));
        return cfg;
    }

    static Properties loadProperties(String path) throws IOException {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(path)) {
            props.load(input);
        }
        return props;
    }
}