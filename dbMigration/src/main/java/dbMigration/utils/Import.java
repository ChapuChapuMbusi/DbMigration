package dbMigration.utils;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

import com.opencsv.CSVWriter;

import dbMigration.config.Config;
import dbMigration.config.DbConfig;

public class Import {



    public void importDataIntoCsv(Config config) {

        String outputDir = config.data_dir;
        int THREADS = config.max_workers;

        DbConfig dbConfig = config.db_config_source;

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);

        try (Connection conn = DriverManager.getConnection(dbConfig.host, dbConfig.user, dbConfig.password)) {
            System.out.println("Connected to Oracle Database");

            File dir = new File(outputDir);
            if (!dir.exists())
                dir.mkdirs();

            List<String> tables = getTableNames(conn, dbConfig.user);

            List<Future<?>> futures = new ArrayList<>();
            for (String table : tables) {
                futures.add(executor.submit(() -> {
                    try (Connection threadConn = DriverManager.getConnection(dbConfig.host, dbConfig.user, dbConfig.password)) {
                        exportTableToCSV(threadConn, table, outputDir);
                    } catch (SQLException e) {
                        System.err.println("Connection error for table " + table + ": " + e.getMessage());
                    }
                }));
            }

            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            System.out.println("All exports finished. Files in: " + dir.getAbsolutePath());
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    private static List<String> getTableNames(Connection conn, String schema) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        String sql = "SELECT table_name FROM all_tables WHERE owner = ? ORDER BY table_name";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, schema.toUpperCase());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tableNames.add(rs.getString("table_name"));
                }
            }
        }
        return tableNames;
    }

    private static void exportTableToCSV(Connection conn, String tableName, String outputDir) {
        String outputFile = outputDir + File.separator + tableName + ".csv";
        System.out.println("[" + Thread.currentThread().getName() + "] Exporting: " + tableName);

        String sql = "SELECT * FROM " + tableName;

        try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            // Stream rows to avoid memory bloat
            stmt.setFetchSize(500);

            try (ResultSet rs = stmt.executeQuery(sql);
                    CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(outputFile)))) {

                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();

                // Header
                String[] header = new String[columnCount];
                for (int i = 1; i <= columnCount; i++) {
                    header[i - 1] = meta.getColumnName(i);
                }
                writer.writeNext(header);

                // Rows
                while (rs.next()) {
                    String[] row = new String[columnCount];
                    for (int i = 1; i <= columnCount; i++) {
                        Object val = rs.getObject(i);
                        row[i - 1] = (val != null) ? val.toString() : null;
                    }
                    writer.writeNext(row);
                }

            }
        } catch (SQLException | IOException e) {
            System.err.println("Error exporting table " + tableName + ": " + e.getMessage());
        }
    }

}
