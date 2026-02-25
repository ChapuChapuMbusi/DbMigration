package dbMigration.utils;

import com.opencsv.CSVWriter;
import dbMigration.config.Config;
import dbMigration.config.DbConfig;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class Import {

    /**
     * Export all tables from Oracle database to CSV files in the given output directory.
     */
    public void importDataIntoCsv(Config config) {
        String outputDir = config.data_dir;
        int THREADS = config.max_workers;
        DbConfig dbConfig = config.db_config_source;

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);

        try (Connection conn = getConnectionTarget(true, config)) {
            System.out.println("Connected to Oracle Source Database");

            // Create output directory if it doesn't exist
            File dir = new File(outputDir);
            if (!dir.exists()) dir.mkdirs();

            // Fetch all table names for the given schema
            List<String> tables = getTableNames(conn, dbConfig.user);

            List<Future<?>> futures = new ArrayList<>();
            for (String table : tables) {
                futures.add(
                    executor.submit(() -> {
                        try (
                            Connection threadConn = getConnectionTarget(
                                true,
                                config
                            )
                        ) {
                            exportTableToCSV(threadConn, table, outputDir);
                        } catch (SQLException e) {
                            System.err.println(
                                "Connection error for table " +
                                    table +
                                    ": " +
                                    e.getMessage()
                            );
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    })
                );
            }

            // Wait for all threads to finish
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            try {
                exportSequencesToCSV(conn, dbConfig.user, outputDir);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to export sequences", e);
            }

            System.out.println(
                "All exports finished. Files in: " + dir.getAbsolutePath()
            );
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Get all table names for the given Oracle schema.
     */
    private static List<String> getTableNames(Connection conn, String schema)
        throws SQLException {
        List<String> tableNames = new ArrayList<>();
        String sql = "SELECT table_name FROM all_tables WHERE owner = ?";
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

    /**
     * Export a single table to a CSV file using streaming to avoid high memory usage.
     */
    private static void exportTableToCSV(
        Connection conn,
        String tableName,
        String outputDir
    ) throws SQLException, IOException {
        String outputFile = outputDir + File.separator + tableName + ".csv";
        System.out.println(
            "[" + Thread.currentThread().getName() + "] Exporting: " + tableName
        );

        String sql = "SELECT * FROM " + tableName;

        try (
            Statement stmt = conn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY
            )
        ) {
            stmt.setFetchSize(1000); // Oracle streaming

            try (
                ResultSet rs = stmt.executeQuery(sql);
                CSVWriter writer = new CSVWriter(
                    new OutputStreamWriter(
                        new FileOutputStream(outputFile),
                        StandardCharsets.UTF_8
                    )
                )
            ) {
                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();

                // Write header
                String[] header = new String[columnCount];
                for (int i = 1; i <= columnCount; i++) {
                    header[i - 1] = meta.getColumnName(i);
                }
                writer.writeNext(header, false);

                // Write rows
                int rowCount = 0;
                while (rs.next()) {
                    String[] row = new String[columnCount];
                    for (int i = 1; i <= columnCount; i++) {
                        Object val = rs.getObject(i);
                        row[i - 1] = (val != null) ? val.toString() : null;
                    }
                    writer.writeNext(row, false);

                    if (++rowCount % 1000 == 0) {
                        writer.flush(); // keep memory usage low
                    }
                }
            }
        }
    }

    /**
     * Export Oracle sequences into a SEQUENCE.csv file compatible with the target MySQL table.
     */
    private static void exportSequencesToCSV(
        Connection conn,
        String schema,
        String outputDir
    ) throws SQLException, IOException {
        String outputFile = outputDir + File.separator + "SEQUENCE.csv";
        String sql = """
            SELECT
                sequence_name,
                increment_by,
                min_value,
                max_value,
                CASE
                    WHEN increment_by >= 0 THEN min_value
                    ELSE max_value
                END AS start_with,
                last_number AS next_value,
                CASE WHEN cycle_flag = 'Y' THEN 1 ELSE 0 END AS cycle_flag,
                cache_size,
                CASE WHEN order_flag = 'Y' THEN 1 ELSE 0 END AS order_flag,
                CAST(NULL AS VARCHAR2(200)) AS description
            FROM all_sequences
            WHERE sequence_owner = ?
            ORDER BY sequence_name
            """;

        try (
            PreparedStatement stmt = conn.prepareStatement(sql);
            CSVWriter writer = new CSVWriter(
                new OutputStreamWriter(
                    new FileOutputStream(outputFile),
                    StandardCharsets.UTF_8
                )
            )
        ) {
            stmt.setString(1, schema.toUpperCase());
            try (ResultSet rs = stmt.executeQuery()) {
                writer.writeNext(
                    new String[] {
                        "sequence_name",
                        "increment_by",
                        "min_value",
                        "max_value",
                        "start_with",
                        "next_value",
                        "cycle_flag",
                        "cache_size",
                        "order_flag",
                        "description",
                    },
                    false
                );

                int rowCount = 0;
                while (rs.next()) {
                    String[] row = new String[] {
                        rs.getString("sequence_name"),
                        String.valueOf(rs.getBigDecimal("increment_by")),
                        String.valueOf(rs.getBigDecimal("min_value")),
                        String.valueOf(rs.getBigDecimal("max_value")),
                        String.valueOf(rs.getBigDecimal("start_with")),
                        String.valueOf(rs.getBigDecimal("next_value")),
                        String.valueOf(rs.getInt("cycle_flag")),
                        String.valueOf(rs.getInt("cache_size")),
                        String.valueOf(rs.getInt("order_flag")),
                        rs.getString("description"),
                    };
                    writer.writeNext(row, false);

                    if (++rowCount % 1000 == 0) {
                        writer.flush();
                    }
                }
            }
        }
    }

    /**
     * Get Oracle connection.
     */
    static Connection getConnectionTarget(boolean source, Config config)
        throws SQLException {
        DbConfig db = source
            ? config.db_config_source
            : config.db_config_target;
        // Using Oracle service name format
        String url = String.format(
            "jdbc:oracle:thin:@//%s:1521/%s",
            db.host,
            db.database
        );
        Properties props = new Properties();
        props.setProperty("user", db.user);
        props.setProperty("password", db.password);
        return DriverManager.getConnection(url, props);
    }
}
