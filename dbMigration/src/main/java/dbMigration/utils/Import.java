package dbMigration.utils;

import com.opencsv.CSVWriter;
import dbMigration.config.Config;
import dbMigration.config.DatabaseDialect;
import dbMigration.config.DbConfig;
import dbMigration.config.LightModeConfig;
import dbMigration.entity.SourceTableRef;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class Import {

    public void importDataIntoCsv(Config config) {
        Path outputDir;
        int THREADS = config.max_workers;
        DbConfig dbConfig = config.db_config_source;
        DatabaseDialect sourceDialect = DbConnectionFactory.getSourceDialect(config);
        LightModeConfig lightMode = config.light_mode;

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);

        try (Connection conn = DbConnectionFactory.openSourceConnection(config)) {
            System.out.println("Connected to Source Database: " + sourceDialect);
            outputDir = OutputPathResolver.resolveOutputDirectory(config.data_dir);

            List<SourceTableRef> tables = getTableRefs(conn, config, dbConfig, sourceDialect);
            if (isLightModeEnabled(lightMode) && config.include_tables != null && !config.include_tables.isEmpty()) {
                System.out.println("Restricting export to tables: " + config.include_tables);
            }
            LightExportPlanner lightExportPlanner = isLightModeEnabled(lightMode)
                ? LightExportPlanner.create(conn, tables, sourceDialect, lightMode)
                : null;

            List<Future<ExportReportEntry>> futures = new ArrayList<>();
            for (SourceTableRef table : tables) {
                futures.add(
                    executor.submit(() -> {
                        try (
                                Connection threadConn = DbConnectionFactory.openSourceConnection(config)
                        ) {
                            return exportTableToCSV(
                                threadConn,
                                table,
                                outputDir,
                                sourceDialect,
                                lightMode,
                                lightExportPlanner
                            );
                        } catch (SQLException e) {
                            System.err.println(
                                "Connection error for table " +
                                    table.getLogicalTableName() +
                                    ": " +
                                    e.getMessage()
                            );
                            return ExportReportEntry.error(table.getLogicalTableName(), e.getMessage());
                        } catch (IOException e) {
                            e.printStackTrace();
                            return ExportReportEntry.error(table.getLogicalTableName(), e.getMessage());
                        }
                    })
                );
            }

            List<ExportReportEntry> reportEntries = new ArrayList<>();
            for (Future<ExportReportEntry> f : futures) {
                try {
                    reportEntries.add(f.get());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (sourceDialect == DatabaseDialect.ORACLE) {
                try {
                    exportSequencesToCSV(conn, resolveSchema(dbConfig, sourceDialect), outputDir, lightMode, reportEntries);
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to export sequences", e);
                }
            }

            if (shouldProduceLightModeReport(lightMode)) {
                try {
                    writeLightModeReport(outputDir, lightMode, reportEntries);
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to write light mode report", e);
                }
            }

            System.out.println(
                "All exports finished. Files in: " + outputDir.toAbsolutePath()
            );
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to prepare output directory", e);
        } finally {
            executor.shutdown();
        }
    }

    private static List<SourceTableRef> getTableRefs(Connection conn, Config config, DbConfig dbConfig, DatabaseDialect dialect)
        throws SQLException {
        List<SourceTableRef> tableRefs = new ArrayList<>();
        Set<String> includeTables = config != null
            && isLightModeEnabled(config.light_mode)
            && config.include_tables != null
            ? config.include_tables
            : Set.of();
        DatabaseMetaData metaData = conn.getMetaData();
        String catalog = dialect == DatabaseDialect.MYSQL || dialect == DatabaseDialect.SQLSERVER
            ? dbConfig.database
            : null;
        String schema = resolveSchema(dbConfig, dialect);

        try (ResultSet rs = metaData.getTables(catalog, schema, "%", new String[] { "TABLE" })) {
            while (rs.next()) {
                tableRefs.add(
                    new SourceTableRef(
                        rs.getString("TABLE_CAT"),
                        rs.getString("TABLE_SCHEM"),
                        rs.getString("TABLE_NAME")
                    )
                );
            }
        }
        if (!includeTables.isEmpty()) {
            tableRefs = tableRefs.stream()
                .filter(tableRef -> includeTables.contains(tableRef.getLogicalTableName()))
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        }
        tableRefs.sort(
            Comparator
                .comparing(SourceTableRef::getCatalog, Comparator.nullsFirst(String::compareTo))
                .thenComparing(SourceTableRef::getSchema, Comparator.nullsFirst(String::compareTo))
                .thenComparing(SourceTableRef::getTableName)
        );
        validateUniqueFileNames(tableRefs);
        return tableRefs;
    }

    /**
     * Export a single table to a CSV file using streaming to avoid high memory usage.
     */
    private static ExportReportEntry exportTableToCSV(
        Connection conn,
        SourceTableRef table,
        Path outputDir,
        DatabaseDialect sourceDialect,
        LightModeConfig lightMode,
        LightExportPlanner lightExportPlanner)
        throws SQLException, IOException {
        Path outputFile = outputDir.resolve(table.getFileStem() + ".csv");
        String qualifiedTableName = buildQualifiedTableName(conn, table);
        System.out.println(
            "[" + Thread.currentThread().getName() + "] Exporting: " + qualifiedTableName
        );

        LightExportPlanner.ExportPlan exportPlan = isLightModeEnabled(lightMode)
            ? lightExportPlanner.plan(table)
            : new LightExportPlanner.ExportPlan(
                "SELECT * FROM " + qualifiedTableName,
                "SELECT COUNT(*) FROM " + qualifiedTableName,
                0,
                false,
                null
            );
        boolean auditEnabled = shouldProduceLightModeReport(lightMode) || isDryRun(lightMode);
        long expectedRowCount = auditEnabled ? countRowsForPlan(conn, exportPlan, lightMode, sourceDialect) : -1L;
        long finalCap = auditEnabled ? resolveFinalCap(expectedRowCount, exportPlan, lightMode) : -1L;
        logLightModeReportLine(table.getLogicalTableName(), expectedRowCount, finalCap, lightMode);

        if (isDryRun(lightMode)) {
            return new ExportReportEntry(table.getLogicalTableName(), expectedRowCount, finalCap, 0, true, exportPlan.codAccFiltered(), null);
        }

        try (
            PreparedStatement stmt = conn.prepareStatement(
                exportPlan.sql(),
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY
            )
        ) {
            if (sourceDialect == DatabaseDialect.MYSQL) {
                stmt.setFetchSize(Integer.MIN_VALUE);
            } else {
                stmt.setFetchSize(1000);
            }
            bindLightModeParameters(stmt, exportPlan, lightMode, sourceDialect);

            try (
                ResultSet rs = stmt.executeQuery();
                CSVWriter writer = new CSVWriter(
                    new OutputStreamWriter(
                        new FileOutputStream(outputFile.toFile()),
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
                        row[i - 1] = JdbcCsvValueFormatter.format(rs, meta, i);
                    }
                    writer.writeNext(row, false);

                    if (++rowCount % 1000 == 0) {
                        writer.flush(); // keep memory usage low
                    }
                }
                return new ExportReportEntry(
                    table.getLogicalTableName(),
                    expectedRowCount,
                    finalCap,
                    rowCount,
                    false,
                    exportPlan.codAccFiltered(),
                    null
                );
            }
        }
    }

    private static void exportSequencesToCSV(
        Connection conn,
        String schema,
        Path outputDir,
        LightModeConfig lightMode,
        List<ExportReportEntry> reportEntries
    ) throws SQLException, IOException {
        Path outputFile = outputDir.resolve("SEQUENCE.csv");
        String countSql = """
            SELECT COUNT(*)
            FROM all_sequences
            WHERE sequence_owner = ?
            """;
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
        Integer sequenceRowLimit = resolveGlobalRowLimit(lightMode);
        if (sequenceRowLimit != null) {
            sql = sql + " FETCH FIRST " + sequenceRowLimit + " ROWS ONLY";
        }
        boolean auditEnabled = shouldProduceLightModeReport(lightMode) || isDryRun(lightMode);
        long expectedRowCount = -1L;
        if (auditEnabled) {
            try (PreparedStatement countStmt = conn.prepareStatement(countSql)) {
                countStmt.setString(1, schema.toUpperCase());
                try (ResultSet rs = countStmt.executeQuery()) {
                    rs.next();
                    expectedRowCount = rs.getLong(1);
                }
            }
        }
        long finalCap = auditEnabled
            ? (sequenceRowLimit == null ? expectedRowCount : Math.min(expectedRowCount, sequenceRowLimit))
            : -1L;
        logLightModeReportLine("SEQUENCE", expectedRowCount, finalCap, lightMode);
        if (isDryRun(lightMode)) {
            reportEntries.add(new ExportReportEntry("SEQUENCE", expectedRowCount, finalCap, 0, true, false, null));
            return;
        }

        try (
            PreparedStatement stmt = conn.prepareStatement(sql);
            CSVWriter writer = new CSVWriter(
                new OutputStreamWriter(
                    new FileOutputStream(outputFile.toFile()),
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
                reportEntries.add(new ExportReportEntry("SEQUENCE", expectedRowCount, finalCap, rowCount, false, false, null));
            }
        }
    }

    private static String resolveSchema(DbConfig dbConfig, DatabaseDialect dialect) {
        if (dbConfig.schema != null && !dbConfig.schema.isBlank()) {
            return dbConfig.schema;
        }
        return switch (dialect) {
            case ORACLE -> dbConfig.user != null ? dbConfig.user.toUpperCase(Locale.ROOT) : null;
            case SQLSERVER -> "dbo";
            case MYSQL -> null;
        };
    }

    private static void validateUniqueFileNames(List<SourceTableRef> tableRefs) {
        Map<String, List<SourceTableRef>> byFileStem = new HashMap<>();
        for (SourceTableRef tableRef : tableRefs) {
            byFileStem.computeIfAbsent(tableRef.getFileStem(), ignored -> new ArrayList<>()).add(tableRef);
        }

        List<String> collisions = byFileStem.entrySet().stream()
            .filter(entry -> entry.getValue().size() > 1)
            .map(entry -> entry.getKey())
            .sorted()
            .toList();

        if (!collisions.isEmpty()) {
            throw new IllegalStateException(
                "Multiple source tables would map to the same CSV file name: " + collisions
                    + ". Narrow db_config_source.schema or rename the target mapping."
            );
        }
    }

    private static String buildQualifiedTableName(Connection conn, SourceTableRef table) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        String quote = normalizeQuote(metaData.getIdentifierQuoteString());
        List<String> parts = new ArrayList<>();
        if (table.getCatalog() != null && !table.getCatalog().isBlank()) {
            parts.add(quoteIdentifier(table.getCatalog(), quote));
        }
        if (table.getSchema() != null && !table.getSchema().isBlank()) {
            parts.add(quoteIdentifier(table.getSchema(), quote));
        }
        parts.add(quoteIdentifier(table.getTableName(), quote));
        return String.join(".", parts);
    }

    private static String normalizeQuote(String quote) {
        if (quote == null) {
            return "\"";
        }
        String trimmed = quote.trim();
        return trimmed.isEmpty() ? "\"" : trimmed;
    }

    private static String quoteIdentifier(String identifier, String quote) {
        String escaped = identifier.replace(quote, quote + quote);
        return quote + escaped + quote;
    }

    private static boolean isLightModeEnabled(LightModeConfig lightMode) {
        return lightMode != null
            && lightMode.enabled
            && lightMode.cod_acc != null
            && !lightMode.cod_acc.isBlank();
    }

    private static boolean shouldProduceLightModeReport(LightModeConfig lightMode) {
        return isLightModeEnabled(lightMode) && lightMode.report_enabled;
    }

    private static boolean isDryRun(LightModeConfig lightMode) {
        return isLightModeEnabled(lightMode) && lightMode.dry_run;
    }

    private static void bindLightModeParameters(
        PreparedStatement stmt,
        LightExportPlanner.ExportPlan exportPlan,
        LightModeConfig lightMode,
        DatabaseDialect sourceDialect
        ) throws SQLException {
        if (!exportPlan.codAccFiltered()) {
            return;
        }
        for (int index = 1; index <= exportPlan.parameterCount(); index++) {
            stmt.setString(index, normalizeCodAcc(lightMode.cod_acc, sourceDialect));
        }
    }

    private static long resolveFinalCap(
        long expectedRowCount,
        LightExportPlanner.ExportPlan exportPlan,
        LightModeConfig lightMode
    ) {
        if (!isLightModeEnabled(lightMode)) {
            return expectedRowCount;
        }
        Integer rowLimit = exportPlan.rowLimit();
        return rowLimit == null ? expectedRowCount : Math.min(expectedRowCount, rowLimit);
    }

    private static Integer resolveGlobalRowLimit(LightModeConfig lightMode) {
        if (lightMode == null || lightMode.max_rows_per_table <= 0) {
            return null;
        }
        return lightMode.max_rows_per_table;
    }

    private static long countRowsForPlan(
        Connection conn,
        LightExportPlanner.ExportPlan exportPlan,
        LightModeConfig lightMode,
        DatabaseDialect sourceDialect
    ) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(exportPlan.countSql())) {
            bindLightModeParameters(stmt, exportPlan, lightMode, sourceDialect);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static void logLightModeReportLine(String tableName, long expectedRowCount, long finalCap, LightModeConfig lightMode) {
        if (!shouldProduceLightModeReport(lightMode)) {
            return;
        }
        String dryRunTag = isDryRun(lightMode) ? " [dry-run]" : "";
        System.out.println(String.format(
            "[LIGHT]%s %s expected_before_cap=%d final_cap=%d",
            dryRunTag,
            tableName,
            expectedRowCount,
            finalCap
        ));
    }

    private static void writeLightModeReport(
        Path outputDir,
        LightModeConfig lightMode,
        List<ExportReportEntry> reportEntries
    ) throws IOException {
        if (!shouldProduceLightModeReport(lightMode)) {
            return;
        }
        List<ExportReportEntry> sortedEntries = reportEntries.stream()
            .sorted(Comparator.comparing(ExportReportEntry::tableName))
            .toList();
        Path reportPath = outputDir.resolve(lightMode.report_file);
        Files.createDirectories(reportPath.getParent());
        try (CSVWriter writer = new CSVWriter(Files.newBufferedWriter(reportPath, StandardCharsets.UTF_8))) {
            writer.writeNext(new String[] {
                "table_name",
                "expected_before_cap",
                "final_cap",
                "exported_rows",
                "dry_run",
                "cod_acc_filtered",
                "error"
            }, false);
            for (ExportReportEntry entry : sortedEntries) {
                writer.writeNext(new String[] {
                    entry.tableName(),
                    String.valueOf(entry.expectedBeforeCap()),
                    String.valueOf(entry.finalCap()),
                    String.valueOf(entry.exportedRows()),
                    String.valueOf(entry.dryRun()),
                    String.valueOf(entry.codAccFiltered()),
                    entry.error() == null ? "" : entry.error()
                }, false);
            }
        }
    }

    private static String normalizeCodAcc(String codAcc, DatabaseDialect sourceDialect) {
        return sourceDialect == DatabaseDialect.ORACLE
            ? codAcc.toUpperCase(Locale.ROOT)
            : codAcc;
    }

    private record ExportReportEntry(
        String tableName,
        long expectedBeforeCap,
        long finalCap,
        long exportedRows,
        boolean dryRun,
        boolean codAccFiltered,
        String error
    ) {
        private static ExportReportEntry error(String tableName, String error) {
            return new ExportReportEntry(tableName, 0, 0, 0, false, false, error);
        }
    }
}
