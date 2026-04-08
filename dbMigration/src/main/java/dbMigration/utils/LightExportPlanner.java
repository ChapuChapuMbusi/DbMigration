package dbMigration.utils;

import dbMigration.config.DatabaseDialect;
import dbMigration.config.LightModeConfig;
import dbMigration.entity.SourceTableRef;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class LightExportPlanner {

    private static final List<String> AGREEMENT_COLUMNS = List.of("COD_ACC", "CODICE_ACCORDO");
    private static final String COD_ACC_PLACEHOLDER = ":cod_acc";

    private final DatabaseDialect dialect;
    private final LightModeConfig config;
    private final String identifierQuote;
    private final Map<String, TableInfo> tablesByName;
    private final Map<String, ExportPlan> planCache = new ConcurrentHashMap<>();

    private LightExportPlanner(
            DatabaseDialect dialect,
            LightModeConfig config,
            String identifierQuote,
            Map<String, TableInfo> tablesByName) {
        this.dialect = dialect;
        this.config = config;
        this.identifierQuote = identifierQuote;
        this.tablesByName = tablesByName;
    }

    public static LightExportPlanner create(
            Connection conn,
            Collection<SourceTableRef> tableRefs,
            DatabaseDialect dialect,
            LightModeConfig config) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        String quote = normalizeQuote(metaData.getIdentifierQuoteString());
        Map<String, TableInfo> tablesByName = new HashMap<>();

        for (SourceTableRef tableRef : tableRefs) {
            TableInfo tableInfo = new TableInfo(tableRef, loadColumns(metaData, tableRef));
            tablesByName.put(normalizeName(tableRef.getTableName()), tableInfo);
        }

        for (TableInfo tableInfo : tablesByName.values()) {
            loadRelationships(metaData, tableInfo, tablesByName);
        }

        return new LightExportPlanner(dialect, config, quote, tablesByName);
    }

    public ExportPlan plan(SourceTableRef tableRef) {
        String tableKey = normalizeName(tableRef.getTableName());
        return planCache.computeIfAbsent(tableKey, ignored -> buildPlan(tableKey));
    }

    private ExportPlan buildPlan(String tableKey) {
        TableInfo tableInfo = tablesByName.get(tableKey);
        if (tableInfo == null) {
            throw new IllegalArgumentException("Unknown table for light export planning: " + tableKey);
        }

        String orderBy = resolveOrderBy(tableInfo);
        ManualFilter manualFilter = resolveManualFilter(tableKey);
        if (manualFilter != null) {
            return new ExportPlan(
                    buildSelectSql(tableInfo, manualFilter.sql(), orderBy),
                    buildCountSql(tableInfo, manualFilter.sql()),
                    manualFilter.parameterCount(),
                    true);
        }

        List<RelationshipEdge> path = findPathToAgreement(tableKey);
        if (path == null) {
            return new ExportPlan(buildSelectSql(tableInfo, null, orderBy), buildCountSql(tableInfo, null), 0, false);
        }

        AliasCounter aliasCounter = new AliasCounter(1);
        String predicate = buildPredicate("t0", tableKey, path, 0, aliasCounter);
        return new ExportPlan(
                buildSelectSql(tableInfo, predicate, orderBy),
                buildCountSql(tableInfo, predicate),
                countParameters(path, tableKey),
                true);
    }

    private String buildSelectSql(TableInfo tableInfo, String predicate, String orderByClause) {
        String qualifiedTable = tableInfo != null ? qualify(tableInfo.tableRef()) : null;
        return switch (dialect) {
            case MYSQL -> buildMySqlLikeSelect(qualifiedTable, predicate, orderByClause);
            case ORACLE -> buildOracleSelect(qualifiedTable, predicate, orderByClause);
            case SQLSERVER -> buildSqlServerSelect(qualifiedTable, predicate, orderByClause);
        };
    }

    private String buildCountSql(TableInfo tableInfo, String predicate) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ")
                .append(qualify(tableInfo.tableRef()))
                .append(" t0");
        if (predicate != null && !predicate.isBlank()) {
            sql.append(" WHERE ").append(predicate);
        }
        return sql.toString();
    }

    private String buildMySqlLikeSelect(String qualifiedTable, String predicate, String orderByClause) {
        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(qualifiedTable).append(" t0");
        if (predicate != null && !predicate.isBlank()) {
            sql.append(" WHERE ").append(predicate);
        }
        if (orderByClause != null && !orderByClause.isBlank()) {
            sql.append(" ORDER BY ").append(orderByClause);
        }
        sql.append(" LIMIT ").append(config.max_rows_per_table);
        return sql.toString();
    }

    private String buildOracleSelect(String qualifiedTable, String predicate, String orderByClause) {
        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(qualifiedTable).append(" t0");
        if (predicate != null && !predicate.isBlank()) {
            sql.append(" WHERE ").append(predicate);
        }
        if (orderByClause != null && !orderByClause.isBlank()) {
            sql.append(" ORDER BY ").append(orderByClause);
        }
        sql.append(" FETCH FIRST ").append(config.max_rows_per_table).append(" ROWS ONLY");
        return sql.toString();
    }

    private String buildSqlServerSelect(String qualifiedTable, String predicate, String orderByClause) {
        StringBuilder sql = new StringBuilder("SELECT TOP (")
                .append(config.max_rows_per_table)
                .append(") * FROM ")
                .append(qualifiedTable)
                .append(" t0");
        if (predicate != null && !predicate.isBlank()) {
            sql.append(" WHERE ").append(predicate);
        }
        if (orderByClause != null && !orderByClause.isBlank()) {
            sql.append(" ORDER BY ").append(orderByClause);
        }
        return sql.toString();
    }

    private int countParameters(List<RelationshipEdge> path, String startTableKey) {
        String currentTableKey = startTableKey;
        for (RelationshipEdge edge : path) {
            currentTableKey = edge.targetTableKey();
        }
        return findAgreementColumns(tablesByName.get(currentTableKey)).size();
    }

    private String buildPredicate(
            String alias,
            String currentTableKey,
            List<RelationshipEdge> path,
            int pathIndex,
            AliasCounter aliasCounter) {
        if (pathIndex >= path.size()) {
            List<String> agreementColumns = findAgreementColumns(tablesByName.get(currentTableKey));
            if (agreementColumns.isEmpty()) {
                throw new IllegalStateException("No agreement column found for table " + currentTableKey);
            }
            return agreementColumns.stream()
                    .map(column -> qualifyColumn(alias, column) + " = ?")
                    .reduce((left, right) -> left + " OR " + right)
                    .map(predicate -> agreementColumns.size() > 1 ? "(" + predicate + ")" : predicate)
                    .orElseThrow();
        }

        RelationshipEdge edge = path.get(pathIndex);
        TableInfo targetTable = tablesByName.get(edge.targetTableKey());
        String nextAlias = "t" + aliasCounter.next();
        String joinClause = buildJoinClause(alias, nextAlias, edge);
        String nestedPredicate = buildPredicate(nextAlias, edge.targetTableKey(), path, pathIndex + 1, aliasCounter);
        return "EXISTS (SELECT 1 FROM " + qualify(targetTable.tableRef()) + " " + nextAlias
                + " WHERE " + joinClause + " AND " + nestedPredicate + ")";
    }

    private String buildJoinClause(String currentAlias, String nextAlias, RelationshipEdge edge) {
        List<String> clauses = new ArrayList<>();
        for (int i = 0; i < edge.currentColumns().size(); i++) {
            clauses.add(qualifyColumn(currentAlias, edge.currentColumns().get(i))
                    + " = "
                    + qualifyColumn(nextAlias, edge.targetColumns().get(i)));
        }
        return String.join(" AND ", clauses);
    }

    private List<RelationshipEdge> findPathToAgreement(String startTableKey) {
        if (!findAgreementColumns(tablesByName.get(startTableKey)).isEmpty()) {
            return List.of();
        }

        record QueueItem(String tableKey, List<RelationshipEdge> path, int depth) {
        }

        Deque<QueueItem> queue = new ArrayDeque<>();
        Set<String> visited = new HashSet<>();
        queue.add(new QueueItem(startTableKey, List.of(), 0));
        visited.add(startTableKey);

        while (!queue.isEmpty()) {
            QueueItem current = queue.removeFirst();
            if (current.depth() >= config.relation_search_depth) {
                continue;
            }

            TableInfo tableInfo = tablesByName.get(current.tableKey());
            if (tableInfo == null) {
                continue;
            }

            for (RelationshipEdge edge : tableInfo.relationships()) {
                if (!visited.add(edge.targetTableKey())) {
                    continue;
                }
                List<RelationshipEdge> nextPath = new ArrayList<>(current.path());
                nextPath.add(edge);
                if (!findAgreementColumns(tablesByName.get(edge.targetTableKey())).isEmpty()) {
                    return nextPath;
                }
                queue.addLast(new QueueItem(edge.targetTableKey(), nextPath, current.depth() + 1));
            }
        }

        return null;
    }

    private static List<ColumnInfo> loadColumns(DatabaseMetaData metaData, SourceTableRef tableRef) throws SQLException {
        List<ColumnInfo> columns = new ArrayList<>();
        try (ResultSet rs = metaData.getColumns(
                tableRef.getCatalog(),
                tableRef.getSchema(),
                tableRef.getTableName(),
                null)) {
            while (rs.next()) {
                columns.add(new ColumnInfo(
                        rs.getInt("ORDINAL_POSITION"),
                        rs.getString("COLUMN_NAME"),
                        rs.getInt("DATA_TYPE")));
            }
        }
        columns.sort(Comparator.comparingInt(ColumnInfo::ordinalPosition));
        return columns;
    }

    private static void loadRelationships(
            DatabaseMetaData metaData,
            TableInfo tableInfo,
            Map<String, TableInfo> tablesByName) throws SQLException {
        Map<String, RelationshipParts> edges = new LinkedHashMap<>();

        try (ResultSet rs = metaData.getImportedKeys(
                tableInfo.tableRef().getCatalog(),
                tableInfo.tableRef().getSchema(),
                tableInfo.tableRef().getTableName())) {
            while (rs.next()) {
                String targetKey = normalizeName(rs.getString("PKTABLE_NAME"));
                if (!tablesByName.containsKey(targetKey)) {
                    continue;
                }
                String relationKey = "I:" + targetKey + ":" + normalizeRelationName(rs.getString("FK_NAME"));
                RelationshipParts parts = edges.computeIfAbsent(relationKey, ignored -> new RelationshipParts(targetKey));
                parts.add(rs.getShort("KEY_SEQ"), rs.getString("FKCOLUMN_NAME"), rs.getString("PKCOLUMN_NAME"));
            }
        }

        try (ResultSet rs = metaData.getExportedKeys(
                tableInfo.tableRef().getCatalog(),
                tableInfo.tableRef().getSchema(),
                tableInfo.tableRef().getTableName())) {
            while (rs.next()) {
                String targetKey = normalizeName(rs.getString("FKTABLE_NAME"));
                if (!tablesByName.containsKey(targetKey)) {
                    continue;
                }
                String relationKey = "E:" + targetKey + ":" + normalizeRelationName(rs.getString("FK_NAME"));
                RelationshipParts parts = edges.computeIfAbsent(relationKey, ignored -> new RelationshipParts(targetKey));
                parts.add(rs.getShort("KEY_SEQ"), rs.getString("PKCOLUMN_NAME"), rs.getString("FKCOLUMN_NAME"));
            }
        }

        tableInfo.relationships().addAll(
                edges.values().stream()
                        .map(RelationshipParts::toEdge)
                        .sorted(Comparator.comparing(RelationshipEdge::targetTableKey))
                        .toList());
        tableInfo.primaryKeyColumns().addAll(loadPrimaryKeyColumns(metaData, tableInfo.tableRef()));
        tableInfo.uniqueIndexes().addAll(loadUniqueIndexes(metaData, tableInfo.tableRef()));
    }

    private static List<String> loadPrimaryKeyColumns(DatabaseMetaData metaData, SourceTableRef tableRef) throws SQLException {
        Map<Short, String> bySeq = new HashMap<>();
        try (ResultSet rs = metaData.getPrimaryKeys(
                tableRef.getCatalog(),
                tableRef.getSchema(),
                tableRef.getTableName())) {
            while (rs.next()) {
                bySeq.put(rs.getShort("KEY_SEQ"), rs.getString("COLUMN_NAME"));
            }
        }
        return bySeq.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .toList();
    }

    private static List<List<String>> loadUniqueIndexes(DatabaseMetaData metaData, SourceTableRef tableRef) throws SQLException {
        Map<String, Map<Short, String>> indexes = new LinkedHashMap<>();
        try (ResultSet rs = metaData.getIndexInfo(
                tableRef.getCatalog(),
                tableRef.getSchema(),
                tableRef.getTableName(),
                true,
                false)) {
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                short ordinal = rs.getShort("ORDINAL_POSITION");
                if (indexName == null || columnName == null || ordinal <= 0) {
                    continue;
                }
                indexes.computeIfAbsent(indexName, ignored -> new LinkedHashMap<>()).put(ordinal, columnName);
            }
        }
        return indexes.values().stream()
                .map(columns -> columns.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(Map.Entry::getValue)
                        .toList())
                .filter(columns -> !columns.isEmpty())
                .toList();
    }

    private String qualify(SourceTableRef tableRef) {
        List<String> parts = new ArrayList<>();
        if (tableRef.getCatalog() != null && !tableRef.getCatalog().isBlank()) {
            parts.add(quoteIdentifier(tableRef.getCatalog()));
        }
        if (tableRef.getSchema() != null && !tableRef.getSchema().isBlank()) {
            parts.add(quoteIdentifier(tableRef.getSchema()));
        }
        parts.add(quoteIdentifier(tableRef.getTableName()));
        return String.join(".", parts);
    }

    private String qualifyColumn(String alias, String column) {
        return alias + "." + quoteIdentifier(column);
    }

    private String quoteIdentifier(String identifier) {
        String escaped = identifier.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + escaped + identifierQuote;
    }

    private static List<String> findAgreementColumns(TableInfo tableInfo) {
        if (tableInfo == null) {
            return List.of();
        }
        return tableInfo.columns().stream()
                .map(ColumnInfo::name)
                .filter(LightExportPlanner::isAgreementLikeColumn)
                .toList();
    }

    private static boolean isAgreementLikeColumn(String columnName) {
        String normalized = normalizeName(columnName);
        if (AGREEMENT_COLUMNS.contains(normalized)) {
            return true;
        }
        return normalized.startsWith("COD_ACC_") || normalized.startsWith("CODICE_ACCORDO_");
    }

    private String resolveOrderBy(TableInfo tableInfo) {
        String manualOrderBy = config.manual_order_by.get(normalizeName(tableInfo.tableRef().getTableName()));
        if (manualOrderBy != null && !manualOrderBy.isBlank()) {
            return manualOrderBy;
        }
        if (!tableInfo.primaryKeyColumns().isEmpty()) {
            return tableInfo.primaryKeyColumns().stream()
                    .map(column -> qualifyColumn("t0", column))
                    .reduce((left, right) -> left + ", " + right)
                    .orElse(null);
        }
        for (List<String> uniqueIndex : tableInfo.uniqueIndexes()) {
            if (!uniqueIndex.isEmpty()) {
                return uniqueIndex.stream()
                        .map(column -> qualifyColumn("t0", column))
                        .reduce((left, right) -> left + ", " + right)
                        .orElse(null);
            }
        }
        return tableInfo.columns().stream()
                .filter(column -> isSortableFallbackType(column.jdbcType()))
                .limit(3)
                .map(column -> qualifyColumn("t0", column.name()))
                .reduce((left, right) -> left + ", " + right)
                .orElse(null);
    }

    private ManualFilter resolveManualFilter(String tableKey) {
        String filter = config.manual_filters.get(tableKey);
        if (filter == null || filter.isBlank()) {
            return null;
        }
        int parameterCount = countOccurrences(filter, COD_ACC_PLACEHOLDER);
        String sql = filter.replace(COD_ACC_PLACEHOLDER, "?");
        return new ManualFilter(sql, parameterCount);
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

    private static boolean isSortableFallbackType(int jdbcType) {
        return switch (jdbcType) {
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB, Types.CLOB, Types.NCLOB,
                    Types.LONGVARCHAR, Types.LONGNVARCHAR, Types.SQLXML -> false;
            default -> true;
        };
    }

    private static String normalizeQuote(String quote) {
        if (quote == null) {
            return "\"";
        }
        String trimmed = quote.trim();
        return trimmed.isEmpty() ? "\"" : trimmed;
    }

    private static String normalizeRelationName(String relationName) {
        return relationName == null || relationName.isBlank() ? "__anonymous__" : relationName;
    }

    private static String normalizeName(String name) {
        return name == null ? "" : name.toUpperCase(Locale.ROOT);
    }

    public record ExportPlan(String sql, String countSql, int parameterCount, boolean codAccFiltered) {
    }

    private record RelationshipEdge(String targetTableKey, List<String> currentColumns, List<String> targetColumns) {
    }

    private static final class TableInfo {
        private final SourceTableRef tableRef;
        private final List<ColumnInfo> columns;
        private final Set<String> normalizedColumns;
        private final List<RelationshipEdge> relationships;
        private final List<String> primaryKeyColumns;
        private final List<List<String>> uniqueIndexes;

        private TableInfo(SourceTableRef tableRef, List<ColumnInfo> columns) {
            this.tableRef = tableRef;
            this.columns = columns;
            this.normalizedColumns = columns.stream()
                    .map(column -> normalizeName(column.name()))
                    .collect(HashSet::new, Set::add, Set::addAll);
            this.relationships = new ArrayList<>();
            this.primaryKeyColumns = new ArrayList<>();
            this.uniqueIndexes = new ArrayList<>();
        }

        private SourceTableRef tableRef() {
            return tableRef;
        }

        private List<ColumnInfo> columns() {
            return columns;
        }

        private List<RelationshipEdge> relationships() {
            return relationships;
        }

        private List<String> primaryKeyColumns() {
            return primaryKeyColumns;
        }

        private List<List<String>> uniqueIndexes() {
            return uniqueIndexes;
        }

        private boolean hasColumn(String name) {
            return normalizedColumns.contains(normalizeName(name));
        }
    }

    private record ColumnInfo(int ordinalPosition, String name, int jdbcType) {
    }

    private record ManualFilter(String sql, int parameterCount) {
    }

    private static final class RelationshipParts {
        private final String targetTableKey;
        private final Map<Short, ColumnPair> orderedColumns = new HashMap<>();

        private RelationshipParts(String targetTableKey) {
            this.targetTableKey = targetTableKey;
        }

        private void add(short keySeq, String currentColumn, String targetColumn) {
            orderedColumns.put(keySeq, new ColumnPair(currentColumn, targetColumn));
        }

        private RelationshipEdge toEdge() {
            List<Map.Entry<Short, ColumnPair>> pairs = orderedColumns.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .toList();
            List<String> currentColumns = new ArrayList<>();
            List<String> targetColumns = new ArrayList<>();
            for (Map.Entry<Short, ColumnPair> entry : pairs) {
                currentColumns.add(entry.getValue().currentColumn());
                targetColumns.add(entry.getValue().targetColumn());
            }
            return new RelationshipEdge(targetTableKey, currentColumns, targetColumns);
        }
    }

    private record ColumnPair(String currentColumn, String targetColumn) {
    }

    private static final class AliasCounter {
        private int value;

        private AliasCounter(int startingValue) {
            this.value = startingValue;
        }

        private int next() {
            return value++;
        }
    }
}
