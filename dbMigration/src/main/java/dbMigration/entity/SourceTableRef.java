package dbMigration.entity;

import java.util.Locale;

public class SourceTableRef {

    private final String catalog;
    private final String schema;
    private final String tableName;

    public SourceTableRef(String catalog, String schema, String tableName) {
        this.catalog = catalog;
        this.schema = schema;
        this.tableName = tableName;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getSchema() {
        return schema;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFileStem() {
        return normalizeTableName(tableName);
    }

    public String getLogicalTableName() {
        return normalizeTableName(tableName);
    }

    private static String normalizeTableName(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }
}
