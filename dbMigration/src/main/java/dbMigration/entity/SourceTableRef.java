package dbMigration.entity;

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
        return tableName;
    }
}
