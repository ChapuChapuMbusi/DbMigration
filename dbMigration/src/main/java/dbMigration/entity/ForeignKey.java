package dbMigration.entity;

import java.util.List;

public class ForeignKey {

    String fkName;
    List<String> fkColumns;
    String fkTable;
    List<String> fkReferenceColumns;
    short updateRule;
    short deleteRule;


    public String getFkName() {
        return fkName;
    }
    public void setFkName(String fkName) {
        this.fkName = fkName;
    }
    public List<String> getFkColumns() {
        return fkColumns;
    }
    public void setFkColumns(List<String> fkColumns) {
        this.fkColumns = fkColumns;
    }
    public String getFkTable() {
        return fkTable;
    }
    public void setFkTable(String fkTable) {
        this.fkTable = fkTable;
    }
    public short getUpdateRule() {
        return updateRule;
    }
    public List<String> getFkReferenceColumns() {
        return fkReferenceColumns;
    }
    public void setFkReferenceColumns(List<String> fkReferenceColumns) {
        this.fkReferenceColumns = fkReferenceColumns;
    }
    public void setUpdateRule(short updateRule) {
        this.updateRule = updateRule;
    }
    public short getDeleteRule() {
        return deleteRule;
    }
    public void setDeleteRule(short deleteRule) {
        this.deleteRule = deleteRule;
    }

    
}
