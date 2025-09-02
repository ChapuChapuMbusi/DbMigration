package dbMigration.entity;

import java.util.List;

public class Index {

    private String name;
    private boolean unique;
    List<String> columns;
    private String referredTable;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public String getReferredTable(){
        return referredTable;
    }

    public void setReferredTable(String referredTable){
        this.referredTable = referredTable;
    }

    
}
