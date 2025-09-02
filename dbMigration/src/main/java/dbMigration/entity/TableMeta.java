package dbMigration.entity;

import java.util.List;

public class TableMeta {

    private List<ForeignKey> fK; 
    private List<Index> i;

    public TableMeta(){}

    public TableMeta(List<ForeignKey> fK, List<Index> i) {
        this.fK = fK;
        this.i = i;
    }

    public List<ForeignKey> getfK() {
        return fK;
    }

    public void setfK(List<ForeignKey> fK) {
        this.fK = fK;
    }

    public List<Index> getI() {
        return i;
    }

    public void setI(List<Index> i) {
        this.i = i;
    }

    
}
