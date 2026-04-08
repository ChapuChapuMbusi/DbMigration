package dbMigration.config;

import java.util.HashMap;
import java.util.Map;

public class LightModeConfig {

    public boolean enabled;
    public boolean dry_run;
    public boolean report_enabled;
    public String report_file;
    public String cod_acc;
    public int max_rows_per_table;
    public int relation_search_depth;
    public Map<String, String> manual_filters = new HashMap<>();
    public Map<String, String> manual_order_by = new HashMap<>();
}
