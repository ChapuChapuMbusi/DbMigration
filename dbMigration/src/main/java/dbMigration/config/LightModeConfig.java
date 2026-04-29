package dbMigration.config;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class LightModeConfig {

    public boolean enabled;
    public boolean dry_run;
    public boolean report_enabled;
    public String report_file;
    public String cod_acc;
    public int max_rows_per_table;
    public int relation_search_depth;
    public Set<String> uncapped_tables = new LinkedHashSet<>();
    public Set<String> replace_filtered_tables = new LinkedHashSet<>();
    public Map<String, String> manual_filters = new HashMap<>();
    public Map<String, String> manual_delete_filters = new HashMap<>();
    public Map<String, String> manual_order_by = new HashMap<>();
}
