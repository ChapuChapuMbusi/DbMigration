package dbMigration.config;

import java.util.LinkedHashSet;
import java.util.Set;

public class Config {

    public DbConfig db_config_target;
    public DbConfig db_config_source;
    public LightModeConfig light_mode;
    public String data_dir;
    public int max_workers;
    public Set<String> include_tables = new LinkedHashSet<>();

}
