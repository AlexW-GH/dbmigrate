package tech.wendt.dbmigrate;

import java.io.IOException;
import java.util.List;

public interface MigrationLoader {
    List<Migration> loadMigrations() throws IOException;
}
