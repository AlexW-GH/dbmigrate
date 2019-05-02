package tech.wendt.dbmigrate;

public interface MigrationInfo {
    int getOrder();

    String getName();
}
