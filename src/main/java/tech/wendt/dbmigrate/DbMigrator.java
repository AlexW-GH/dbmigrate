package tech.wendt.dbmigrate;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.field.DatabaseFieldConfig;
import com.j256.ormlite.jdbc.DataSourceConnectionSource;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.DatabaseTableConfig;
import com.j256.ormlite.table.TableUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DbMigrator {

    private String migrationTableName;
    private DataSource dataSource;
    private MigrationLoader migrationLoader;
    private Dao<MigrationEntry, Integer> migrationDao;

    public DbMigrator(String migrationTableName, DataSource dataSource, MigrationLoader migrationLoader)
            throws MigrationException {
        this.migrationTableName = migrationTableName;
        this.migrationLoader = migrationLoader;
        this.dataSource = dataSource;
        init();
    }

    public void migrateAll() throws MigrationException {
        try{
            List<Migration> migrations = migrationLoader.loadMigrations();
            migrate(migrations);
        } catch (SQLException | IOException e) {
            throw new MigrationException("Could not execute migration", e);
        }
    }

    public void migrate(int migrationNumber) throws MigrationException {
        try{
            List<Migration> migrations = migrationLoader.loadMigrations().stream()
                    .filter(migration -> migration.getOrder() <= migrationNumber)
                    .collect(Collectors.toList());
            migrate(migrations);
        } catch (SQLException | IOException e) {
            throw new MigrationException("Could not execute migration", e);
        }
    }

    public void rollbackAll() throws MigrationException {
        try{
            List<Migration> migrations = migrationLoader.loadMigrations();
            rollback(migrations);
        } catch (SQLException | IOException e) {
            throw new MigrationException("Could not execute rollback", e);
        }

    }
   public void rollback(int migrationNumber) throws MigrationException {
        try{
            List<Migration> migrations = migrationLoader.loadMigrations().stream()
                    .filter(migration -> migration.getOrder() >= migrationNumber)
                    .collect(Collectors.toList());
            rollback(migrations);
        } catch (SQLException | IOException e) {
            throw new MigrationException("Could not execute rollback", e);
        }
   }

    private void migrate(List<Migration> migrations) throws SQLException, MigrationException{
        removeCompletedMigrations(migrations);
        List<Migration> sortedMigrations = migrations.stream()
                .sorted(Comparator.comparingInt(Migration::getOrder))
                .collect(Collectors.toList());

        for(Migration migration: sortedMigrations){
            MigrationEntry migrationEntry = new MigrationEntry();
            migrationEntry.setName(migration.getName());
            migrationEntry.setOrder(migration.getOrder());
            migration.up(dataSource);
            migrationDao.create(migrationEntry);
        }
    }

    private void rollback(List<Migration> migrations) throws SQLException, MigrationException{
        List<MigrationEntry> migrationEntries = retrieveMigrationsToRollback(migrations);
        List<Migration> sortedMigrations = migrations.stream()
                .sorted((left, right) -> Integer.compare(right.getOrder(), left.getOrder()))
                .collect(Collectors.toList());

        for(Migration migration: sortedMigrations){
            Optional<MigrationEntry> migrationEntry = migrationEntries.stream()
                    .filter(entry -> entry.getOrder() == migration.getOrder() && entry.getName().equals(migration.getName()))
                    .findAny();
            if(migrationEntry.isPresent()){
                migration.down(dataSource);
                migrationDao.delete(migrationEntry.get());
            }
        }
    }

    private void checkNoIllegalMigrations(List<? extends MigrationInfo> migrations, int lastMigrationNumber)
            throws MigrationException {
        List<MigrationInfo> illegalMigrations = migrations.stream()
                .filter(migration -> migration.getOrder() <= lastMigrationNumber)
                .collect(Collectors.toList());
        if(!illegalMigrations.isEmpty()){
            String illegalMigrationsString = illegalMigrations.stream()
                    .map(migration ->
                            String.format("%s__%s", migration.getOrder(), migration.getName()))
                    .reduce(String::concat)
                    .orElse("This should not happen");
            throw new MigrationException(
                    String.format("There are migrations that are not in the database, " +
                            "but smaller then the most recent migration number: %s", illegalMigrationsString));
        }
    }


    private int removeCompletedMigrations(List<Migration> migrations) throws SQLException, MigrationException {
        List<MigrationEntry> existingMigrations = migrationDao.queryForAll()
                .stream()
                .sorted(Comparator.comparingInt(MigrationEntry::getOrder))
                .collect(Collectors.toList());

        int lastMigrationNumber;
        if(!existingMigrations.isEmpty()){
            lastMigrationNumber = existingMigrations.get(existingMigrations.size() - 1).getOrder();
        } else {
            lastMigrationNumber = -1;
        }
        for(MigrationEntry entry : existingMigrations){
            migrations.removeIf(migration ->
                    migration.getOrder() == entry.getOrder() && migration.getName().equals(entry.getName()));
        }
        checkNoIllegalMigrations(migrations, lastMigrationNumber);
        return lastMigrationNumber;
    }

    private List<MigrationEntry> retrieveMigrationsToRollback(List<Migration> migrations) throws SQLException, MigrationException {
        List<MigrationEntry> existingMigrations = migrationDao.queryForAll()
                .stream()
                .sorted(Comparator.comparingInt(MigrationEntry::getOrder))
                .collect(Collectors.toList());
        int lastMigrationNumber;
        if(!existingMigrations.isEmpty()){
            lastMigrationNumber = existingMigrations.get(existingMigrations.size() - 1).getOrder();
        } else {
            lastMigrationNumber = -1;
        }
        List<MigrationEntry> toRemove = new ArrayList<>();
        for(MigrationEntry entry : existingMigrations){
            for(Migration migration: migrations){
                if(migration.getOrder() == entry.getOrder()
                        && migration.getName().equals(entry.getName())
                        && entry.getOrder() <= lastMigrationNumber){
                    toRemove.add(entry);
                    break;
                }
            }
        }
        existingMigrations.removeAll(toRemove);
        checkNoIllegalMigrations(existingMigrations, lastMigrationNumber);

        if(existingMigrations.isEmpty()){
            return toRemove;
        } else {
            throw new MigrationException("");
        }
    }

    private void init() throws MigrationException {
        try(
                Connection connection = dataSource.getConnection();
                ConnectionSource connectionSource =
                        new DataSourceConnectionSource(dataSource, connection.getMetaData().getURL())
            ){
            List<DatabaseFieldConfig> fields = new ArrayList<>();
            DatabaseFieldConfig idField = new DatabaseFieldConfig("id");
            idField.setGeneratedId(true);
            idField.setCanBeNull(false);
            fields.add(idField);

            DatabaseFieldConfig orderField = new DatabaseFieldConfig("order");
            orderField.setCanBeNull(false);
            orderField.setUnique(true);
            fields.add(orderField);

            DatabaseFieldConfig nameField = new DatabaseFieldConfig("name");
            nameField.setCanBeNull(false);
            fields.add(nameField);

            DatabaseTableConfig<MigrationEntry> tableConfig =
                    new DatabaseTableConfig<>(MigrationEntry.class, migrationTableName, fields);
            tableConfig.setTableName(migrationTableName);

            TableUtils.createTableIfNotExists(connectionSource, tableConfig);
            migrationDao = DaoManager.createDao(connectionSource, tableConfig);
        } catch (SQLException | IOException e) {
            throw new MigrationException("Could not initialize Migration", e);
        }
    }
}
