package tech.wendt.dbmigrate;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.misc.TransactionManager;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.DatabaseTableConfig;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public class Migration implements MigrationInfo {
    private String name;
    private int order;
    private String upSql;
    private String downSql;

    public Migration(String name, int order, String upSql) {
        this(name, order, upSql, null);
    }

    public Migration(String name, int order, String upSql, String downSql) {
        this.name = name;
        this.order = order;
        this.upSql = upSql;
        this.downSql = downSql;
    }

    public void up(ConnectionSource connectionSource) throws SQLException {
            executeTransaction(connectionSource, upSql);
    }

    private static void executeTransaction(ConnectionSource connectionSource, String sql) throws SQLException {
        Dao<MigrationEntry, ?> dao = DaoManager.createDao(connectionSource, MigrationEntry.class);
        TransactionManager.callInTransaction(connectionSource, () -> {
            String[] statements = sql.split(";");
            for (String statement : statements) {
                dao.executeRaw(statement.trim());
            }
            return null;
        });
    }

    public void down(ConnectionSource connectionSource) throws SQLException {
        if (downSql != null) {
                executeTransaction(connectionSource, downSql);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getOrder() {
        return order;
    }

    String getUpSql() {
        return upSql;
    }

    String getDownSql() {
        return downSql;
    }
}
