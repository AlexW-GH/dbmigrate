package tech.wendt.dbmigrate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Migration implements MigrationInfo {
    private String name;
    private int order;
    private String upSql;
    private String downSql;

    public Migration(String name, int order, String upSql){
        this(name, order, upSql, null);
    }

    public Migration(String name, int order, String upSql, String downSql){
        this.name = name;
        this.order = order;
        this.upSql = upSql;
        this.downSql = downSql;
    }

    public void up(DataSource dataSource) throws SQLException{
        Connection connection = dataSource.getConnection();
        executeTransaction(connection, upSql);
    }

    public void down(DataSource dataSource) throws SQLException{
        if(downSql != null){
            Connection connection = dataSource.getConnection();
            executeTransaction(connection, downSql);
        }
    }

    private static void executeTransaction(Connection connection, String sql) throws SQLException {
        try{
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw new SQLException("Migration could not be executed", e);
        } finally {
            connection.setAutoCommit(true);
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
