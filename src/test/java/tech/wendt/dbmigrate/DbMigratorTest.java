package tech.wendt.dbmigrate;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.wendt.dbmigrate.impl.ResourceMigrationLoader;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class DbMigratorTest {
    private static JdbcDataSource  dataSource;
    private static DbMigrator underTest;


    @BeforeAll
    static void init() throws Exception{
        dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:./db/test");
        dataSource.setUser("sa");
        dataSource.setPassword("sa");
    }

    @Test
    void migrateAll_rollbackAll() throws Exception {
        underTest = new DbMigrator("test_migration", dataSource, new ResourceMigrationLoader("migration_multiple_updown"));
        underTest.rollbackAll();
        underTest.migrateAll();
        Statement stmt = null;
        String query = "select * from test_migration";
        try(Connection connection = dataSource.getConnection()){
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            List<String> result = new ArrayList<>();
            while(rs.next()){
                int order = rs.getInt("order");
                String name = rs.getString("name");
                result.add(order + "__" + name);
            }
            result = result.stream().sorted(Comparator.naturalOrder()).collect(Collectors.toList());
            assertThat(result.size()).isEqualTo(3);
            assertThat(result.get(0)).isEqualTo("11__test11");
            assertThat(result.get(1)).isEqualTo("1__test1");
            assertThat(result.get(2)).isEqualTo("2__test2");
            List<String> createdTables = new ArrayList<>();
            createdTables.add("t_test1");
            createdTables.add("t_test2");
            createdTables.add("t_test3");
            assertTablesExist(createdTables, connection);
        }
        underTest.rollbackAll();
        try(Connection connection = dataSource.getConnection()){
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while(rs.next()){
                fail("Should not contain anything after rollback");
            }
        }
    }

    private void assertTablesExist(List<String> createdTables, Connection connection) throws Exception {
        for(String table: createdTables){
            ResultSet tables = connection.getMetaData().getTables(null, null, table, null);
            while (tables.next()){
                tables.findColumn("id");
                tables.findColumn("value");
            }
        }
    }
}