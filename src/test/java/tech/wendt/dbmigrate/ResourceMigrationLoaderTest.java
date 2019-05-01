package tech.wendt.dbmigrate;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tech.wendt.dbmigrate.impl.ResourceMigrationLoader;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ResourceMigrationLoaderTest {

    private ResourceMigrationLoader underTest;

    @Test
    @DisplayName("Can load single Migration with up and down script")
    void loadMigrations_single_updown() throws Exception {
        String upSql = FileUtil.getFileContent("/migration_single_updown/01__test_up.sql");
        String downSql = FileUtil.getFileContent("/migration_single_updown/01__test_down.sql");
        underTest = new ResourceMigrationLoader("/migration_single_updown");

        List<Migration> migrations = underTest.loadMigrations();

        assertThat(migrations).hasSize(1);
        assertThat(migrations.get(0).getName()).isEqualTo("test");
        assertThat(migrations.get(0).getOrder()).isEqualTo(1);
        assertThat(migrations.get(0).getUpSql()).isEqualTo(upSql);
        assertThat(migrations.get(0).getDownSql()).isEqualTo(downSql);
    }

    @Test
    @DisplayName("Can load single Migration with only up script")
    void loadMigrations_single_up() throws Exception {
        String upSql = FileUtil.getFileContent("/migration_single_up/01__test_up.sql");
        underTest = new ResourceMigrationLoader("/migration_single_up");

        List<Migration> migrations = underTest.loadMigrations();

        assertThat(migrations).hasSize(1);
        assertThat(migrations.get(0).getName()).isEqualTo("test");
        assertThat(migrations.get(0).getOrder()).isEqualTo(1);
        assertThat(migrations.get(0).getUpSql()).isEqualTo(upSql);
        assertThat(migrations.get(0).getDownSql()).isNull();
    }

    @Test
    @DisplayName("Fails to load single Migration with only down script")
    void loadMigrations_single_down() throws Exception {
        String upSql = FileUtil.getFileContent("/migration_single_down/01__test_down.sql");
        underTest = new ResourceMigrationLoader("/migration_single_down");

        IOException thrown =
                assertThrows(IOException.class,
                        () -> underTest.loadMigrations(),
                        "Expected loadMigrations() to throw, because no valid migration was found");

        assertThat(thrown.getMessage()).isEqualTo("No migrations found");
    }

    @Test
    @DisplayName("Can load multiple Migrations with only up script in correct Order")
    void loadMigrations_multiple_up() throws Exception {
        String upSql1 = FileUtil.getFileContent("/migration_multiple_up/1__test1_up.sql");
        String upSql2 = FileUtil.getFileContent("/migration_multiple_up/02__test2_up.sql");
        String upSql3 = FileUtil.getFileContent("/migration_multiple_up/11__test11_up.sql");
        underTest = new ResourceMigrationLoader("/migration_multiple_up");

        List<Migration> migrations = underTest.loadMigrations();

        assertThat(migrations).hasSize(3);

        assertThat(migrations.get(0).getName()).isEqualTo("test1");
        assertThat(migrations.get(0).getOrder()).isEqualTo(1);
        assertThat(migrations.get(0).getUpSql()).isEqualTo(upSql1);
        assertThat(migrations.get(0).getDownSql()).isNull();
        assertThat(migrations.get(1).getName()).isEqualTo("test2");
        assertThat(migrations.get(1).getOrder()).isEqualTo(2);
        assertThat(migrations.get(1).getUpSql()).isEqualTo(upSql2);
        assertThat(migrations.get(1).getDownSql()).isNull();
        assertThat(migrations.get(2).getName()).isEqualTo("test11");
        assertThat(migrations.get(2).getOrder()).isEqualTo(11);
        assertThat(migrations.get(2).getUpSql()).isEqualTo(upSql3);
        assertThat(migrations.get(2).getDownSql()).isNull();
    }

    @Test
    @DisplayName("Can load multiple Migrations with up and down scripts in correct Order")
    void loadMigrations_multiple_updown() throws Exception {
        String upSql1 = FileUtil.getFileContent("/migration_multiple_updown/1__test1_up.sql");
        String upSql2 = FileUtil.getFileContent("/migration_multiple_updown/02__test2_up.sql");
        String upSql3 = FileUtil.getFileContent("/migration_multiple_updown/11__test11_up.sql");
        String downSql1 = FileUtil.getFileContent("/migration_multiple_updown/1__test1_down.sql");
        String downSql2 = FileUtil.getFileContent("/migration_multiple_updown/02__test2_down.sql");
        String downSql3 = FileUtil.getFileContent("/migration_multiple_updown/11__test11_down.sql");
        underTest = new ResourceMigrationLoader("/migration_multiple_updown");

        List<Migration> migrations = underTest.loadMigrations();

        assertThat(migrations).hasSize(3);

        assertThat(migrations.get(0).getName()).isEqualTo("test1");
        assertThat(migrations.get(0).getOrder()).isEqualTo(1);
        assertThat(migrations.get(0).getUpSql()).isEqualTo(upSql1);
        assertThat(migrations.get(0).getDownSql()).isEqualTo(downSql1);
        assertThat(migrations.get(1).getName()).isEqualTo("test2");
        assertThat(migrations.get(1).getOrder()).isEqualTo(2);
        assertThat(migrations.get(1).getUpSql()).isEqualTo(upSql2);
        assertThat(migrations.get(1).getDownSql()).isEqualTo(downSql2);
        assertThat(migrations.get(2).getName()).isEqualTo("test11");
        assertThat(migrations.get(2).getOrder()).isEqualTo(11);
        assertThat(migrations.get(2).getUpSql()).isEqualTo(upSql3);
        assertThat(migrations.get(2).getDownSql()).isEqualTo(downSql3);
    }

    @Test
    @DisplayName("Fails to load Migration with mismatched down script")
    void loadMigrations_multiple_name_mismatch() throws Exception {
        underTest = new ResourceMigrationLoader("/migration_multiple_name_mismatch");

        IOException thrown = assertThrows(IOException.class,
                () -> underTest.loadMigrations(),
               "Expected loadMigrations() to throw, because a mismatched down script was found");

        assertThat(thrown.getMessage()).isEqualTo("Down script without matching up script found");
    }

    @Test
    @DisplayName("Fails to load Migration with wrong regex")
    void loadMigrations_name_notregex() throws Exception {
        underTest = new ResourceMigrationLoader("/migration_name_notregex");

        IOException thrown = assertThrows(IOException.class,
                () -> underTest.loadMigrations(),
                "Expected loadMigrations() to throw, because a mismatched down script was found");

        assertThat(thrown.getMessage()).isEqualTo("migration file 1_test1_up.sql does not match regex \\d+_{2}[^.]+_(down|up)\\.sql");
    }

    @Test
    @DisplayName("Can not create ResourceMigrationLoader with null as argument")
    void constructor_null() throws Exception {
        assertThrows(NullPointerException.class,
                () -> new ResourceMigrationLoader(null),
                "Expected ResourceMigrationLoader constructor to throw, because null is not supported");
    }

    @Test
    @DisplayName("Can not create ResourceMigrationLoader with empty string as argument")
    void constructor_emptystring() throws Exception {
        IOException thrown = assertThrows(IOException.class,
                () -> new ResourceMigrationLoader("/wrong_folder"),
                "Expected ResourceMigrationLoader constructor to throw, because null is not supported");

        assertThat(thrown.getMessage()).isEqualTo("Resource directory /wrong_folder does not exist");
    }

    @Test
    @DisplayName("Can not create ResourceMigrationLoader with empty string as argument")
    void constructor_missingSeperator() throws Exception {
        underTest = new ResourceMigrationLoader("migration_single_updown");

        List<Migration> migrations = underTest.loadMigrations();

        assertThat(migrations).hasSize(1);
    }

    @Test
    @DisplayName("Can not create ResourceMigrationLoader with file as argument")
    void constructor_notDirectory() throws Exception {

        IOException thrown = assertThrows(IOException.class,
                () -> new ResourceMigrationLoader("/notDir.sql"),
                "Expected ResourceMigrationLoader constructor to throw, because null is not supported");

        assertThat(thrown.getMessage()).isEqualTo("/notDir.sql is not a directory");
    }
}