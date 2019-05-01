package tech.wendt.dbmigrate;

import com.j256.ormlite.field.DatabaseField;

public class MigrationEntry implements MigrationInfo {

    @DatabaseField(generatedId = true)
    private int id = 0;

    @DatabaseField
    private int order = 0;

    @DatabaseField
    private String name = "";

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
