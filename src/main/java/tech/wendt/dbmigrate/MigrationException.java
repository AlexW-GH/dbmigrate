package tech.wendt.dbmigrate;

public class MigrationException extends Exception {
    public MigrationException(String msg) {
        super(msg);
    }

    public MigrationException(String msg, Throwable throwable) {
        super(msg, throwable);
    }

}
