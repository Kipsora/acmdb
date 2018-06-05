package simpledb;

public class Table {
    private DbFile file;
    private String name;
    private String pkeyField;

    public Table(DbFile file, String name, String pkeyField) {
        this.file = file;
        this.name = name;
        this.pkeyField = pkeyField;
    }

    public DbFile getFile() {
        return file;
    }

    public String getName() {
        return name;
    }

    public String getPkeyField() {
        return pkeyField;
    }
}
