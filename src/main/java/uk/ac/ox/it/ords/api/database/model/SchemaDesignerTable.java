package uk.ac.ox.it.ords.api.database.model;




/**
 *
 * @author mark
 */

public class SchemaDesignerTable {
	private int id;
    private int databaseId;
    private String tableName;
    private int x;
    private int y;


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }


    public int getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(int databaseId) {
        this.databaseId = databaseId;
    }


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }
    
}
