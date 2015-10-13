/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package uk.ac.ox.it.ords.api.database.model;



/**
 *
 * @author dave
 */

public class LinkedFields {
    private int id;
    private int physicalDatabaseId;
    private int userId;
    private String primaryTable;
    private String primaryField;
    private String foreigntable;
    private String foreignField;

    

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getPhysicalDatabaseId() {
        return physicalDatabaseId;
    }

    public void setPhysicalDatabaseId(int physicalDatabaseId) {
        this.physicalDatabaseId = physicalDatabaseId;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getPrimaryTable() {
        return primaryTable;
    }

    public void setPrimaryTable(String primaryTable) {
        this.primaryTable = primaryTable;
    }

    public String getPrimaryField() {
        return primaryField;
    }

    public void setPrimaryField(String primaryField) {
        this.primaryField = primaryField;
    }

    public String getForeigntable() {
        return foreigntable;
    }

    public void setForeigntable(String foreigntable) {
        this.foreigntable = foreigntable;
    }

    public String getForeignField() {
        return foreignField;
    }

    public void setForeignField(String foreignField) {
        this.foreignField = foreignField;
    }
    
    
    
}
