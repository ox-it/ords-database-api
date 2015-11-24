package uk.ac.ox.it.ords.api.database.model;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * This class holds database views that the user has defined.
 *
 * @author dave
 */
@Entity
@Table(name = "tableView")
public class TableView {
    private int id;
    private String viewName = "";
    private String viewDescription = "";
    private String associatedDatabase = "";
    private String associatedTable = "";
    private int physicalDatabaseId;
    private int creatorId;
    private int projectId;
    private String query = "";
    private String tvAuthorization = "authmembers";
    private boolean staticDataset;
//    /**
//     * The name of the server where the static data is held. This will currently
//     * be the same server as where the original data is held.
//     */
//    private String dbServer;
    /**
     * This is to be used when the view is static - it will then point to
     * the original database that the copy was made from.
     */
    private String originalDatabase = "";

    public TableView() {

    }

    @Id
    @GeneratedValue
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getProjectId() {
        return projectId;
    }

    public void setProjectId(int projectId) {
        this.projectId = projectId;
    }

    public String getAssociatedDatabase() {
        return associatedDatabase;
    }

    public void setAssociatedDatabase(String associatedDatabase) {
        this.associatedDatabase = associatedDatabase;
    }

    public String getTvAuthorization() {
        return tvAuthorization;
    }

    public void setTvAuthorization(String tvAuthorization) {
        this.tvAuthorization = tvAuthorization;
    }

    public String getAssociatedTable() {
        return associatedTable;
    }

    public void setAssociatedTable(String associatedTable) {
        this.associatedTable = associatedTable;
    }

    public int getCreatorId() {
        return creatorId;
    }

    public void setCreatorId(int creatorId) {
        this.creatorId = creatorId;
    }

    public String getViewName() {
        return viewName;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    public String getViewDescription() {
        return viewDescription;
    }

    public void setViewDescription(String viewDescription) {
        this.viewDescription = viewDescription;
    }

    @Column(columnDefinition="TEXT")
    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public int getPhysicalDatabaseId() {
        return physicalDatabaseId;
    }

    public void setPhysicalDatabaseId(int physicalDatabaseId) {
        this.physicalDatabaseId = physicalDatabaseId;
    }

    public boolean isStaticDataset() {
        return staticDataset;
    }

    public void setStaticDataset(boolean staticDataset) {
        this.staticDataset = staticDataset;
    }
    
//    public String getDbServer() {
//        return dbServer;
//    }
//
//    public void setDbServer(String dbServer) {
//        this.dbServer = dbServer;
//    }

    public String getOriginalDatabase() {
        return originalDatabase;
    }

    public void setOriginalDatabase(String originalDatabase) {
        this.originalDatabase = originalDatabase;
    }
}
