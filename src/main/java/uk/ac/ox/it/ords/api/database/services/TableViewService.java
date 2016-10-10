package uk.ac.ox.it.ords.api.database.services;

import java.util.List;
import java.util.ServiceLoader;

import uk.ac.ox.it.ords.api.database.data.Row;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.data.TableViewInfo;
import uk.ac.ox.it.ords.api.database.model.TableView;
import uk.ac.ox.it.ords.api.database.services.impl.hibernate.TableViewServiceImpl;

public interface TableViewService {

	// static data sets
	
	/**
	 * 
	 * @param term
	 * @return
	 * @throws Exception
	 */
	public List<TableViewInfo> searchDataSets(String term) throws Exception;
	

	/**
	 * 
	 * @param physicalDatabaseId
	 * @return
	 * @throws Exception
	 */
	public List<TableViewInfo> listDatasetsForDatabase ( int physicalDatabaseId ) throws Exception;
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param datasetId
	 * @return
	 * @throws Exception
	 */
	public TableData getStaticDataSetData ( int dbId,   int datasetId, int startIndex, int rowsPerPage, String sort, String direction ) throws Exception;
	
	
	/**
	 * @param dbId
	 * @param table
	 * @param foreignKey
	 * @param referencedTable
	 * @param referencedColumn
	 * @param offset
	 * @param limit
	 * @param sort
	 * @param sortDirection
	 * @param filter
	 * @param parameters
	 * @return
	 */
	public TableData getReferenceValues(int dbId, String table, String foreignKey, String referencedTable, String referencedColumn, int offset, int limit, String sort, String sortDirection, String filter, String parameters) throws Exception;

		
	/**
	 * 
	 * @param datasetId
	 * @return
	 * @throws Exception
	 */
	public TableViewInfo getStaticDataSet ( int datasetId ) throws Exception;
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param query
	 * @return
	 * @throws Exception
	 */
	public int createStaticDataSetOnQuery ( int dbId,   TableViewInfo viewInfo ) throws Exception;
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param query
	 * @return
	 * @throws Exception
	 */
	public int updateStaticDataSet ( int dbId,   int datasetId, TableViewInfo viewInfo ) throws Exception;
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param datasetId
	 * @throws Exception
	 */
	public void deleteStaticDataSet ( int datasetId ) throws Exception;
	
	
	/**
	 * 
	 * @param tableViewId
	 * @return TableView class
	 */
	public TableView getTableViewRecord ( int tableViewId );
	// live data
	
	/**
	 * 
	 * @param dbId
	 * @param tableName
	 * @param startIndex
	 * @param rowsPerPage
	 * @param filter
	 * @param params
	 * @param sort
	 * @param sortDirection
	 * @return
	 * @throws Exception
	 */
	public TableData getDatabaseRows ( 
			int dbId,   
			String tableName, 
			int startIndex, 
			int rowsPerPage, 
			String sort, 
			String sortDirection,
			String filter,
			String params
	) throws Exception;
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param newData
	 * @return true if the operation was successful
	 * @throws Exception
	 */
	public boolean appendTableData ( int dbId,   String tableName, Row newData ) throws Exception;
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param lookupColName
	 * @param lookupValue
	 * @param rowData
	 * @return
	 * @throws Exception
	 */
	public int updateTableRow ( int dbId,   String tableName, List<Row> rowData ) throws Exception;
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param dataToRemove
	 * @throws Exception
	 */
	public void deleteTableData (int dbId,   String tableName, String primaryKey, String primaryKeyValue ) throws Exception;
	
	
    public static class Factory {
		private static TableViewService provider;
	    public static TableViewService getInstance() {
	    	//
	    	// Use the service loader to load an implementation if one is available
	    	// Place a file called uk.ac.ox.it.ords.api.structure.service.CommentService in src/main/resources/META-INF/services
	    	// containing the classname to load as the CommentService implementation. 
	    	// By default we load the Hibernate/Postgresql implementation.
	    	//
	    	if (provider == null){
	    		ServiceLoader<TableViewService> ldr = ServiceLoader.load(TableViewService.class);
	    		for (TableViewService service : ldr) {
	    			// We are only expecting one
	    			provider = service;
	    		}
	    	}
	    	//
	    	// If no service provider is found, use the default
	    	//
	    	if (provider == null){
	    		provider = new TableViewServiceImpl();
	    	}
	    	
	    	return provider;
	    }
	}
}
