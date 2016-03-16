package uk.ac.ox.it.ords.api.database.services;

import java.util.List;
import java.util.ServiceLoader;

import uk.ac.ox.it.ords.api.database.data.DataRow;
import uk.ac.ox.it.ords.api.database.data.Row;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.services.impl.hibernate.TableViewServiceImpl;

public interface TableViewService {

	// static data sets
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param datasetId
	 * @return
	 * @throws Exception
	 */
	public TableData getStaticDataSet ( int dbId, String instance, int datasetId ) throws Exception;
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param query
	 * @return
	 * @throws Exception
	 */
	public int createStaticDataSetOnQuery ( int dbId, String instance, String query ) throws Exception;
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param query
	 * @return
	 * @throws Exception
	 */
	public int updateStaticDataSet ( int dbId, String instance, String query ) throws Exception;
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param datasetId
	 * @throws Exception
	 */
	public void deleteStaticDataSet ( int dbId, String instance, int datasetId ) throws Exception;
	
	
	// live data
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param startIndex
	 * @param rowsPerPage
	 * @param filter
	 * @param sort
	 * @param sortDirection
	 * @return
	 * @throws Exception
	 */
	public TableData getDatabaseRows ( int dbId, String instance, String tableName, 
			int startIndex, int rowsPerPage, String sort, String sortDirection ) throws Exception;
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param newData
	 * @return
	 * @throws Exception
	 */
	public int appendTableData ( int dbId, String instance, String tableName, Row newData ) throws Exception;
	
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
	public int updateTableRow ( int dbId, String instance, String tableName, List<Row> rowData ) throws Exception;
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param tableName
	 * @param dataToRemove
	 * @throws Exception
	 */
	public void deleteTableData (int dbId, String instance, String tableName, String primaryKey, String primaryKeyValue ) throws Exception;
	
	
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
