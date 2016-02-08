package uk.ac.ox.it.ords.api.database.services;

import java.util.List;
import java.util.ServiceLoader;

import uk.ac.ox.it.ords.api.database.data.*;
import uk.ac.ox.it.ords.api.database.services.impl.hibernate.QueryServiceImpl;

public interface QueryService {
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param table
	 * @param foreignKeyColumn
	 * @return
	 * @throws Exception
	 */
	public TableData getReferenceColumnData (int dbId, String instance, String table, String foreignKeyColumn ) throws Exception;
	
	
	/**
	 * 
	 * @param dbId
	 * @param instance
	 * @param q
	 * @param startIndex
	 * @param rowsPerPage
	 * @param filter
	 * @param order
	 * @return
	 * @throws Exception
	 */
	public TableData performQuery ( int dbId, String instance, String q, int startIndex, int rowsPerPage, String filter, String order ) throws Exception;
	
	
    public static class Factory {
		private static QueryService provider;
	    public static QueryService getInstance() {
	    	//
	    	// Use the service loader to load an implementation if one is available
	    	// Place a file called uk.ac.ox.it.ords.api.structure.service.CommentService in src/main/resources/META-INF/services
	    	// containing the classname to load as the CommentService implementation. 
	    	// By default we load the Hibernate/Postgresql implementation.
	    	//
	    	if (provider == null){
	    		ServiceLoader<QueryService> ldr = ServiceLoader.load(QueryService.class);
	    		for (QueryService service : ldr) {
	    			// We are only expecting one
	    			provider = service;
	    		}
	    	}
	    	//
	    	// If no service provider is found, use the default
	    	//
	    	if (provider == null){
	    		provider = new QueryServiceImpl();
	    	}
	    	
	    	return provider;
	    }
	}

}
