package uk.ac.ox.it.ords.api.database.services;

import java.io.File;
import java.util.ServiceLoader;

import uk.ac.ox.it.ords.api.database.services.impl.hibernate.SQLServicePostgresImpl;

public interface SQLService {
	
	/**
	 * 
	 * @param id
	 * @param instance
	 * @return
	 * @throws Exception
	 */
	public File exportSQLFileFromDatabase( int dbId ) throws Exception;
	
	/**
	 * 
	 * @param sql
	 * @return
	 */
	public void importSQLFileToDatabase ( String databaseServer, String databaseName, File sqlFile, int databaseId ) throws Exception;
	

    public static class Factory {
		private static SQLService provider;
	    public static SQLService getInstance() {
	    	//
	    	// Use the service loader to load an implementation if one is available
	    	// Place a file called uk.ac.ox.it.ords.api.structure.service.CommentService in src/main/resources/META-INF/services
	    	// containing the classname to load as the CommentService implementation. 
	    	// By default we load the Hibernate/Postgresql implementation.
	    	//
	    	if (provider == null){
	    		ServiceLoader<SQLService> ldr = ServiceLoader.load(SQLService.class);
	    		for (SQLService service : ldr) {
	    			// We are only expecting one
	    			provider = service;
	    		}
	    	}
	    	//
	    	// If no service provider is found, use the default
	    	//
	    	if (provider == null){
	    		provider = new SQLServicePostgresImpl();
	    	}
	    	
	    	return provider;
	    }
	}

}
