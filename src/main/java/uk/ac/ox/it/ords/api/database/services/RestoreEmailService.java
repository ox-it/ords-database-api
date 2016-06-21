package uk.ac.ox.it.ords.api.database.services;

import java.util.ServiceLoader;

import uk.ac.ox.it.ords.api.database.services.impl.RestoreEmailServiceImpl;

public interface RestoreEmailService {

	public void setEmail ( String email );
	
	public void setDatabaseName ( String databaseName );
	
	public void sendStartRestoreMessage ( ) throws Exception;
	
	public void sendRestoreSuccessfulMessage ( ) throws Exception;
	
	public void sendRestoreUnsuccessfulMessage ( String error ) throws Exception;
	
	
    public static class Factory {
		private static RestoreEmailService provider;
	    public static RestoreEmailService getInstance() {
	    	//
	    	// Use the service loader to load an implementation if one is available
	    	// Place a file called uk.ac.ox.it.ords.api.structure.service.CommentService in src/main/resources/META-INF/services
	    	// containing the classname to load as the CommentService implementation. 
	    	// By default we load the Hibernate/Postgresql implementation.
	    	//
	    	if (provider == null){
	    		ServiceLoader<RestoreEmailService> ldr = ServiceLoader.load(RestoreEmailService.class);
	    		for (RestoreEmailService service : ldr) {
	    			// We are only expecting one
	    			provider = service;
	    		}
	    	}
	    	//
	    	// If no service provider is found, use the default
	    	//
	    	if (provider == null){
	    		provider = new RestoreEmailServiceImpl();
	    	}
	    	
	    	return provider;
	    }
	}

}
