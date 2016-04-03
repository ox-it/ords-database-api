package uk.ac.ox.it.ords.api.database.services;

import java.util.ServiceLoader;

import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.services.impl.hibernate.DatabaseRoleServiceImpl;

public interface DatabaseRoleService {
	public void createInitialPermissions(OrdsPhysicalDatabase database) throws Exception;
	   public static class Factory {
			private static DatabaseRoleService provider;
		    public static DatabaseRoleService getInstance() {
		    	//
		    	// Use the service loader to load an implementation if one is available
		    	// Place a file called uk.ac.ox.oucs.ords.utilities.csv in src/main/resources/META-INF/services
		    	// containing the classname to load as the CsvService implementation. 
		    	// By default we load the Hibernate implementation.
		    	//
		    	if (provider == null){
		    		ServiceLoader<DatabaseRoleService> ldr = ServiceLoader.load(DatabaseRoleService.class);
		    		for (DatabaseRoleService service : ldr) {
		    			// We are only expecting one
		    			provider = service;
		    		}
		    	}
		    	//
		    	// If no service provider is found, use the default
		    	//
		    	if (provider == null){
		    		provider = new DatabaseRoleServiceImpl();
		    	}
		    	
		    	return provider;
		    }
	   }

	String getPrivateUserRole(String role, int projectId);

	String getPublicUserRole(String role);

}
