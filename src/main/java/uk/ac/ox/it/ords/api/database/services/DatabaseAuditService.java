/*
 * Copyright 2015 University of Oxford
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.ac.ox.it.ords.api.database.services;

import java.util.ServiceLoader;

import uk.ac.ox.it.ords.api.database.services.impl.DatabaseAuditServiceImpl;

public interface DatabaseAuditService {
	
    /**
     * Create audit message that the user is not authorised to perform a specific action
     * @param request the action that is not authorised
     */
    public abstract void createNotAuthRecord(String request);
    
    public abstract void createNotAuthRecord(String request, int logicalDatabaseId);
	
	public abstract void createDataInsertRecord(int databaseId);
	
	public abstract void createDataChangeRecord(int databaseId);

	public abstract void createDataDeleteRecord(int databaseId);

	public abstract void createDataQueryRecord(int databaseId);
	
	public abstract void createDatasetCreateRecord(int databaseId, int datasetId);

	public abstract void createDatasetDeleteRecord(int databaseId, int datasetId);
	
	public abstract void createImportRecord(int databaseId);

	public abstract void createExportRecord(int databaseId);

	/**
	 * Factory for obtaining implementations
	 */
    public static class Factory {
		private static DatabaseAuditService provider;
	    public static DatabaseAuditService getInstance() {
	    	//
	    	// Use the service loader to load an implementation if one is available
	    	// Place a file called uk.ac.ox.oucs.ords.utilities.csv in src/main/resources/META-INF/services
	    	// containing the classname to load as the CsvService implementation. 
	    	// By default we load the Hibernate implementation.
	    	//
	    	if (provider == null){
	    		ServiceLoader<DatabaseAuditService> ldr = ServiceLoader.load(DatabaseAuditService.class);
	    		for (DatabaseAuditService service : ldr) {
	    			// We are only expecting one
	    			provider = service;
	    		}
	    	}
	    	//
	    	// If no service provider is found, use the default
	    	//
	    	if (provider == null){
	    		provider = new DatabaseAuditServiceImpl();
	    	}
	    	
	    	return provider;
	    }
	}

	

}
