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

import java.io.File;
import java.util.ServiceLoader;

import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.services.impl.hibernate.DatabaseRoleServiceImpl;
import uk.ac.ox.it.ords.api.database.services.impl.hibernate.DatabaseUploadServiceImpl;




public interface DatabaseUploadService {
	
	
	public int createNewDatabaseFromFile(int physicalDBId, File dbFile, String type) throws Exception;
	
	

	   public static class Factory {
			private static DatabaseUploadService provider;
		    public static DatabaseUploadService getInstance() {
		    	//
		    	// Use the service loader to load an implementation if one is available
		    	// Place a file called uk.ac.ox.oucs.ords.utilities.csv in src/main/resources/META-INF/services
		    	// containing the classname to load as the CsvService implementation. 
		    	// By default we load the Hibernate implementation.
		    	//
		    	if (provider == null){
		    		ServiceLoader<DatabaseUploadService> ldr = ServiceLoader.load(DatabaseUploadService.class);
		    		for (DatabaseUploadService service : ldr) {
		    			// We are only expecting one
		    			provider = service;
		    		}
		    	}
		    	//
		    	// If no service provider is found, use the default
		    	//
		    	if (provider == null){
		    		provider = new DatabaseUploadServiceImpl();
		    	}
		    	
		    	return provider;
		    }
	   }

}
