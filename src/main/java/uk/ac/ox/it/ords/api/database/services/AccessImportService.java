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
import java.util.Map;
import java.util.ServiceLoader;

import uk.ac.ox.it.ords.api.database.services.impl.hibernate.PostgresAccessDatabaseServiceImpl;

public interface AccessImportService {

	/**
	 * Check that a schema can be successfully created for this database
	 * 
	 * @param database
	 * @return true if we are confident that an import can be performed
	 */
	public abstract boolean preflightImport(File database) throws Exception;

	/**
	 * Create a schema from the supplied Access database file
	 * 
	 * @param databaseServer
	 * @param databaseName
	 * @param database
	 * @return a map of results for each table; true if the table was
	 *         successfully created
	 */
	public abstract Map<String, TableImportResult> createSchema(
			String databaseServer, String databaseName, File database) throws Exception;

	/**
	 * Import data from supplied Access database file. This method must only be
	 * called after creating a schema.
	 * 
	 * @param databaseServer
	 * @param databaseName
	 * @param database
	 * @return a map of results for each table; true if the data is successfully
	 *         imported for that table
	 */
	public abstract Map<String, TableImportResult> importData(
			String databaseServer, String databaseName, File database) throws Exception;

	public static class Factory {
		private static AccessImportService provider;
		public static AccessImportService getInstance() {
			//
			// Use the service loader to load an implementation if one is
			// available
			// Place a file called
			// uk.ac.ox.it.ords.api.structure.service.CommentService in
			// src/main/resources/META-INF/services
			// containing the classname to load as the CommentService
			// implementation.
			// By default we load the Hibernate/Postgresql implementation.
			//
			if (provider == null) {
				ServiceLoader<AccessImportService> ldr = ServiceLoader
						.load(AccessImportService.class);
				for (AccessImportService service : ldr) {
					// We are only expecting one
					provider = service;
				}
			}
			//
			// If no service provider is found, use the default
			//
			if (provider == null) {
				provider = new PostgresAccessDatabaseServiceImpl();
			}

			return provider;
		}
	}

}
