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

import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.services.impl.hibernate.PostgresCsvServiceImpl;

public interface CSVService {
	/**
	 * Export a database table as a CSV file
	 * 
	 * @param database
	 * @param tableData
	 * @return the File written with the data from the table
	 */
	public abstract File exportTable(String server, String database,
			TableData tableData) throws Exception;

	/**
	 * Export a database table as a CSV file
	 * 
	 * @param dbname
	 * @param tablename
	 * @return the File written with the data from the table
	 */
	public abstract File exportTable(String server, String dbName,
			String tableName) throws Exception;

	/**
	 * Export a database table as a CSV file
	 * 
	 * @param database
	 * @param tableData
	 * @param file
	 *            the file to export the data to
	 * @return the File written with the data from the table
	 */
	public abstract File exportTable(String server, String database,
			TableData tableData, File file)
			throws Exception;

	/**
	 * Export a pre-run database query as a CSV file
	 * 
	 * @param database
	 * @param query
	 *            the query
	 * @return the File written with the data from the table
	 */
	public abstract File exportQuery(String server, String database,
			String query) throws Exception;
	
	
	/**
	 * Export a whole database either as a single CSV or a zipped folder of CSVs
	 * @param server
	 * @param database
	 * @param zipped
	 * @return
	 * @throws Exception 
	 */
	public abstract File exportDatabase(String server, String database, boolean zipped) throws Exception;

	/**
	 * Export a database table as a CSV file
	 * 
	 * @param database
	 * @param query
	 *            the query
	 * @param file
	 *            the file to export the data to
	 * @return the File written with the data from the table
	 */
	public abstract File exportQuery(String server, String database,
			String query, File file)
			throws Exception;

	/**
	 * Read data from a CSV file and create a new table in the database
	 * 
	 * @param database
	 *            the name of the database to import into
	 * @param newTableName
	 *            the name to use for the resulting table
	 * @param file
	 *            the file to import
	 * @param headerRow
	 *            true if the first row of the file are the column headers
	 * @return
	 */
	public abstract TableData newTableDataFromFile(String server,
			String database, String newTableName, File file, boolean headerRow) throws Exception;

	/**
	 * Read data from a CSV file and create a new table in the database
	 * 
	 * @param database
	 *            the name of the database to import into
	 * @param file
	 *            the file to import
	 * @param headerRow
	 *            true if the first row of the file are the column headers
	 * @return
	 */
	public abstract TableData newTableDataFromFile(String server,
			String database, File file, boolean headerRow) throws Exception;

	/**
	 * Read data from a CSV file and create a new table in the database
	 * 
	 * @param database
	 *            the name of the database to import into
	 * @param newTableName
	 *            the name to use for the resulting table
	 * @param file
	 *            the file to import
	 * @param headerRow
	 *            true if the first row of the file are the column headers
	 * @param addPrimaryKeyColumn
	 *            when set to true the resulting table will also have a
	 *            generated PK column
	 * @return
	 */
	public abstract TableData newTableDataFromFile(String server,
			String database, String newTableName, File file, boolean headerRow,
			boolean addPrimaryKeyColumn)
			throws Exception;

	/**
	 * Read data from a CSV file and append it to an existing table in the
	 * database
	 * 
	 * @param database
	 *            the name of the database to import into
	 * @param tableName
	 *            the name of the table to import the data into
	 * @param file
	 *            the file to read data from
	 */
	public abstract void appendDataFromFile(String server, String database,
			String tableName, File file)
			throws Exception;

	/**
	 * Read data from a CSV file and append it to an existing table in the
	 * database
	 * 
	 * @param database
	 *            the name of the database to import into
	 * @param tableName
	 *            the name of the table to import the data into
	 * @param file
	 *            the file to read data from
	 * @param headerRow
	 *            true if the first row of data is to be interpreted as a header
	 *            row
	 */
	public abstract void appendDataFromFile(String server, String database,
			String tableName, File file, boolean headerRow) throws Exception;

	public static class Factory {
		private static CSVService provider;
		public static CSVService getInstance() {
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
				ServiceLoader<CSVService> ldr = ServiceLoader
						.load(CSVService.class);
				for (CSVService service : ldr) {
					// We are only expecting one
					provider = service;
				}
			}
			//
			// If no service provider is found, use the default
			//
			if (provider == null) {
				provider = new PostgresCsvServiceImpl();
			}

			return provider;
		}
	}

}
