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


import uk.ac.ox.it.ords.api.database.structure.TableData;

public interface CSVService {
	/**
	 * Export a database table as a CSV file
	 * @param database
	 * @param tableData
	 * @return the File written with the data from the table
	 */
	public abstract File exportTable(String server, String database, TableData tableData, String user, String password) throws Exception;
	
	/**
	 * Export a database table as a CSV file
	 * @param dbname 
	 * @param tablename
	 * @return the File written with the data from the table
	 */
	public abstract File exportTable(String server, String dbName, String tableName, String user, String password) throws Exception;
	
	/**
	 * Export a database table as a CSV file
	 * @param database
	 * @param tableData
	 * @param file the file to export the data to
	 * @return the File written with the data from the table
	 */
	public abstract File exportTable(String server, String database, TableData tableData, File file, String user, String password) throws Exception;

	/**
	 * Export a pre-run database query as a CSV file
	 * @param database
	 * @param query the query
	 * @return the File written with the data from the table
	 */
	public abstract File exportQuery(String server, String database, String query, String user, String password) throws Exception;
	
	/**
	 * Export a database table as a CSV file
	 * @param database
	 * @param query the query
	 * @param file the file to export the data to
	 * @return the File written with the data from the table
	 */
	public abstract File exportQuery(String server, String database, String query, File file, String user, String password) throws Exception;

	/**
	 * Read data from a CSV file and create a new table in the database
	 * @param database the name of the database to import into
	 * @param newTableName the name to use for the resulting table
	 * @param file the file to import
	 * @param headerRow true if the first row of the file are the column headers
	 * @return
	 */
	public abstract TableData newTableDataFromFile(String server, String database, String newTableName, File file, boolean headerRow, String user, String password) throws Exception;

	
	/**
	 * Read data from a CSV file and create a new table in the database
	 * @param database the name of the database to import into
	 * @param file the file to import
	 * @param headerRow true if the first row of the file are the column headers
	 * @return
	 */
	public abstract TableData newTableDataFromFile(String server, String database, File file, boolean headerRow, String user, String password) throws Exception;
	
	/**
	 * Read data from a CSV file and create a new table in the database
	 * @param database the name of the database to import into
	 * @param newTableName the name to use for the resulting table
	 * @param file the file to import
	 * @param headerRow true if the first row of the file are the column headers
	 * @param addPrimaryKeyColumn when set to true the resulting table will also have a generated PK column
	 * @return
	 */
	public abstract TableData newTableDataFromFile(String server, String database, String newTableName, File file, boolean headerRow, boolean addPrimaryKeyColumn, String user, String password) throws Exception;

	/**
	 * Read data from a CSV file and append it to an existing table in the database
	 * @param database the name of the database to import into
	 * @param tableName the name of the table to import the data into
	 * @param file the file to read data from
	 */
	public abstract void appendDataFromFile(String server, String database, String tableName, File file, String user, String password)  throws Exception;
	
	/**
	 * Read data from a CSV file and append it to an existing table in the database
	 * @param database the name of the database to import into
	 * @param tableName the name of the table to import the data into
	 * @param file the file to read data from
	 * @param headerRow true if the first row of data is to be interpreted as a header row
	 */
	public abstract void appendDataFromFile(String server, String database, String tableName, File file, boolean headerRow, String user, String password)  throws Exception;

}
