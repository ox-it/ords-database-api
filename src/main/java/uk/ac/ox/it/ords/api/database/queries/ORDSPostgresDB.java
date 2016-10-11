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

package uk.ac.ox.it.ords.api.database.queries;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.conf.CommonVars;
import uk.ac.ox.it.ords.api.database.data.DataCell;
import uk.ac.ox.it.ords.api.database.data.DataRow;
import uk.ac.ox.it.ords.api.database.data.OrdsTableColumn;
import uk.ac.ox.it.ords.api.database.data.TableData;

/**
 * A class to run some key database commands against the database.
 * Commands that might, for example, get a list of the columns in a table in 
 * a db or a command to count the number of records in a table.
 * 
 * @author dave
 */
public class ORDSPostgresDB extends QueryRunner {
	
    private static Logger log = LoggerFactory.getLogger(ORDSPostgresDB.class);
    
    /**
     * Construct an instance; you need to supply both a database name AND a database server to
     * use this class
     * 
     * @param dbServer
     * @param dbName
     */
	public ORDSPostgresDB(String dbServer, String dbName) {
		super(dbServer, dbName);
    }
    
	/**
	 * Gets the names of all the columns in a table
	 * @param tableName
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
    public TableData getColumnNamesForTable(String tableName) throws ClassNotFoundException, SQLException {
    	
        String command = String.format("select column_name from information_schema.columns where table_name = '%s';",
                tableName);
        TableData td = null;
        if (runDBQuery(command)) {
        	td = this.getTableData();
        	td.tableName = tableName;
        }
        
        return td;
    }
    
    /**
     * Gets column metadata for a table. Used in populating table metadata.
     * @param tableName
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private TableData getColumnDataTypesForTable(String tableName) throws ClassNotFoundException, SQLException {

        String command = String.format("select column_name, data_type from information_schema.columns where table_name = '%s';",
                tableName);
        
        TableData td = null;
        if (runDBQuery(command)) {
        	td = this.getTableData();
        	td.tableName = tableName;
        }
        
        return td;
    }
    
   /**
    * Returns the first primary key found. Used for setting default sort columns etc.
    * @param tableName
    * @return the primary key of the table, but only if there is one (it will return the first entry if more than one),
    * else null
    */
   public String getSingularPrimaryKeyColumn(String tableName) throws ClassNotFoundException, SQLException {
	   
	   List<String> primaryKeys = this.getPrimaryKeys(tableName);
	   if (primaryKeys.isEmpty()){
		   return null;
	   } else {
		   return primaryKeys.get(0);
	   }
   }
   
   /**
    * Get all the primary keys for a table. Used for populating metadata.
    * @param tableName
    * @return
    * @throws ClassNotFoundException
    * @throws SQLException
    */
   private List<String> getPrimaryKeys(String tableName) throws ClassNotFoundException, SQLException {
	   List<String> primaryKeys = new ArrayList<String>();
	   
       String command = String
               .format("SELECT pg_attribute.attname,  format_type(pg_attribute.atttypid, pg_attribute.atttypmod)  FROM pg_index, pg_class, pg_attribute WHERE pg_class.oid = '\"%s\"'::regclass AND indrelid = pg_class.oid AND pg_attribute.attrelid = pg_class.oid AND pg_attribute.attnum = any(pg_index.indkey) AND indisprimary",
               tableName);
       
       if (!runDBQuery(command)) {
    	   return null;
       }
       if (this.getTableData().rows.size() >= 1) {
           for (DataRow dr : this.getTableData().rows) {
               String primaryKey = dr.cell.get("attname").getValue();
               primaryKeys.add(primaryKey);
           }
       } 

       return primaryKeys;
   }
   
   /**
    * Adds relation metadata for a column. Used in populating metadata.
    * @param otc
    * @param constraints
    * @return
    * @throws SQLException
    */
   private OrdsTableColumn addReferencesToColumn(OrdsTableColumn otc, TableData constraints) throws SQLException {
       try {
           for (DataRow constraint : constraints.rows) {

               if (otc.columnName.equals(constraint.cell.get("column_name").getValue())) {
                   otc.referencedColumn = constraint.cell.get("foreign_column_name").getValue();
                   otc.referencedTable = constraint.cell.get("foreign_table_name").getValue();

                   otc.alternateColumns = new ArrayList<String>();
                   TableData columnNames = getColumnNamesForTable(otc.referencedTable);
                   for (int rowIndex = 0; rowIndex < columnNames.rows.size(); rowIndex++) {
                       DataRow rd = columnNames.rows.get(rowIndex);
                       for (String s2 : rd.cell.keySet()) {
                           otc.alternateColumns.add(rd.cell.get(s2).getValue());
                       }
                   }
               }
           }
       } catch (ClassNotFoundException e) {
           log.error(e.getMessage());
       } 
       return otc;
   }
    /*
     * ... Work on column information
     */
   
   
   /**
    * Get a list of all tables associated with a particular database
    *
    * @return true if the command has been successful
    * @throws java.sql.SQLException
    * @throws java.lang.ClassNotFoundException
    */
   public boolean checkDatabaseHasTables() throws SQLException, ClassNotFoundException {

       String command = String.format("select c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','') AND n.nspname NOT IN ('pg_catalog', 'pg_toast') AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY UPPER(c.relname);");
       return runDBQuery(command);
   }
   
   /**
    * Get a list of table names for the database in a TableData object.  The table names are the
    * @return
    * @throws SQLException
    * @throws ClassNotFoundException
    */
   public TableData getTableNamesForDatabase() throws SQLException, ClassNotFoundException {
       if (log.isDebugEnabled()) {
           log.debug(String.format("getTablesForDatabase()"));
       }
       String command = String.format("select c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','') AND n.nspname NOT IN ('pg_catalog', 'pg_toast') AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY UPPER(c.relname);");
       ParameterList params = new ParameterList();
       TableData tableData = runDBQuery(command, params,0,0, true);
       log.debug("getTablesForDatabase:return");
       return tableData;
   }
    		    
    /**
     * Get rows of data from the table. This function will get ALL table rows, which may be large.
     * 
     * NOTE this only seems to be used in testing. Consider removing it.
     * 
     * @param tableName the table whose data is to be returned
     * @param sort
     * @param direction
     * @return the table data for the table or NULL if the table doesn't exist
     * @throws SQLException 
     */
    public TableData getTableDataForTable(String tableName, String sort, boolean direction) throws ClassNotFoundException, SQLException {
        return getTableDataForTable(tableName, 1, 0, sort, direction);
    }
    
    /**
     * Get the metadata associated with a table - columns, data types, relationships
     * 
     * @param tableName
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private TableData getTableMetadataForTable(String tableName) throws ClassNotFoundException, SQLException {

        ArrayList<String> sequences = getSequencesForTable(tableName);
        TableData tableData = getColumnDataTypesForTable(tableName);
        TableData constraints = getForeignConstraintsForTable(tableName);
        
        tableData.columns.clear();
        int i = 1;
        for (DataRow row : tableData.rows) {
            OrdsTableColumn otc = new OrdsTableColumn();
            otc.orderIndex = i++;
            DataCell dc = row.cell.get("column_name");
            otc.columnName = dc.getValue();
            dc = row.cell.get("data_type");
            /*  The following is a bit of a hack. Sometimes we've got
                data types as varchars, sometimes we've got columns of
                the actual data types.  The data type will always be a valid value so
                we'll use that as the fallback.
            */
            try {
                String type = dc.getValue();
                type = type.replace(" precision", "")
                        .replace("character varying", "varchar")
                        .replace("character", "char")
                        .replace(" without time zone", "")
                        .replace("bytea", "binary")
                        .toUpperCase();
                otc.columnType = DataCell.DataType.valueOf(type);
            } catch (java.lang.IllegalArgumentException e) {
                otc.columnType = DataCell.DataType.valueOf(dc.getType().toString());
            }

            otc = addReferencesToColumn(otc, constraints);
            
            otc.comment = getColumnComment(tableName, otc.columnName);

            tableData.columns.add(otc);
        }
        
        tableData.rows = new ArrayList<DataRow>();
        tableData.sequences = sequences;
        tableData.setNumberOfRowsInEntireTable(0);
        tableData.comment = getTableComment(tableName);
        tableData.primaryKeys = getPrimaryKeys(tableName);
        return tableData;
    }
    
    /**
     * Get table comments. Used when collecting table metadata
     * 
     * @param tableName
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private String getTableComment(String tableName) throws ClassNotFoundException, SQLException{
        String tableComment = "";
        boolean ret = runDBQuery("SELECT obj_description(quote_ident('"+tableName+"')::regclass::oid, 'pg_class') as comment");
        TableData commentData = this.getTableData();
        if (ret && commentData.rows.size() > 0) {
            tableComment = commentData.rows.get(0).cell.get("comment").getValue();
            if (tableComment == null) {
                tableComment = "";
            }
        }
        return tableComment;
    }
    
    /**
     * Get table data using a specified query and parameters as input. Returns table metadata as well
     * as the rows.
     * 
     * @param query
     * @param parameters
     * @param iscasesensitive
     * @param tableName
     * @param rowStart
     * @param numberOfRowsRequired
     * @param sort
     * @param direction
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public TableData getTableDataForTable(String query, ParameterList parameters, String tableName, int rowStart, int numberOfRowsRequired, String sort, boolean direction) throws ClassNotFoundException, SQLException {
        if (tableName == null) {
            log.error("Null table name info");
            return null;
        }

        int databaseRowStart;
        if (rowStart == 0) {
            log.error("Coding error - rowStart of zero input. The rowStart is from a users perspective and this should start at value 1");
            databaseRowStart = rowStart;
        }
        else {
            databaseRowStart = rowStart - 1;
        }

        if (!this.checkTableExists(tableName)) {
            log.info(String.format("Table %s does not exist", tableName));
            return null;
        }

        if (!runDBQuery("select count(*) from \"" + tableName+"\"")) {
            return null;
        }
        String totalRows = this.getTableData().rows.get(0).cell.get("count").getValue();
        int totalRowsInt = 0;
        if (totalRows == null) {
            log.error("Null value for total rows. Should not happen!");
            return null;
        }
       
        TableData metadata = this.getTableMetadataForTable(tableName);
        totalRowsInt = Integer.parseInt(totalRows);
        if (totalRowsInt == 0) {
        	// There are no rows in the table. Just set up column information and return
        	return metadata;
        }

        
        TableData tableData = runDBQuery(query, parameters, databaseRowStart, numberOfRowsRequired, true);

        if (tableData == null) {
            log.error("Null table data returned - can't do anything with this");
            return null;
        }
        
        //
        // This really is the total rows in the table (from the table metadata). 
        // The actual row count from the query (which may be filtered) is returned below from runDBQuery.
        //
        tableData.setNumberOfRowsInEntireTable(totalRowsInt);
        
        //
    	// Note that we want to tell the UI the actual start index.
    	// We've used the OFFSET, which is one less than where you want to
    	// start the zero-based index. So to return a 1-based index of rows,
    	// we have to add 1.
    	//
        tableData.setCurrentRow(databaseRowStart + 1);
        
        //
        // If there were no results, we won't have column metadata, so we need to add it in now
        // anyway from the metadata we collected earlier.
        //
        tableData.columns = metadata.columns;
        
        tableData.tableName = tableName;
        tableData.comment = metadata.comment;
        tableData.sequences = metadata.sequences;
        tableData.primaryKeys = metadata.primaryKeys;
//        TableData td2 = getForeignConstraintsForTable(tableName);
//
        // Look at each column in our current data
//        log.debug("About to loop through columns");
//        for (OrdsTableColumn column : tableData.columns) {
//            if (td2 != null && !td2.rows.isEmpty()) {
//                column = addReferencesToColumn(column, td2);
//            }
//            column.comment = getColumnComment(tableData.tableName, column.columnName);
//        }

        return tableData;
    }


    /**
     * Checks whether the specified table exists
     * @param tableName
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private boolean checkTableExists(String tableName) throws ClassNotFoundException, SQLException{
        /*
         * Get all table names in the database and ensure the table exists
         */
        boolean exists = false;
        QueryRunner lqr = new QueryRunner(this.getCurrentDbServer(), this.getCurrentDbName());
        if (!lqr.runDBQuery(String.format("SELECT table_name FROM information_schema.tables WHERE table_schema='%s'", CommonVars.SCHEMA_NAME))) {
            return false;
        }
        for (DataRow dr : lqr.getTableData().rows) {
            for (DataCell dc : dr.cell.values()) {
                String existingTable = dc.getValue();
                if (existingTable.equalsIgnoreCase(tableName)) {
                    exists = true;
                    break;
                }
            }
            if (exists) {
                break;
            }
        }
        return exists;
    }

    /**
     * Get rows of data from the table. This function will get a subset of the table data.
     *
     * @param tableName the table whose data is to be returned
     * @param rowStart the number of the first row to return. In terms of the user, the row number starts at row 1, so
     * for example they may display rows 1 to 30, meaning the first 30 rows of data. In terms of the database, however,
     * the row number starts at 0, so rows 1 to 30 (from a user perspective) correspond to rows 0 to 29 from a database
     * perspective.
     * @param numberOfRowsRequired the number of rows to return. If zero, then all rows are returned.
     * @return table data or null if there has been an error or the table doesn't exist
     * @throws SQLException 
     */
    public TableData getTableDataForTable(String tableName, int rowStart, int numberOfRowsRequired, String sort, boolean direction) throws ClassNotFoundException, SQLException {
        if (tableName == null) {
            log.error("Null table name info");
            return null;
        }
 
        String keyName = null;
        try {
            keyName = getSingularPrimaryKeyColumn(tableName);
        }
        catch (ClassNotFoundException ex) {
            log.error("Class error - unable to find index", ex);
            return null;
        }
        catch (SQLException ex) {
            log.error("Unable to find index", ex);
            return null;
        }

        String query = "";
        
        if ( (sort == null) || (sort.equals("null")) ) {
        	if (keyName == null) {
        		log.debug("No key specified - simple select ");
        		query = String.format("select * from \"%s\"", tableName);
        	}
        	else {
        		log.debug("Key specified - select ordered by key");
        		query = String.format("select * from \"%s\" order by \"%s\"", tableName, keyName);
        	}
        }
        else {
             query = String.format("select * from \"%s\" order by \"%s\" %s", tableName, sort, direction ? "asc" : "desc");
        }
        
        return this.getTableDataForTable(query, null, tableName, rowStart, numberOfRowsRequired, sort, direction);
    }
    
    /**
     * Get any auto-increment sequences for the specified table. Used for metadata collection.
     * @param tableName
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private ArrayList<String> getSequencesForTable(String tableName) throws ClassNotFoundException, SQLException {
        ArrayList<String> sequences = new ArrayList<String>();
        if (runDBQuery(String.format("SELECT column_name, column_default from information_schema.columns where table_name='%s' AND column_default IS NOT NULL", tableName))) {
	        for (DataRow row : this.getTableData().rows) {
	            String columnDefault = row.cell.get("column_default").getValue();
	            if (columnDefault.startsWith("nextval(")) {
	                sequences.add(row.cell.get("column_name").getValue());
	            }
	        }
        }
        return sequences;
    }

    /**
     * Provide the foreign constraints for a table. Using this function, the caller will be provided with all
     * relationship information about the table. Used in collecting meteadata.
     *
     * @param tableName the table whose information is required.
     * @return a TableData object with all the information
     * @throws SQLException 
     */
    private TableData getForeignConstraintsForTable(String tableName) throws ClassNotFoundException, SQLException {
        log.debug("getForeignConstraintsForTable");

        String command = String
                .format("SELECT tc.constraint_name, tc.table_name, kcu.column_name,  ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name  FROM  information_schema.table_constraints AS tc  JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name='%s'",
                tableName);
        if (!runDBQuery(command)) {
        	return null;
        }
        TableData tableData = this.getTableData();

        tableData.tableName = tableName;
        return tableData;
    }
 
    /**
     * Gets a list of primary keys and the specified column from the specified table.
     * The query aliases the primary key as "value" and the specified column as "label" in the results.
     * This data is used for creating select lists by {@link GeneralUtils.buildReferenceDataSelect}
     * 
     * @param referencedTable The name of the table
     * @param referencedColumn The column to use as "label"
     * @return All the unique values from referencedTable
     */
    public TableData getReferenceValues(String referencedTable, String referencedColumn) {
        TableData referenceValues = new TableData();
        try {
            String primaryKey = getSingularPrimaryKeyColumn(referencedTable);
            String query = String.format("SELECT DISTINCT \"%s\" AS value, \"%s\" AS label FROM \"%s\" ORDER BY label ASC LIMIT 100", primaryKey, referencedColumn, referencedTable);
            runDBQuery(query);
            referenceValues = this.getTableData();
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage());
        } catch (SQLException e) {
            log.error(e.getMessage());
        }
        return referenceValues;
    }
    
    /**
     * Gets a list of foreign keys, primary keys and labels from specified table as "id", "value" and "label".
     * 
     * Used for obtaining a foreign table column for transposing over the foreign key in a table; for example, for the
     * given table with filters and pagination applied, with a column of this.fk, what are the values of rel.label rather 
     * than rel.pk for this.fk?
     * 
     * @param table
     * @param foreignKey
     * @param referencedTable
     * @param referencedColumn
     * @param offset
     * @param limit
     * @param filter
     * @param parameters
     * @param isCaseSensitive
     * @return
     */
    public TableData getReferenceValues(String table, String foreignKey, String referencedTable, String referencedColumn, int start, int length, String sort, boolean direction, String filter, ParameterList parameters){
        TableData referenceValues = new TableData();
        try {
        	//
        	// Get the primary key of the referencing table
        	// 
        	String originPrimaryKey = getSingularPrimaryKeyColumn(table);
        	
        	//
        	// Get the primary key of the referenced table
        	//
            String primaryKey = getSingularPrimaryKeyColumn(referencedTable);
            
            //
            // If no column is given, use the primary key column
            //
            if (referencedColumn == null) referencedColumn = primaryKey;
            
			if (sort == null || sort.isEmpty() || sort.equalsIgnoreCase("undefined")){
				sort = originPrimaryKey;
			}
            
            //
            // The SQL query captures a value and label
            //
            String sql = String.format("SELECT \"%1$s\".\"%2$s\" AS id, "
            		+ "\"%3$s\".\"%5$s\" AS value, \"%3$s\".\"%4$s\" AS label "
            		+ "FROM \"%1$s\" LEFT OUTER JOIN \"%3$s\" ON \"%3$s\".\"%5$s\" = \"%1$s\".\"%6$s\" "
            		+ "ORDER BY \"%1$s\".\"%7$s\" %8$s", table, originPrimaryKey, referencedTable, referencedColumn, primaryKey, foreignKey, sort, direction? "ASC" : "DESC", start, length);     
			//
			// The optional WHERE clause looks like this:
            //
			// WHERE documents.docid IN (SELECT docid FROM documents WHERE docid > 200)
            //
            // We use this when the table is being filtered, as we then need to return
            // the keys from a subquery based on the filter.
            //
            if (filter != null){
            	try {
            		            		
            		//
            		// Just include WHERE clause from filter
            		//
					filter = filter.split("WHERE")[1];

		          	sql = String.format("SELECT \"%1$s\".\"%2$s\" AS id, "
	                		+ "\"%3$s\".\"%5$s\" AS value, \"%3$s\".\"%4$s\" AS label "
	                		+ "FROM \"%1$s\" LEFT OUTER JOIN \"%3$s\" ON \"%3$s\".\"%5$s\" = \"%1$s\".\"%6$s\" "
	                		+ "WHERE \"%1$s\".\"%2$s\" IN (SELECT \"%2$s\" FROM \"%1$s\" WHERE %9$s) "
	                		+ "ORDER BY \"%1$s\".\"%7$s\" %8$s", table, originPrimaryKey, referencedTable, referencedColumn, primaryKey, foreignKey, sort, direction? "ASC" : "DESC", filter);
	  
				} catch (Exception e) {
					log.debug("getReferenceValues: filter did not contain a WHERE clause");
				}
            }

            //SELECT documents.docid AS id, sites.id AS value, sites.sitename AS label FROM documents LEFT OUTER JOIN sites ON sites.id = documents.docsite ORDER BY docsite ASC LIMIT 100 OFFSET 0 
            
            log.debug("Getting references values using SQL:" + sql);
            referenceValues = runDBQuery(sql, parameters, start, length, true);
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage());
        } catch (SQLException e) {
            log.error(e.getMessage());
        }
        return referenceValues;
    }
    
    /**
     * Gets a list of primary keys and the specified column from the specified table.
     * The query aliases the primary key as "value" and the specified column as "label" in the results.
     * This data is used for creating auto-completion select lists using the supplied query term
     * 
     * @param referencedTable The name of the table
     * @param referencedColumn The column to use as "label"
     * @return matching values for the query in this column, a maximum of 100 are returned by default
     */
    public TableData getReferenceValues(String referencedTable, String referencedColumn, String query) {
        TableData referenceValues = new TableData();
        try {
            String primaryKey = getSingularPrimaryKeyColumn(referencedTable);
            //
            // If no column is given, use the primary key column
            //
            if (referencedColumn == null) referencedColumn = primaryKey;
            
            //
            // The SQL query captures a value and label
            //
            // We can't do SELECT DISTINCT on a different column from the sort key. However,
            // it wouldn't make sense to do a SELECT DISTINCT where there are ambiguous values 
            // (i.e. the same label may apply to multiple rows with different primary keys)
            // Hence we select value and label using PK and referenced label column, ordered
            // by label
            //
            String sql = String.format("SELECT \"%1$s\" AS value, \"%2$s\" AS label FROM \"%3$s\" WHERE CAST (\"%2$s\" AS TEXT) ILIKE '%%%4$s%%\' ORDER BY \"%2$s\" ASC LIMIT 100", primaryKey, referencedColumn, referencedTable, query);
            //
            // This is another alternative, but the order returned will be random
            //
            // String sql = String.format("SELECT DISTINCT ON (\"%1$s\") \"%1$s\" AS value, \"%2$s\" AS label FROM \"%3$s\" WHERE CAST (\"%2$s\" AS TEXT) ILIKE '%%%4$s%%\', primaryKey, referencedColumn, referencedTable, query);
            //
            // This doesn't work, but is what you'd 'want' as the best option
            //
            // String sql = String.format("SELECT DISTINCT ON (\"%1$s\") \"%1$s\" AS value, \"%2$s\" AS label FROM \"%3$s\" WHERE CAST (\"%2$s\" AS TEXT) ILIKE '%%%4$s%%\' ORDER BY \"%2$s\" ASC LIMIT 100", primaryKey, referencedColumn, referencedTable, query);
            //
            log.debug("Getting refrences values using SQL:" + sql);
            if (runDBQuery(sql)) {
            	referenceValues = this.getTableData();
            }
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage());
        } catch (SQLException e) {
            log.error(e.getMessage());
        }
        return referenceValues;
    }
        
    
    /**
     * Get a list of all tables associated with a particular database. Used in export/import
     *
     * @return true if the command has been successful
     * @throws java.sql.SQLException
     * @throws java.lang.ClassNotFoundException
     */
    public boolean getTablesForDatabase() throws SQLException, ClassNotFoundException {
        if (log.isDebugEnabled()) {
            log.debug(String.format("getTablesForDatabase()"));
        }
        String command = String.format("select c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','') AND n.nspname NOT IN ('pg_catalog', 'pg_toast') AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY UPPER(c.relname);");
        return runDBQuery(command);
    }
    
    /**
     * Get a list of all tables associated with a particular database. Used in import
     * refactor: merge with the above method.
     *
     * @return true if the command has been successful
     * @throws java.sql.SQLException
     * @throws java.lang.ClassNotFoundException
     */
    public TableData getTableDataForDatabase() throws SQLException, ClassNotFoundException {
        if (log.isDebugEnabled()) {
            log.debug(String.format("getTablesForDatabase()"));
        }
        String command = String.format("select c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','') AND n.nspname NOT IN ('pg_catalog', 'pg_toast') AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY UPPER(c.relname);");
        ParameterList params = new ParameterList();
        TableData tableData = runDBQuery(command, params,0,0, true);
        log.debug("getTablesForDatabase:return");
        return tableData;
    }

    /**
     * Gets comments for a column. Used in metadata collection
     * @param tableName
     * @param columnName
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private String getColumnComment(String tableName, String columnName) throws ClassNotFoundException, SQLException {
        String columnComment = "";
        String commentQuery = "SELECT col_description(quote_ident('%s')::regclass::oid, (SELECT attnum FROM pg_attribute WHERE attrelid = quote_ident('%s')::regclass::oid AND attname = '%s')) as comment";
        boolean ret = runDBQuery(String.format(commentQuery, tableName, tableName, columnName));
        TableData columnCommentData = this.getTableData();
        if (!ret && columnCommentData.rows.size() > 0) {
            columnComment = columnCommentData.rows.get(0).cell.get("comment").getValue();
            if (columnComment == null) {
                columnComment = "";
            }
        }
        return columnComment;
    }
   
}
