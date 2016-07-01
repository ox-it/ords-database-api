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
    public static final String MAIN_DB_DOES_NOT_EXIST_DEPRICATED = "Main database does not exist.";
    
    /*
     * Constructors ...
     */
	public ORDSPostgresDB(String dbServer, String dbName) {
		super(dbServer, dbName);
    }
    
    /*
     * ... Constructors
     */
    
    
    /*
     * Work on column information ...
     */
    public TableData getColumnNamesForTable(String tableName) throws ClassNotFoundException, SQLException {
        if (log.isDebugEnabled()) {
            log.debug(String.format("getColumnNamesForTable(%s)", tableName));
        }

        String command = String.format("select column_name from information_schema.columns where table_name = '%s';",
                tableName);
        TableData td = null;
        if (runDBQuery(command)) {
        	td = this.getTableData();
        	td.tableName = tableName;
        }
        
        return td;
    }
    
    
//    public TableData getColumnDataTypesForTableTest(String tableName) throws ClassNotFoundException, SQLException {
//    	return getColumnDataTypesForTable(tableName);
//    }
    
    
    private TableData getColumnDataTypesForTable(String tableName) throws ClassNotFoundException, SQLException {
        if (log.isDebugEnabled()) {
            log.debug(String.format("getColumnDataTypesForTable(%s)", tableName));
        }

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
    *
    * @param tableName
    * @return the primary key of the table, but only if there is one (it will return the first entry if more than one),
    * else null
    */
   public String getSingularPrimaryKeyColumn(String tableName) throws ClassNotFoundException, SQLException {
       log.debug("getSingularPrimaryKeyColumn");
       String command = String
               .format("SELECT pg_attribute.attname,  format_type(pg_attribute.atttypid, pg_attribute.atttypmod)  FROM pg_index, pg_class, pg_attribute WHERE pg_class.oid = '\"%s\"'::regclass AND indrelid = pg_class.oid AND pg_attribute.attrelid = pg_class.oid AND pg_attribute.attnum = any(pg_index.indkey) AND indisprimary",
               tableName);

       if (!runDBQuery(command)) {
    	   return null;
       }

       /*
        * Note We shall currently restrict this function to only return the
        * primary key if there is a single primary key. Maybe we can extend that
        * later
        */
       if (this.getTableData().rows.size() >= 1) {
           for (DataRow dr : this.getTableData().rows) {
               String primaryKey = dr.cell.get("attname").getValue();
               return primaryKey;
           }
       }

       return null;
   }
   
   
   


   

   
   public OrdsTableColumn addReferencesToColumn(OrdsTableColumn otc, TableData constraints) throws SQLException {
       try {
           for (DataRow constraint : constraints.rows) {
               if (log.isDebugEnabled()) {
                   log.debug(String.format("Check column name %s against %s", otc.columnName, constraint.cell.get("column_name").getValue()));
               }
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
                   if (log.isDebugEnabled()) {
                       log.debug(String.format("Setting up ref col %s and ref table %s with %d alternatives", otc.referencedColumn, otc.referencedTable, otc.alternateColumns.size()));
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
       if (log.isDebugEnabled()) {
           log.debug(String.format("getTablesForDatabase()"));
       }
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
        if (log.isDebugEnabled()) {
            log.debug(String.format("getTableDataForTable(%s, %d, %d)", tableName, rowStart, numberOfRowsRequired));
        }
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

        /*
         * Get all table names in the database and ensure the table exists
         */
        boolean exists = false;
        QueryRunner lqr = new QueryRunner(this.getCurrentDbServer(), this.getCurrentDbName());
        if (!lqr.runDBQuery(String.format("SELECT table_name FROM information_schema.tables WHERE table_schema='%s'", CommonVars.SCHEMA_NAME))) {
            return null;
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
        if (!exists) {
            log.info(String.format("Table %s does not exist", tableName));
            return null;
        }

        String tableComment = "";
        
        ArrayList<String> sequences = getSequencesForTable(tableName);


        if (!runDBQuery("select count(*) from \"" + tableName+"\"")) {
            return null;
        }
        String totalRows = this.getTableData().rows.get(0).cell.get("count").getValue();
        int totalRowsInt = 0;
        if (totalRows == null) {
            log.error("Null value for total rows. Should not happen!");
        }
        else {
            totalRowsInt = Integer.parseInt(totalRows);
            boolean ret = runDBQuery("SELECT obj_description(quote_ident('"+tableName+"')::regclass::oid, 'pg_class') as comment");
            TableData commentData = this.getTableData();
            if (ret && commentData.rows.size() > 0) {
                tableComment = commentData.rows.get(0).cell.get("comment").getValue();
                if (tableComment == null) {
                    tableComment = "";
                }
            }
            if (totalRowsInt == 0) {
                // There are no rows in the table. Just set up column information and return
                log.info("No rows in this table");

                TableData tableData = getColumnDataTypesForTable(tableName);
                TableData constraints = getForeignConstraintsForTable(tableName);
                
                tableData.columns.clear();
                int i = 0;
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
                tableData.setNumberOfRowsInEntireTable(totalRowsInt);
                tableData.comment = tableComment;

                return tableData;
            }
        }


        String keyName = null;
        try {
            keyName = getSingularPrimaryKeyColumn(tableName);
        }
        catch (ClassNotFoundException ex) {
            log.error("Class error - unable to find index", ex);
        }
        catch (SQLException ex) {
            log.error("Unable to find index", ex);
        }

        log.debug("About to run the select statement");
        TableData tableData = null;
        if ( (sort == null) || (sort.equals("null")) ) {
            if (numberOfRowsRequired == 0) {
                if (keyName == null) {
                    log.debug("No key specified - simple select");
                    runDBQuery(String.format("select * from \"%s\"", tableName), databaseRowStart, numberOfRowsRequired, true);
                }
                else {
                    log.debug("Key specified");
                    runDBQuery(String.format("select * from \"%s\" order by \"%s\"", tableName, keyName), databaseRowStart, numberOfRowsRequired, true);
                }
            }
            else {
                if (keyName == null) {
                    log.debug("No key specified - simple select with limit");
                    runDBQuery(String.format("select * from \"%s\" limit %d offset %d", tableName, numberOfRowsRequired, databaseRowStart));
                }
                else {
                    log.debug("Key specified - select with limit");
                    runDBQuery(String.format("select * from \"%s\" order by \"%s\" limit %d offset %d", tableName, keyName, numberOfRowsRequired, databaseRowStart));
                }
            }
        }
        else {
            if (numberOfRowsRequired == 0) {
                runDBQuery(String.format("select * from \"%s\" order by \"%s\" %s", tableName, sort, direction ? "asc" : "desc"), databaseRowStart, numberOfRowsRequired, true);
            }
            else {
                runDBQuery(String.format("select * from \"%s\" order by \"%s\" %s limit %d offset %d", tableName, sort, direction ? "asc" : "desc", numberOfRowsRequired, databaseRowStart));
            }
        }
        tableData = this.getTableData();
        if (tableData == null) {
            log.error("Null table data returned - can't do anything with this");
            return null;
        }
        tableData.setNumberOfRowsInEntireTable(totalRowsInt);
        tableData.setCurrentRow(databaseRowStart);
        tableData.tableName = tableName;
        tableData.comment = tableComment;


        /*
         * Now we have basic data. Let's get more details on the structure of the table.
         */
        TableData td = new TableData(tableData);

        List<String> primKeys = new ArrayList<String>();
        primKeys.add(keyName);
        if (log.isDebugEnabled()) {
            log.debug("Adding primary key of " + keyName);
        }
        td.sequences = sequences;
        td.primaryKeys = primKeys;


        TableData td2 = getForeignConstraintsForTable(tableName);

        // Look at each column in our current data
        log.debug("About to loop through columns");
        for (OrdsTableColumn column : td.columns) {
            if (td2 != null && !td2.rows.isEmpty()) {
                column = addReferencesToColumn(column, td2);
            }
            column.comment = getColumnComment(td.tableName, column.columnName);
        }


        log.debug("getTableDataForTable:Returning...");
        return td;
    }
    
    /**
     * Get any auto-increment sequences for the specified table
     * @param tableName
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public ArrayList<String> getSequencesForTable(String tableName) throws ClassNotFoundException, SQLException {
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

//    /**
//     * Returns the data from a table. This does a lot more than just querying the data. It also understands the
//     * structure of the table and is able to cross reference other tables for their data if necessary.
//     *
//     * @param tableName the table whose data is to be returned
//     * @param rowStart initial row of data that needs to be viewed
//     * @param tableColumnReferences A HashMap of foreign keys from the table, and the foreign values to display in their place
//     * @param rowsPerPage the number of rows of data to be displayed per page
//     * @param sort the name of the column to sort by, or null if no sorting needed
//     * @param direction if sort is specified then this boolean will show if sorting is in normal or reverse order
//     * @return a TableData object containing all the data for a specific table or null if there has been a problem
//     * @throws SQLException 
//     */
//    public TableData getTableDataForTable(String tableName, int rowStart, HashMap<String, String> tableColumnReferences, int rowsPerPage, String sort, boolean direction) throws ClassNotFoundException, SQLException {
//        if (log.isDebugEnabled()) {
//            String tcr;
//            try {
//                tcr = tableColumnReferences.toString();
//            } catch (NullPointerException e) {
//                tcr = "null";
//            }
//            log.debug(String.format("getTableDataForTable(%s, %s, %d)",
//                    tableName, tcr, rowStart));
//        }
//        if (tableName == null) {
//            log.error("Null table name - cannot proceed");
//            return null;
//        }
//
//        /*
//         * First up, let's get the data that would be viewed
//         */
//        TableData baseTableData = getTableDataForTable(tableName, rowStart, rowsPerPage, sort, direction);
//        if (baseTableData == null) {
//            log.error("Null tabledata - this is bad");
//            return null;
//        }
//        if (!baseTableData.tableName.equals(tableName)) {
//            log.error("Coding error - table name set incorrectly. Correcting ...");
//            baseTableData.tableName = tableName;
//        }
//        
//        /*
//         * We need to understand if there are any foreign keys associated with this table and, if so, add
//         * them to the data.
//         */
//        if (tableColumnReferences != null) {
//        	addTableColumnReferences(tableName, baseTableData,tableColumnReferences);
//        }
//
//        baseTableData.logData();
//        baseTableData.setCurrentRow(rowStart);
//        return baseTableData;
//    }

    
//    /**
//     * Adds foreign key references to a table result
//     * @param tableName the table
//     * @param baseTableData the base table data (results)
//     * @param tableColumnReferences A HashMap of foreign keys from the table, and the foreign values to display in their place
//     * @throws ClassNotFoundException
//     * @throws SQLException
//     */
//    public void addTableColumnReferences(String tableName, TableData baseTableData, HashMap<String, String> tableColumnReferences) throws ClassNotFoundException, SQLException{
//    	{
//            TableData constraintData = getForeignConstraintsForTable(tableName);
//
//            /*
//             * The constraintData object now contains constraint data. Due to the nature of
//             * result sets, this constraint data is contained within the row information of
//             * the object.
//             */
//            if (constraintData.rows != null) {
//                /*
//                 * Look at each row to get the constraint information
//                 */
//                if (log.isDebugEnabled()) {
//                    log.debug(String.format("There are %d constraint rows", constraintData.rows.size()));
//                }
//                for (int index = 0; index < constraintData.rows.size(); index++) {
//                    // Get the name of the referree table
//                    String requiredReferencedTable = constraintData.rows.get(index).cell.get("foreign_table_name").getValue();
//                    String requiredReferencingTableIndex = constraintData.rows.get(index).cell.get("foreign_column_name").getValue();
//                    String referencingColumn = constraintData.rows.get(index).cell.get("column_name").getValue();
//
//                    if (log.isDebugEnabled()) {
//                        log.debug(String.format("Req ref table is %s", requiredReferencedTable));
//                        log.debug(String.format("Req ref table index is %s", requiredReferencingTableIndex));
//                    }
//                    // find the reference column
//                    OrdsTableColumn col = null;
//                    for (OrdsTableColumn column: baseTableData.columns) {
//                    	if ( column.columnName.equals(referencingColumn)) {
//                    		col = column;
//                    		break;
//                    	}
//                    }
//                    if (col == null) {
//                        log.error("Null column information for " + referencingColumn);
//                        continue;
//                    }
//                    //sort it here
//                    boolean b1 = false, b2 = false;
//                    if ( (requiredReferencedTable != null) && (requiredReferencedTable.equals(col.referencedTable))) {
//                        b1 = true;
//                    }
//                    if ( (requiredReferencingTableIndex != null) && (col.referencedColumn != null)){
//                    	if (requiredReferencingTableIndex.endsWith(col.referencedColumn)) {
//                        b2 = true;
//                    	}
//                    }
//
//
//                    /*
//                     * Now we need to replace all the data in referencingColumn with the data in requiredReferencedColumn.
//                     * First, we need to get the relevant data from the target table.
//                     */
//                    String command;
//                    if (b1 && b2) {
//                        if (log.isDebugEnabled()) {
//                            log.debug(String.format("RefTable is %s", requiredReferencedTable));
//                            log.debug(String.format("RefIndex is %s", requiredReferencingTableIndex));
//                        }
//
//                        /*
//                         * Now we should set up column information
//                         */
//                        String requiredReferencedColumn = tableColumnReferences.get(referencingColumn);
//                        if (requiredReferencedColumn != null) {
//                            command = String.format("select \"%s\" from \"%s\"", requiredReferencedColumn, requiredReferencedTable);
//                            TableData td;
//                            runDBQuery(command);
//                            List<String> alternativeOptions = new ArrayList<>();
//                            if (getTableData() == null) {
//                                log.error("No table data present. Something has gone wrong. Unable to continue here!");
//                                break;
//                            }
//                            for (DataRow row : getTableData().rows) {
//                                alternativeOptions.add(row.cell.get(requiredReferencedColumn).getValue());
//                            }
//
//                            td = getColumnNamesForTable(requiredReferencedTable);
//                            List<String> alternateColumns = new ArrayList<String>();
//                            for (DataRow row : td.rows) {
//                                alternateColumns.add(row.cell.get("column_name").getValue());
//                            }
//
//                            log.debug("Looping through columns");
//                            for (OrdsTableColumn otc : baseTableData.columns) {
//                                if (otc.columnName.equals(referencingColumn)) {
//                                    otc.alternativeOptions = alternativeOptions;
//                                    otc.alternateColumns = alternateColumns;
//                                    otc.referencedColumn = requiredReferencedColumn;
//                                    otc.referencedTable = requiredReferencedTable;
//                                    break;
//                                }
//                            }
//                        }
//                    }
//                    else {
//                        if (log.isDebugEnabled()) {
//                            log.debug(String.format("%s == %s?", requiredReferencedTable, col.referencedTable));
//                            log.debug(String.format("%s == %s?", requiredReferencingTableIndex, col.referencedColumn));
//                        }
//                    }
//                }
//            }
//        }
//    }

    

    /*
     * Constraints ...
     */
    /**
     * This function will return the table name referenced by the foreign key in the table provided.
     *
     * @param tableName the table to run the query against
     * @return a String containing the name of the table that is referenced. If more than one table exists, it will
     * return the first table it comes across. If there are no foreign tables it will return null.
     * @throws SQLException 
     */
    public String getSingularForeignConstraintTableName(String tableName) throws ClassNotFoundException, SQLException {
        log.debug("getSingularForeignConstraintTableName");

        String command = String
                .format("SELECT ccu.table_name AS foreign_table_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name='%s'",
                tableName);
        if (!runDBQuery(command)) {
        	return null;
        }

        if (this.getTableData().rows.size() >= 1) {
            for (DataRow dr : this.getTableData().rows) {
                return dr.cell.get("foreign_table_name").getValue();
            }
        }

        return null;
    }

    /**
     * Provide the foreign constraints for a table. Using this function, the caller will be provided with all
     * relationship information about the table.
     *
     * @param tableName the table whose information is required.
     * @return a TableData object with all the information
     * @throws SQLException 
     */
    public TableData getForeignConstraintsForTable(String tableName) throws ClassNotFoundException, SQLException {
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
    /*
     * ... Constraints
     */
    
//    public boolean runGenericQuery(String query, int rowStart, int rowsPerPage) throws ClassNotFoundException, SQLException {
//    	return runDBQuery(query, rowStart, rowsPerPage);
//    }
    
    
//    public boolean runGenericQuery(String query) throws ClassNotFoundException, SQLException {
//    	return runDBQuery(query);
//    }

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
            String query = String.format("SELECT DISTINCT \"%s\" AS value, \"%s\" AS label FROM \"%s\" ORDER BY label ASC", primaryKey, referencedColumn, referencedTable);
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
     * Used for updating selection controls that don't use drop-downs
     * @param table
     * @param foreignKey
     * @param referencedTable
     * @param referencedColumn
     * @param offset
     * @param limit
     * @return
     */
    public TableData getReferenceValues(String table, String foreignKey, String referencedTable, String referencedColumn, int offset, int limit, String sort, boolean direction, String filter, ParameterList parameters){
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
            String sql = String.format("SELECT %1$s.%2$s AS id, "
            		+ "%3$s.%5$s AS value, %3$s.%4$s AS label "
            		+ "FROM %1$s LEFT OUTER JOIN %3$s ON %3$s.%5$s = %1$s.%6$s "
            		+ "ORDER BY %1$s.%7$s %8$s", table, originPrimaryKey, referencedTable, referencedColumn, primaryKey, foreignKey, sort, direction? "ASC" : "DESC");
            
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
            		// Replace any aliases in the filter with table names
            		//
					filter = this.normaliseFilterQuery(filter);
            		
            		//
            		// Just include WHERE clause from filter
            		//
					filter = filter.split("WHERE")[1];

		          	sql = String.format("SELECT %1$s.%2$s AS id, "
	                		+ "%3$s.%5$s AS value, %3$s.%4$s AS label "
	                		+ "FROM %1$s LEFT OUTER JOIN %3$s ON %3$s.%5$s = %1$s.%6$s "
	                		+ "WHERE %1$s.%2$s IN (SELECT %2$s FROM %1$s WHERE %9$s) "
	                		+ "ORDER BY %1$s.%7$s %8$s", table, originPrimaryKey, referencedTable, referencedColumn, primaryKey, foreignKey, sort, direction? "ASC" : "DESC", filter);
	  
				} catch (Exception e) {
					log.debug("getReferenceValues: filter did not contain a WHERE clause");
				}
            }

            //SELECT documents.docid AS id, sites.id AS value, sites.sitename AS label FROM documents LEFT OUTER JOIN sites ON sites.id = documents.docsite ORDER BY docsite ASC LIMIT 100 OFFSET 0 
            
            log.debug("Getting references values using SQL:" + sql);
            referenceValues = runDBQuery(sql, parameters, offset, limit, true);
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
     * This data is used for creating select lists by {@link GeneralUtils.buildReferenceDataSelect}
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
     * Get a list of all tables associated with a particular database
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
     * Get a list of all tables associated with a particular database
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

    public String getColumnComment(String tableName, String columnName) throws ClassNotFoundException, SQLException {
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
    
    
	public String normaliseFilterQuery(String filter){
		//
		// We need to replace any temporary identifiers from RedQueryBuilder with the actual referenced table names
		//
		// RQB gives us filters that look like this:
		//
		// SELECT * FROM "documents" INNER JOIN "sites" "x0" ON "docsite" = "x0"."id" WHERE ("x0"."sitename" = ?)"
		//
		// We need to make this instead:
		//
		// SELECT * FROM "documents" INNER JOIN "sites" ON "docsite" = "sites"."id" WHERE ("sites"."sitename" = ?)"
		//
		// Ideally we should be able to specify within RQB not to use these aliases at all ...
		//
		// Note also here the 20 is an arbitrary number - if tables have more than 20 foreign key relations
		// with filter conditions attached then this is going to fail.
		//
		for (int i = 0; i < 20; i++){
			String identifier = String.format("\"x%d\"", i);
			//
			// Potentially we could get problems with legitimate uses of "x0" in queries
			// so we check for '"x0" ON' which should be uniquely used by these cases
			//
			if (filter.contains(identifier+" ON ")){
				//
				// Find the table identifier used before the identifier
				// first take the part form before the first use of the identifier
				// e.g. SELECT * FROM "documents" INNER JOIN "sites" 
				//
				String part = filter.split(identifier + " ON ")[0].trim();
				//
				// Now get the last word
				//
				String name = part.substring(part.lastIndexOf(" ")+1);
				//
				// ... and replace all occurences where the identifier is used
				//
				filter = filter.replaceAll(identifier, name);
			} else {
				//
				// Another case is a simpler filter such as:
				//
				// SELECT "x0".* FROM "city" "x0" WHERE ("x0"."population" > ?)
				//
				// For this we get the table identifier from just before the WHERE clause
				//
				if (filter.contains(identifier + " WHERE")){
					String part = filter.split(identifier + " WHERE")[0].trim();
					//
					// Now get the last word
					//
					String name = part.substring(part.lastIndexOf(" ")+1);
					//
					// ... and replace all occurences where the identifier is used
					//
					filter = filter.replaceAll(identifier, name);
				}
			}
			
		}
		return filter;
	}

    
    
    
    /*
     * Static routines
     */
    
}
