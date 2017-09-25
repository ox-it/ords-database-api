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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.data.DataCell;
import uk.ac.ox.it.ords.api.database.data.DataRow;
import uk.ac.ox.it.ords.api.database.data.DataTypeMap;
import uk.ac.ox.it.ords.api.database.data.OrdsTableColumn;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.data.DataCell.DataType;
import uk.ac.ox.it.ords.security.model.DatabaseServer;
import uk.ac.ox.it.ords.security.services.ServerConfigurationService;

/**
 *
 * @author dave
 */
public class QueryRunner {
    private static Logger log = LoggerFactory.getLogger(QueryRunner.class);
    
    protected String dbErrorMessage;
    protected DatabaseServer dbc;
    protected String databaseName;
    private TableData tableData = null;
    
    
    public QueryRunner(String dbServer, String dbName) {
    	setCredentials(dbServer, dbName);
    }
    
    public QueryRunner(DatabaseServer server, String dbName){
    	setCredentials(server.getHost(), dbName);
    }
    
    /**
     * This class is required to allow classes that extend this class to omit defining
     * their constructors.
     */
    public QueryRunner() {
    	throw new UnsupportedOperationException();
    }
    
    
    public TableData getTableData() {
    	return tableData;
    }
    
    public String getCurrentDbName() {
    	return this.databaseName;
    }
    
    public String getCurrentDbServer() {
    	if (dbc == null) {
    		return null;
    	}
    	return dbc.getHost();
    }
    
    protected void setCredentials(String dbServer, String dbName) {
    	this.databaseName = dbName;
    	
    	try {
			if (dbServer == null){
				this.dbc = ServerConfigurationService.Factory.getInstance().getDatabaseServer();    		
			} else {
				this.dbc = ServerConfigurationService.Factory.getInstance().getDatabaseServer(dbServer);    		
			}
		} catch (Exception e) {
			
			log.error("Error locating database server",e);

		}
    } 
    
    protected Connection initialiseConnection() throws ClassNotFoundException, SQLException {
        return initialiseConnection(true);
    }
    
    /**
     * Initialise the database connection
     * @param logException if true then any exception should be logged. Set this to false
     * in the case of checking the database exists to not log an exception unnecessarily
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException 
     */
    protected Connection initialiseConnection(boolean logException) throws ClassNotFoundException, SQLException {

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(dbc.getUrl(databaseName), dbc.getUsername(), dbc.getPassword());
            conn.setAutoCommit(true);
        }
        catch (SQLException e) {
            if (logException) {
                log.error(String.format("Unable to initialise connection for user <%s> url <%s> with password %s",
                        dbc.getUsername(), dbc.getUrl(), //dbc.getPassword(),
                        dbc.getPassword() == null ? "null" : "not null"));
                log.error("Exception", e);
            }
            
            throw new SQLException (e);
        }        
        log.debug("initialiseConnection:return");
        
        return conn;
    }
    
    public Connection getConnection() throws ClassNotFoundException, SQLException{
    	return initialiseConnection();
    }
    
    public boolean runDBQuery(String query) throws ClassNotFoundException, SQLException {
        return runDBQuery(query, 1, 0, false);
    }
    
    public boolean runDBQuery(String query, int startRecord, int numberOfRecordsRequired, boolean readOnly) throws ClassNotFoundException, SQLException {
    	this.tableData = this.runDBQuery(query, new ParameterList(), startRecord, numberOfRecordsRequired, readOnly);
    	return tableData != null;
    }
      
    private void closeConnection(Connection conn) {
        log.trace("closeConnection");
        if (conn != null) {
	        try {
	            if (!conn.getAutoCommit()) {
	                conn.commit();
	            }
	            conn.close();
	        } catch (SQLException e) {
	            log.error("Cannot close the connection: ", e);
	        }
        }
    }
    
    /**
     * Get table data using a prepared statement.
     * 
     * Note that this method also runs the query without OFFSET and LIMIT to return the total number of rows without
     * pagination.
     * 
     * @param query
     * @param parameters
     * @param startRecord
     * @param numberOfRecordsRequired
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException 
     */
    public TableData runDBQuery(String query, ParameterList parameters, int startRecord, int numberOfRecordsRequired, boolean readOnly) throws ClassNotFoundException, SQLException {
        
        TableData tableData = new TableData();
        boolean pagination = true;
        
        if (numberOfRecordsRequired == 0) {
            pagination = false;
        }
        else {
            tableData.setCurrentRow(startRecord);
        }
        
        PreparedStatement getResults = null;
        PreparedStatement getResultsWithoutLimit = null;
        ResultSet rs = null;
        int numberOfResults = 0;
        Connection conn = null;
        try {
        	conn = initialiseConnection();
            conn.setReadOnly(readOnly);
        }
        catch (SQLException e) {
            log.error("No connection set", e);
            return null;
        }
        try {
	        try {
	            
	            int offset = 0;
	            if (startRecord > 0) {
	                offset = startRecord-1;
	            }
	            
	            getResultsWithoutLimit = conn.prepareStatement(query, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

	            if (numberOfRecordsRequired == 0) {
	                conn.setAutoCommit(true);
	            }
	            else {
	                if (!query.toLowerCase().contains("limit")) {
	                    query = createLimitAndOffsetCommand(query, numberOfRecordsRequired, offset);
	                    conn.setAutoCommit(false);
	                }   
	            }
	            
	            getResults = conn.prepareStatement(query, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
	            addParametersToPreparedStatement(getResults, parameters);
	            addParametersToPreparedStatement(getResultsWithoutLimit, parameters);


	        	
	        	//
	        	// Get the result set
	        	//
	            if (getResults.execute()) {
	                rs = getResults.getResultSet();
	                ResultSetMetaData md;
	                int colCount;
	
	                DataRow dr;
	                int counter = 0;
	                boolean firstTimeThrough = true;
	                
	                while (rs.next()) {
	                    md = rs.getMetaData();
	                    colCount = md.getColumnCount();
	                    dr = new DataRow();
	
	                    if (firstTimeThrough) {
	                        /*
	                         * During the first iteration we need to set up column data
	                         */
	                        firstTimeThrough = false;
	                        for (int colNum = 1; colNum <= colCount; colNum++) {
	                            OrdsTableColumn otc = new OrdsTableColumn();
	                            otc.columnName = md.getColumnName(colNum);
	                            otc.columnType = DataTypeUtils.getDataType(md.getColumnType(colNum));
	                            tableData.columns.add(otc);
	                            otc.orderIndex = colNum;
	                        }
	                    }
	                    for (int colNum = 1; colNum <= colCount; colNum++) {
	                        DataCell dc = new DataCell();
	                        dc.setType(DataTypeUtils.getDataType(md.getColumnType(colNum)));
	                        if (rs.getString(colNum) != null) {
	                            dc.setValue(rs.getString(colNum));
	                        } 
	                        dr.cell.put(md.getColumnName(colNum), dc);
	                    }
	
	                    tableData.rows.add(dr);
	                    counter++;
	                    
	                    if (pagination && (counter >= numberOfRecordsRequired)) {
	                        break;
	                    }
	                }
	                rs.last();
	                numberOfResults = rs.getRow();
	                rs.close();
	            }
	            
	            //
	            // Get the number of results before LIMIT is applied
	            //
	            if (pagination){
	            	if (getResultsWithoutLimit.execute()){
	            		rs = getResultsWithoutLimit.getResultSet();
	            		rs.last();
	            		numberOfResults = rs.getRow();
	            		rs.close();
	            	}
	            }
	        	tableData.setNumberOfRowsInEntireTable(numberOfResults);
	        	tableData.setNumberOfRowsReturnedByQuery(numberOfResults);
	        }
	        catch (SQLException ex) {
	            log.error("Exception", ex);
	            log.error("Database: " + databaseName);
	            log.error("User: " + dbc.getUsername());
	            log.error("Command: " + query);
	            dbErrorMessage = "" + ex;
	            log.error("SQLState: " + ex.getSQLState());
	            log.error("VendorError: " + ex.getErrorCode());
	            tableData = null;
	            throw ex;
	        }
	        finally {
	            if (rs != null) {
	                try {
	                    rs.close();
	                }
	                catch (SQLException sqlEx) {
	                    log.error("Cannot close the resultset: " + sqlEx.getMessage());
	                }
	            }
	            if (getResults != null) {
	                try {
	                	getResults.close();
	                }
	                catch (SQLException sqlEx) {
	                    log.error("Cannot close the statement: " + sqlEx.getMessage());
	                }
	            }
	            if (getResultsWithoutLimit != null) {
	                try {
	                	getResultsWithoutLimit.close();
	                }
	                catch (SQLException sqlEx) {
	                    log.error("Cannot close the statement: " + sqlEx.getMessage());
	                }
	            }
	        }
        }
        finally {
        	closeConnection(conn);
        }
        
        return tableData;
    }
    
	/**
	 * Creates and executes a prepared statement with a supplied map of typed values; used when no return is expected
	 * 
	 * @param statement the command to execute
	 * @param ParameterList parameters to include in the statement
	 * @return true if the statement executes successfully
	 * @throws ClassNotFoundException if there is a problem with the database driver
	 * @throws SQLException if there is a problem executing the SQL, typically a type validation problem, or a syntax error in the command
	 */
	public boolean createAndExecuteStatement(String statement, ParameterList parameters) throws ClassNotFoundException, SQLException {
        boolean ret = false;

        PreparedStatement pst = null;
        Connection conn = initialiseConnection();
                
        try {
        	pst = conn.prepareStatement(statement);
            addParametersToPreparedStatement(pst, parameters);                
            pst.executeUpdate();

            ret = true;
        } 
        catch (SQLException ex) {
            log.error("Exception", ex);
            dbErrorMessage = "" + ex;
            log.error("SQLState: " + ex.getSQLState());
            log.error("VendorError: " + ex.getErrorCode());
        }
        finally {
        	if (pst != null) {
            	pst.close();
            }
            if (conn != null) {
            	conn.close();
            }
        }
        
        if(dbErrorMessage != null) throw new SQLException(dbErrorMessage);
                        
        return ret;
    }

    /**
     * Apply a ParameterList to a PreparedStatement
     * @param pstmt
     * @param parameters
     * @throws SQLException
     */
    private void addParametersToPreparedStatement(PreparedStatement pstmt, ParameterList parameters) throws SQLException{
    	if (parameters != null ){
    		for (int i=0; i<parameters.size(); i++){
    			DataTypeMap parameter = parameters.getParameter(i);
    			addParameterToPreparedStatement(pstmt, parameter.index, parameter);
    		}
    	}
    }
    
    /**
     * Add a DataTypeMap as a parameter to a PreparedStatement
     * @param pst
     * @param index
     * @param dtm
     * @throws SQLException
     */
	private void addParameterToPreparedStatement(PreparedStatement pst, int index, DataTypeMap dtm) throws SQLException{
        if (dtm.dt.equals(DataType.INTEGER) 
                || dtm.dt.equals(DataType.BIGINT)
                || dtm.dt.equals(DataType.DECIMAL)
                || dtm.dt.equals(DataType.REAL)
                || dtm.dt.equals(DataType.DOUBLE)
                || dtm.dt.equals(DataType.NUMERIC)
                || dtm.dt.equals(DataType.DATE) 
                || dtm.dt.equals(DataType.TIME)
                || dtm.dt.equals(DataType.TIMESTAMP)
                || dtm.dt.equals(DataType.BINARY)) {
            // These types need to be explicitly nulled if empty
            try {
                if ( (dtm.stringValue == null) || (dtm.stringValue.length() == 0) ) {
                	pst.setNull(index, Types.NULL);
                }
                else {
                    // Cast numbers to the correct type
                    if (dtm.dt.equals(DataType.INTEGER)) {
                    	pst.setInt(index, Integer.parseInt(dtm.stringValue));
                    } else if (dtm.dt.equals(DataType.BIGINT)) {
                    	pst.setLong(index, Long.parseLong(dtm.stringValue));
                    } else if (dtm.dt.equals(DataType.REAL)) {
                    	pst.setFloat(index, Float.parseFloat(dtm.stringValue));
                    } else if (dtm.dt.equals(DataType.DOUBLE)) {
                    	pst.setDouble(index, Double.parseDouble(dtm.stringValue));
                    } else if (dtm.dt.equals(DataType.NUMERIC)) {
                    	pst.setBigDecimal(index, new BigDecimal(dtm.stringValue));
                    } else if (dtm.dt.equals(DataType.BINARY)) {
                        byte[] bytes = dtm.stringValue.getBytes();
                        pst.setBytes(index, bytes);
                    } else if (dtm.dt.equals(DataType.DECIMAL)) {
                    	pst.setBigDecimal(index, new BigDecimal(dtm.stringValue));
                    } else {
                        try {
                            // Parse dates
                            if (dtm.dt.equals(DataType.DATE)) {    
                                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                                Date date = new Date(df.parse(dtm.stringValue).getTime());
                                pst.setDate(index, date);
                            } else if (dtm.dt.equals(DataType.TIMESTAMP)){
                                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                                Timestamp timestamp = new Timestamp(df.parse(dtm.stringValue).getTime());
                                pst.setTimestamp(index, timestamp);
                            } else if (dtm.dt.equals(DataType.TIME)) {
                                SimpleDateFormat df = new SimpleDateFormat("hh:mm:ss");
                                Time time = new Time(df.parse(dtm.stringValue).getTime());
                                pst.setTime(index, time);
                            }
                        } catch (ParseException e) {
                            log.error("Date could not be parsed: "+e.getMessage());
                            throw new SQLException(e.getMessage());
                        }
                    }
                }
            }
            catch (NumberFormatException e) {
                log.error("Invalid data - probably an attempt to use a non-numeric value for a numeric field");
                throw new SQLException(e.getMessage());
            }
        } else if (dtm.dt.equals(DataType.BIT) || dtm.dt.equals(DataType.BOOLEAN)) {
            // Convert checkboxes to boolean
        	pst.setBoolean(index, dtm.stringValue.equals("t"));
        } else if (dtm.dt.equals(DataType.NULL)) {
        	pst.setNull(index, Types.NULL);
        } else {
            // For everything else (Varchar, text, char) just use the raw string
        	pst.setString(index, dtm.stringValue);
        }
	}
	
    private static String createLimitAndOffsetCommand(String command, int limit, int offset) {
        String fullCommand = command;
        
        if (fullCommand == null) {
            log.error("Null input");
        }
        else if ( (fullCommand.contains("limit(")) || (fullCommand.contains("limit (")) ) {
            log.debug("Limiter found");
            if ( (fullCommand.contains("offset(")) || (fullCommand.contains("offset (")) ) {
                log.debug("Offset found");
            }
            else {
                log.debug("No offset found");
                fullCommand += String.format(" offset(%d)", offset);
            }
        }
        else if ( (fullCommand.contains("offset(")) || (fullCommand.contains("offset (")) ) {
            log.debug("Offset found - no limiter");
            fullCommand += String.format(" limit(%d)", limit);
        }
        else {
            log.debug("Neither offset nor limit found");
            fullCommand += String.format(" limit(%d)  offset(%d)", limit, offset);
        }
        
        return fullCommand;
    }
}