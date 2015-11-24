/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.dbName
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
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.conf.DBCredentials;
import uk.ac.ox.it.ords.api.database.data.DataCell;
import uk.ac.ox.it.ords.api.database.data.DataRow;
import uk.ac.ox.it.ords.api.database.data.DataTypeMap;
import uk.ac.ox.it.ords.api.database.data.OrdsTableColumn;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.data.DataCell.DataType;

/**
 *
 * @author dave
 */
public class QueryRunner {
    private static Logger log = LoggerFactory.getLogger(QueryRunner.class);
    
    protected String dbErrorMessage;
    protected DBCredentials dbc;
    private TableData tableData = null;
    
    
    public QueryRunner(String dbServer, String dbName, String odbcUser, String odbcPassword) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("QR Constructor:%s,%s,%s", dbServer, dbName, odbcUser));
        }
    	setCredentials(dbServer, dbName);
    	setCredentialsSpecial(odbcUser, odbcPassword);
    }
    

    public QueryRunner(String dbServer) {
        if (log.isDebugEnabled()) {
            log.debug("Instantiating with server " + dbServer);
        }
    	setCredentials(dbServer);
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

    public String getDbErrorMessage() {
        return dbErrorMessage;
    }
    
    
    public String getCurrentDbName() {
    	if (dbc == null) {
    		return null;
    	}
    	return dbc.getDbName();
    }
    
    public String getCurrentDbServer() {
    	if (dbc == null) {
    		return null;
    	}
    	return dbc.getDbServer();
    }
    
    public String getCurrentDbUser() {
    	if (dbc == null) {
    		return null;
    	}
    	return dbc.getUser();
    }
    
    public String getCurrentDbPw() {
    	if (dbc == null) {
    		return null;
    	}
    	return dbc.getPassword();
    }
    
    
//    @Deprecated
//    public QueryRunner(String dbServer, String dbName) {
//    	setCredentials(dbServer, dbName);
//    }
    
    protected void setCredentials(String dbServer, String dbName) {
    	dbc = new DBCredentials(dbServer, dbName);
    }
    
    private void setCredentials(String dbServer) {
    	dbc = new DBCredentials(dbServer, "");
    }
    
    public void setCredentialsSpecial(String user, String password) {
        //log.debug("setCredentialsSpecial:"+user+":"+password);
    	dbc.setUser(user);
    	dbc.setPassword(password);
    }
    
    
    public boolean isDatabaseExist() {
    	boolean exists = true;
    	Connection conn = null;
    	try {
    		conn = initialiseConnection(false);
    	}
    	catch (ClassNotFoundException | SQLException e) {
    		exists = false;
    	}
    	finally {
    		closeConnection(conn);
    	}
    	
    	return exists;
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
        if (log.isTraceEnabled()) {
            log.trace("initialiseConnection");
            log.trace("jdbc driver is " + dbc.getJDBC_DRIVER());
            log.trace("dbUrl value is " + dbc.getDbUrl());
            log.trace("creds user value is " + dbc.getUser());
        }
        
        Class clazz = Class.forName(dbc.getJDBC_DRIVER());
            
        if (clazz == null) {
            log.warn("Null clazz");
        }

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(dbc.getDbUrl(), dbc.getUser(), dbc.getPassword());
            conn.setAutoCommit(true);
        }
        catch (SQLException e) {
            if (logException) {
                log.error(String.format("Unable to initialise connection for user <%s> url <%s> with password %s",
                        dbc.getUser(), dbc.getDbUrl(), //dbc.getPassword(),
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
        return runDBQuery(query, 1, 0);
    }
    
    
    
    
    public boolean runDBQuery(String query, int startRecord, int numberOfRecordsRequired) throws ClassNotFoundException, SQLException {
        if (query == null) {
            log.error("Null query - returning");
            return false;
        }
        if (log.isDebugEnabled()) {
            if (query.toLowerCase().contains("password")) {
                int index = query.toLowerCase().indexOf("password");
                log.debug(String.format("runDBQuery: about to run command %s ...", query.substring(0, index)));
            }
            else {
                log.debug("runDBQuery: about to run command: " + query);
            }
            if (log.isTraceEnabled()) {
	            log.trace("Number of records required:" + numberOfRecordsRequired);
	            log.trace("Starting record:" + startRecord);
	            log.trace("User:" + dbc.getUser());
	            log.trace("Server:" + dbc.getDbServer());
	            log.trace("DB:" + dbc.getDbName());
            }
        }
        boolean ret = false;
        tableData = null;
        boolean pagination = true;
        
        if (numberOfRecordsRequired == 0) {
            //log.debug("All records needed");
            pagination = false;
        }
        else {
        	tableData = new TableData();
            tableData.setCurrentRow(startRecord);
        }
        
        Statement stmt = null;
        ResultSet rs = null;
        
        Connection conn = null;
        try {
        	conn = initialiseConnection();
        }
        catch (SQLException e) {
            log.error("No connection set", e);
            throw new SQLException (e);
        }
        
        try {
	        try {
	            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
	            
	            int offset = 0;
	            if (startRecord > 0) {
	                offset = startRecord-1;
	            }
	            if (numberOfRecordsRequired == 0) {
	                conn.setAutoCommit(true);
	            }
	            else {
	                if (!query.toLowerCase().contains("limit")) {
	                    query = createLimitAndOffsetCommand(query, numberOfRecordsRequired, offset);
	                    conn.setAutoCommit(false);
	                }   
	            }
	            
	            if (stmt.execute(query)) {
	            	/*
	            	 * There are results to collect
	            	 */
	                rs = stmt.getResultSet();
	                ResultSetMetaData md;
	                int colCount;
	
	                DataRow dr;
	                int counter = 0;
	                boolean firstTimeThrough = true;
	                
	                if (tableData == null) {
	                	tableData = new TableData();
	                }
	                
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
	                            otc.columnType = getDataType(md.getColumnType(colNum));
	                            tableData.columns.put(otc.columnName, otc);
	                            otc.orderIndex = colNum;
	                        }
	                    }
	                    for (int colNum = 1; colNum <= colCount; colNum++) {
	                        DataCell dc = new DataCell();
	                        dc.setType(getDataType(md.getColumnType(colNum)));
	                        if (rs.getString(colNum) != null) {
	                            dc.setValue(rs.getString(colNum));
	                        } 
	                        dr.cell.put(md.getColumnName(colNum), dc);
	                    }
	
	                    tableData.rows.put(counter, dr);
	                    counter++;
	                    
	                    if (pagination && (counter >= numberOfRecordsRequired)) {
	                        break;
	                    }
	                }
	                rs.last();
	                tableData.setNumberOfRowsInEntireTable(rs.getRow());
	                rs.close();
	            }
	            else {
	            	// This was either an update count or a query that provided no results.
	            }
	            ret = true;
	        }
	        catch (SQLException ex) {
	            log.error("Exception", ex);
	            log.error("Database: " + dbc.getDbName());
	            log.error("User: " + dbc.getUser());
	            log.error("Command: " + query);
	            dbErrorMessage = "" + ex;
	            log.error("SQLState: " + ex.getSQLState());
	            log.error("VendorError: " + ex.getErrorCode());
	            tableData = null;
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
	            if (stmt != null) {
	                try {
	                    stmt.close();
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
        
        if (log.isDebugEnabled()) {
            log.debug("Returning from the Query Runner with ret:" + (ret ? "true" : "false"));
        }
        
        return ret;
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
    
    
    
    
    public static DataType getDataType(int ct) {
        DataType dt;
        // See java.sql.Types
        switch (ct) {
            case 2003: dt = DataType.ARRAY;
				break;
            case -5: dt = DataType.BIGINT;
				break;
            case -2: dt = DataType.BINARY;
				break;
            case -7: dt = DataType.BOOLEAN;
                // -7 is really BIT, but JDBC sees BOOLEANs as BITs and
                // we can't save BITs with PreparedStatement, so it's easiest
                // to pretend its a BOOLEAN.
				break;
            case 2004: dt = DataType.BLOB;
				break;
            case 16: dt = DataType.BOOLEAN;
				break;
            case 1: dt = DataType.CHAR;
				break;
            case 2005: dt = DataType.CLOB;
				break;
            case 70: dt = DataType.DATALINK;
				break;
            case 91: dt = DataType.DATE;
				break;
            case 3: dt = DataType.DECIMAL;
				break;
            case 2001: dt = DataType.DISTINCT;
				break;
            case 8: dt = DataType.DOUBLE;
				break;
            case 6: dt = DataType.FLOAT;
				break;
            case 4: dt = DataType.INTEGER;
				break;
            case 2000: dt = DataType.JAVA_OBJECT;
				break;
            case -16: dt = DataType.LONGNVARCHAR;
				break;
            case -4: dt = DataType.LONGVARBINARY;
				break;
            case -1: dt = DataType.LONGVARCHAR;
				break;
            case -15: dt = DataType.NCHAR;
				break;
            case 2011: dt = DataType.NCLOB;
				break;
            case 0: dt = DataType.NULL;
				break;
            case 2: dt = DataType.NUMERIC;
				break;
            case -9: dt = DataType.NVARCHAR;
				break;
            case 1111: dt = DataType.OTHER;
				break;
            case 7: dt = DataType.REAL;
				break;
            case 2006: dt = DataType.REF;
				break;
            case -8: dt = DataType.ROWID;
				break;
            case 5: dt = DataType.SMALLINT;
				break;
            case 2009: dt = DataType.SQLXML;
				break;
            case 2002: dt = DataType.STRUCT;
				break;
            case 92: dt = DataType.TIME;
				break;
            case 93: dt = DataType.TIMESTAMP;
				break;
            case -6: dt = DataType.TINYINT;
				break;
            case -3: dt = DataType.VARBINARY;
				break;
            case 12: dt = DataType.VARCHAR;
				break;
            default: dt = DataType.OTHER;
                break;
        }

        return dt;
    }
    
    
    /**
     * The following is taken from http://www.postgresql.org/docs/9.1/static/datatype.html
     * There seems to be a disconnect between what Postgres knows as a datatype and what
     * ORDS might expect (e.g. macaddr is a Postgres datatype we don't know about ... similarly
     * we have space for DataType.BLOB that doesn't appear as a Postgres datatype).
     * TODO more work needed here
     * @param type
     * @return the datatype or DataType.OTHER if unknown
     */
    public static DataType getDataType(String type) {
        DataType dt;
        if (type == null) {
        	return null;
        }
        if (type.equals("character varying")) {
        	return DataType.VARCHAR;
        }
        if (type.equals("integer")) {
        	return DataType.INTEGER;
        }
        if (type.startsWith("timestamp")) {
        	return DataType.TIMESTAMP;
        }
        if (type.equals("boolean")) {
        	return DataType.BOOLEAN;
        }
        if (type.equals("bigint")) {
        	return DataType.BIGINT;
        }
        if (type.equals("numeric")) {
        	return DataType.DECIMAL;
        }
        if (type.equals("date")) {
        	return DataType.DATE;
        }
        if (type.startsWith("double ")) {
        	return DataType.DOUBLE;
        }
        if (type.equals("xml")) {
        	return DataType.SQLXML;
        }
        if (type.equals("character")) {
        	return DataType.CHAR;
        }
        if (type.equals("time without time zone")) {
        	return DataType.TIME;
        }
        if (type.equals("time")) {
        	return DataType.TIME;
        }
        if (type.equals("timestamp")) {
        	return DataType.TIMESTAMP;
        }
        if (type.equals("smallint")) {
        	return DataType.SMALLINT;
        }
        if (type.equals("real")) {
        	return DataType.REAL;
        }
        
        return DataType.OTHER;
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
     */
    public TableData runDBQuery(String query, ParameterList parameters, int startRecord, int numberOfRecordsRequired) throws ClassNotFoundException {
        if (log.isDebugEnabled()) {
            log.debug("runDBQuery: about to run command: " + query);
            log.debug("Number of records required:" + numberOfRecordsRequired);
            log.debug("Starting record:" + startRecord);
            log.debug("User:" + dbc.getUser());
            log.debug("Server:" + dbc.getDbServer());
            log.debug("DB:" + dbc.getDbName());
        }
        
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
	                            otc.columnType = getDataType(md.getColumnType(colNum));
	                            tableData.columns.put(otc.columnName, otc);
	                            otc.orderIndex = colNum;
	                        }
	                    }
	                    for (int colNum = 1; colNum <= colCount; colNum++) {
	                        DataCell dc = new DataCell();
	                        dc.setType(getDataType(md.getColumnType(colNum)));
	                        if (rs.getString(colNum) != null) {
	                            dc.setValue(rs.getString(colNum));
	                        } 
	                        dr.cell.put(md.getColumnName(colNum), dc);
	                    }
	
	                    tableData.rows.put(counter, dr);
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
	        }
	        catch (SQLException ex) {
	            log.error("Exception", ex);
	            log.error("Database: " + dbc.getDbName());
	            log.error("User: " + dbc.getUser());
	            log.error("Command: " + query);
	            dbErrorMessage = "" + ex;
	            log.error("SQLState: " + ex.getSQLState());
	            log.error("VendorError: " + ex.getErrorCode());
	            tableData = null;
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
     * Apply a ParameterList to a PreparedStatement
     * @param pstmt
     * @param parameters
     * @throws SQLException
     */
    public void addParametersToPreparedStatement(PreparedStatement pstmt, ParameterList parameters) throws SQLException{
    	if (parameters != null ){
    		for (int i=0; i<parameters.size(); i++){
    			DataTypeMap parameter = parameters.getParameter(i);
    			log.debug(String.format("adding parameter %s", parameter.stringValue));
    			addParameterToPreparedStatement(pstmt, i + 1, parameter);
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
	public void addParameterToPreparedStatement(PreparedStatement pst, int index, DataTypeMap dtm) throws SQLException{
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
                log.error("Unable to get data - internal corruption");
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
	
    public static String createLimitAndOffsetCommand(String command, int limit, int offset) {
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

	
	
	/*
	 * For Mark's Rest stuff
	 */
	public PreparedStatement getPreparedStatement(String query) throws SQLException, ClassNotFoundException {
		Connection conn = null;

		PreparedStatement pst = null;
		try {
			conn = initialiseConnection();
			pst = conn.prepareStatement(query);
		}
		catch (SQLException e) {
			if (conn != null) {
				conn.close();
			}
		}
		
		
		return pst;
	}
}
