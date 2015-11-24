package uk.ac.ox.it.ords.api.database.queries;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.rowset.CachedRowSet;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.conf.CommonVars;
import uk.ac.ox.it.ords.api.database.data.DataCell.DataType;
import uk.ac.ox.it.ords.api.database.data.DataTypeMap;

import com.sun.rowset.CachedRowSetImpl;


public class DatabaseQueries extends QueryRunner {
	private static Logger log = LoggerFactory.getLogger(DatabaseQueries.class);
	
	public DatabaseQueries(String dbServer, String dbName, String odbcUser, String odbcPassword) {
		super(dbServer, dbName, odbcUser, odbcPassword);
	}
	
	public DatabaseQueries(String dbServer) {
		super(dbServer);
	}
    
    
    
    public void setDbOwner(String owner) throws ClassNotFoundException, SQLException {
        String query = String.format("alter database \"%s\" owner to %s", getCurrentDbName(), owner);
        runDBQuery(query);
        query = String.format("alter schema \"%s\" owner to %s", CommonVars.SCHEMA_NAME, owner);
        runDBQuery(query);
        for (String table : getAllTables()) {
            query = String.format("alter table \"%s\" owner to %s", table, owner);
            runDBQuery(query);
        }
    }
    
	
    /**
     * Get list of all databases on the server
     * @return
     * @throws SQLException 
     */
	public List<String> getDbList() throws SQLException {
		String query = "SELECT datname FROM pg_database WHERE datistemplate = false;";
		List<String> databases = new ArrayList<>();

        Connection conn = null;

		ResultSet rs = null;
		PreparedStatement pst = null;
		try {
			conn = initialiseConnection();
			pst = conn.prepareStatement(query);

			rs = pst.executeQuery();

			while (rs.next()) {
				databases.add(rs.getString(1));
	        }
		}
		catch (SQLException | ClassNotFoundException e) {
			log.error("Error", e);
		}
		finally {
			if (rs != null) {
				rs.close();
			}
			if (pst != null) {
				pst.close();
			}
			if (conn != null) {
				conn.close();
			}
		}


        return databases;
	}


	public boolean createAndExecuteStatement(String query, Map<String, DataTypeMap> dataTypeMap, String tableName) throws ClassNotFoundException, SQLException {
        return createAndExecuteStatement(query, dataTypeMap, tableName, null, null);    
    }
	
	
	public boolean createAndExecuteStatement(String updateString, Map<String, DataTypeMap> dataTypeMap, String tableName, String primKey, String originalKeyValue) throws ClassNotFoundException, SQLException {
        boolean ret = false;
        if (log.isDebugEnabled()) {
        	int mapSize = dataTypeMap.size();
            log.debug(String.format("createAndExecuteStatement(%s, %s, %s, %s), map size %d", updateString, primKey, tableName, originalKeyValue, mapSize));
            if (mapSize > 0) {
            	for (DataTypeMap dtm : dataTypeMap.values()) {
            		log.debug(String.format("map (%s)", dtm.stringValue));
            	}
            }
        }

        PreparedStatement pst = null;
        Connection conn = null;
        try {
        	conn = initialiseConnection();
        }
        catch (SQLException e) {
            log.error("No connection set", e);
            return false;
        }
        
        try {
        	pst = conn.prepareStatement(updateString);
            
            if (log.isDebugEnabled()) {
                log.debug(String.format("Primary key is %s", primKey));
            }
            DataType primKeyDt = DataType.OTHER;
            int index;
            for (DataTypeMap dtm : dataTypeMap.values()) {
                index = dtm.index+1;
                if (log.isDebugEnabled()) {
                    log.debug("Setting value for " + dtm.stringValue);
                }
                addParameterToPreparedStatement(pst, index, dtm);                
                if ( (primKey != null) && (dtm.colName.equals(primKey)) ) {
                    if (dtm.dt.equals(DataType.INTEGER)) {
                        primKeyDt = DataType.INTEGER;
                    }
                }
                
            }
            index = dataTypeMap.size()+1;
            if (primKey != null && originalKeyValue != null) {
                log.debug("Non null primary key " + index);
                
                if (primKeyDt.equals(DataType.INTEGER)) {
                    try {
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Setting prim key value to %d int for counter %d", Integer.parseInt(originalKeyValue), index));
                        }
                        pst.setInt(index, Integer.parseInt(originalKeyValue));
                    }
                    catch (NumberFormatException e) {
                        log.error("Unable to get prim key data - internal corruption");
                        if (pst != null) {
                        	pst.close();
                        }
                        if (conn != null) {
                        	conn.close();
                        }
                        return false;
                    }
                }
                else {
                    if (log.isDebugEnabled()) {
                        log.debug(String.format("Setting prim key value to %s string for counter %d", originalKeyValue, index));
                    }
                    pst.setString(index, originalKeyValue);
                }
            }
        
            if (log.isDebugEnabled()) {
                log.debug("About to execute the update prepared statement");
                log.debug(String.format("Command to run is %s", pst.toString()));
            }
            int num = pst.executeUpdate();
            if (log.isDebugEnabled()) {
                log.debug(String.format("Return from update is %d", num));
            }
            log.debug("Prepared statement executed");
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
                        
        return ret;
    }
	
	
	
	/**
     * Postgres specific command to get primary keys from a Postgres table.
     *
     * @param table the table to query
     * @return a list of String arrays containing the primary keys for the table
     * @throws SQLException
     * @throws ClassNotFoundException 
     */
	public List<String> getPrimaryKeysFromPostgres(String table) throws SQLException,
			ClassNotFoundException {
		log.debug("getPrimaryKeysFromPostgres");
		
		String command = String.format(
                "SELECT pg_attribute.attname as attname, format_type(pg_attribute.atttypid, pg_attribute.atttypmod) "
                + "FROM pg_index, pg_class, pg_attribute WHERE pg_class.oid = quote_ident(?)::regclass AND indrelid = pg_class.oid AND "
                + "pg_attribute.attrelid = pg_class.oid AND pg_attribute.attnum = any(pg_index.indkey) AND indisprimary");

		List<String> primaryKeys = new ArrayList<String>();

		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement pst = null;
		try {
			conn = initialiseConnection();
			pst = conn.prepareStatement(command);
			pst.setString(1, table);

			rs = pst.executeQuery();
			while (rs.next()) {
				primaryKeys.add(rs.getString("attname"));
			}

		}
		catch (SQLException e) {
			log.error("Error", e);
		}
		catch (ClassNotFoundException e) {
			log.error("Error", e);
		}
		finally {
			if (rs != null) {
				rs.close();
			}
			if (pst != null) {
				pst.close();
			}
			if (conn != null) {
				conn.close();
			}
		}

		return primaryKeys;
	}
	
	public List<HashMap<String, Object>> getIndexesFromPostgres(String table) throws SQLException, ClassNotFoundException {
        String command = "SELECT " +
            "i.relname as indexname, " +
            "idx.indrelid::regclass as tablename, " +
            "ARRAY( " +
                "SELECT pg_get_indexdef(idx.indexrelid, k + 1, true) " +
                "FROM generate_subscripts(idx.indkey, 1) as k " +
                "ORDER BY k " +
            ") as colnames, " +
            "indisunique as isunique, " +
            "indisprimary as isprimary " +
        "FROM " +   
            "pg_index as idx " +
            "JOIN pg_class as i " +
                "ON i.oid = idx.indexrelid " +
        "WHERE CAST(idx.indrelid::regclass as text) = quote_ident(?)";
        
        List<HashMap<String, Object>> indexes = getIndexesFromPostgres(command, table);

        return indexes;
    }
	
	private List<HashMap<String, Object>> getIndexesFromPostgres(String command, String table) throws SQLException,
			ClassNotFoundException {
		List<HashMap<String, Object>> indexes = new ArrayList<HashMap<String, Object>>();
		HashMap<String, Object> index;
		String type;

		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement pst = null;
		try {
			conn = initialiseConnection();
			pst = conn.prepareStatement(command);
			pst.setString(1, table);

			rs = pst.executeQuery();
			while (rs.next()) {
				index = new HashMap<String, Object>();
				index.put("name", rs.getString("indexname"));
				ArrayList<String> columns = new ArrayList<String>();
				ResultSet columnSet = rs.getArray("colnames").getResultSet();
				while (columnSet.next()) {
					columns.add(columnSet.getString("value"));
				}
				index.put("columns", columns);
				if (rs.getBoolean("isprimary")) {
					type = "PRIMARY";
				}
				else if (rs.getBoolean("isunique")) {
					type = "UNIQUE";
				}
				else {
					type = "INDEX";
				}
				index.put("type", type);

				indexes.add(index);
			}
		}
		catch (SQLException e) {
			log.error("Error", e);
		}
		catch (ClassNotFoundException e) {
			log.error("Error", e);
		}
		finally {
			if (rs != null) {
				rs.close();
			}
			if (pst != null) {
				pst.close();
			}
			if (conn != null) {
				conn.close();
			}
		}

		return indexes;
	}

	
	
	public List<HashMap<String, String>> getTableDescription(String table)
			throws SQLException {
		log.debug("getTableDescription");
		
		ArrayList<String> fields = new ArrayList<String>();
        fields.add("column_name");
        fields.add("data_type");
        fields.add("character_maximum_length");
        fields.add("numeric_precision");
        fields.add("numeric_scale");
        fields.add("column_default");
        fields.add("is_nullable");
        fields.add("ordinal_position");
        
        String command = String.format(
            "select %s from INFORMATION_SCHEMA.COLUMNS where table_name = ? ORDER BY ordinal_position ASC",
                StringUtils.join(fields.iterator(), ","));

		HashMap<String, String> columnDescription;
		List<HashMap<String, String>> columnDescriptions = new ArrayList<HashMap<String, String>>();

		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement pst = null;
		try {
			conn = initialiseConnection();
			pst = conn.prepareStatement(command);
			pst.setString(1, table);

			rs = pst.executeQuery();

			// First get all column names
			if (log.isDebugEnabled()) {
				log.debug(String.format("Found columns for table %s", table));
			}

			while (rs.next()) {
				columnDescription = new HashMap<String, String>();
				for (String field : fields) {
					columnDescription.put(field, rs.getString(field));
				}

				columnDescriptions.add(columnDescription);
			}

		}
		catch (SQLException e) {
			log.error("Error", e);
		}
		catch (ClassNotFoundException e) {
			log.error("Error", e);
		}
		finally {
			if (rs != null) {
				rs.close();
			}
			if (pst != null) {
				pst.close();
			}
			if (conn != null) {
				conn.close();
			}
		}

		if (log.isDebugEnabled()) {
			log.debug("getTableDescription:return " + columnDescriptions.size() + " entries");
		}

		return columnDescriptions;
	}

	
	
	/**
     * Postgres specific command to get foreign keys from a Postgres table
     *
     * @param table the table to query
     * @return a list of String arrays containing the foreign keys for the table
     * @throws SQLException
     * @throws ClassNotFoundException 
     */
    public List<HashMap<String, String>> getForeignKeysFromPostgres(String table) throws SQLException {
        log.debug("getForeignKeysFromPostgres");
        
        String command = "SELECT " +
                "tc.constraint_name, tc.table_name, kcu.column_name, " +
                "ccu.table_name AS foreign_table_name, " +
                "ccu.column_name AS foreign_column_name " + 
            "FROM information_schema.table_constraints tc " +
                "JOIN information_schema.key_column_usage kcu " +
                    "ON tc.constraint_name = kcu.constraint_name " +
                "JOIN information_schema.constraint_column_usage ccu " +
                    "ON ccu.constraint_name = tc.constraint_name " +
            "WHERE "+
                "constraint_type = 'FOREIGN KEY' " +
                "AND tc.table_name = ?";

        List foreignKeys = new ArrayList();
        HashMap<String, String> foreignKey;
        
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement pst = null;
		try {
			conn = initialiseConnection();
			pst = conn.prepareStatement(command);
            pst.setString(1, table);

            rs = pst.executeQuery();
            while (rs.next()) {
                foreignKey = new HashMap();
                foreignKey.put("constraintName", rs.getString("constraint_name"));
                foreignKey.put("tableName", rs.getString("table_name"));
                foreignKey.put("columnName", rs.getString("column_name"));
                foreignKey.put("foreignTableName", rs.getString("foreign_table_name"));
                foreignKey.put("foreignColumnName", rs.getString("foreign_column_name"));

                foreignKeys.add(foreignKey);
            }
		}
		catch (SQLException e) {
			log.error("Error", e);
		}
		catch (ClassNotFoundException e) {
			log.error("Error", e);
		}
		finally {
			if (rs != null) {
				rs.close();
			}
			if (pst != null) {
				pst.close();
			}
			if (conn != null) {
				conn.close();
			}
		}

        return foreignKeys;
    }
	
	
	public String getCommaSeparatedListOfColumnNamesFromTable(String tableName) throws ClassNotFoundException, SQLException {
		return getCommaSeparatedListFromTable(tableName, "SELECT column_name from information_schema.columns WHERE table_name=? ORDER BY ordinal_position ASC",
				"column_name");
	}
	
	
	public String getCommaSeparatedListFromTable(String tableName, String query, String identifier) throws ClassNotFoundException, SQLException {
		String data = "";
		
		Connection conn = null;
		PreparedStatement pst = null;
		try {
			conn = initialiseConnection();
		}
		catch (SQLException e) {
			return null;
		}
		ResultSet rs = null;
        try {
        	pst = conn.prepareStatement(query);
        	pst.setString(1, tableName);
        	pst.executeQuery();
            rs = pst.getResultSet();
            while (rs.next()) {
            	data += rs.getString(identifier)+",";
            }
            if (data.endsWith(",")) {
            	data = data.substring(0, data.length()-1);
            }
        }
        finally {
        	if (rs != null) {
                rs.close();
            }
            if (pst != null) {
            	pst.close();
            }
            if (conn != null) {
				conn.close();
			}
        }
		
		return data;
	}
	
	
    /**
     * Get a list of all tables belonging to the current database
     * @return
     * @throws SQLException 
     */
	public List<String> getAllTables() throws SQLException {
        log.debug("getAllTables");
        
        String command = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name";
        
        List<String> tables = new ArrayList<String>();

        Connection conn = null;

		ResultSet rs = null;
		PreparedStatement pst = null;
		try {
			conn = initialiseConnection();
			pst = conn.prepareStatement(command);

			rs = pst.executeQuery();

			while (rs.next()) {
	            tables.add(rs.getString(1));
	        }
		}
		catch (SQLException | ClassNotFoundException e) {
			log.error("Error", e);
		}
		finally {
			if (rs != null) {
				rs.close();
			}
			if (pst != null) {
				pst.close();
			}
			if (conn != null) {
				conn.close();
			}
		}


        return tables;
    }
    
    public CachedRowSet prepareAndExecuteStatement(String query) throws SQLException, ClassNotFoundException {
        return prepareAndExecuteStatement(query, null);    
    }
    
    public CachedRowSet prepareAndExecuteStatement(String query, List<Object> parameters) 
            throws SQLException, ClassNotFoundException {
    	if (log.isDebugEnabled()) {
    		log.debug("prepareAndExecuteStatement: " + query);
    		dbc.logCreds();
    	}
        
        Connection conn = null;
        PreparedStatement pst = null;
        try {
            conn = initialiseConnection();
            pst = conn.prepareStatement(query);

            if (parameters != null) {
                /*
                for (int i = 0; i < parameters.size(); i++) {
                    SimpleEntry<String, String> entry = parameters.get(i);
                    String value = entry.getKey();
                    String type = entry.getValue();
                    if (type.equalsIgnoreCase("int")) {
                        pst.setInt(i+1, Integer.parseInt(value));
                    } else if (type.equalsIgnoreCase("string")) {
                        pst.setString(i+1, value);
                    }
                }
                */
                int paramCount = 1;
                for (Object parameter : parameters) {
                    Class type = parameter.getClass();
                    if (type.equals(String.class)) {
                        pst.setString(paramCount, (String) parameter);
                    }
                    if (type.equals(Integer.class)) {
                        pst.setInt(paramCount, (Integer) parameter);
                    }
                    paramCount++;
                }
            }
            // Not sure about this bit, if non-select queries
            // dont return a ResultSet we might need to have an
            // argument that tells it whether to run execute() (and
            // return null) or executeQuery();
            if (query.toLowerCase().startsWith("select")) {
                ResultSet result = pst.executeQuery();
                CachedRowSet rowSet = new CachedRowSetImpl();
                rowSet.populate(result);
                log.debug("prepareAndExecuteStatement:return result");
                return rowSet;
            } else {
                pst.execute();
                log.debug("prepareAndExecuteStatement:return null");
                return null;
            }
        
        } 
        catch (ClassNotFoundException | SQLException e) {
        	log.error("Error with this command", e);
        	log.error("Query:" + query);
        	return null;
        }
        finally {
            if (pst != null) {
                pst.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
}
