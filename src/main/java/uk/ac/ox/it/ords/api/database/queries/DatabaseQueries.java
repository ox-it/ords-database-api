package uk.ac.ox.it.ords.api.database.queries;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.data.DataCell.DataType;
import uk.ac.ox.it.ords.api.database.data.DataTypeMap;


public class DatabaseQueries extends QueryRunner {
	private static Logger log = LoggerFactory.getLogger(DatabaseQueries.class);
	
	public DatabaseQueries(String dbServer, String dbName) {
		super(dbServer, dbName);
	}
	
	public DatabaseQueries(String dbServer) {
		super(dbServer);
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

}
