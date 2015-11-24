/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.ox.it.ords.api.database.exceptions;

/**
 *
 * @author dave
 */
public class DBEnvironmentException extends Exception {
    public DBEnvironmentException(String dbName, String serverName) {
        super(String.format("Unable to understand database environment (database %s, server %s)",
            dbName, serverName));
    }
    
    public DBEnvironmentException(String error) {
        super(error);
    }

    public DBEnvironmentException() {
        super("No database server available");
    }
}
