/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package uk.ac.ox.it.ords.api.database.conf;


import java.io.File;

import org.hibernate.cfg.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.security.configuration.MetaConfiguration;


/**
 *
 * @author dave
 * @author kjpopat
 */
public class DBCredentials {
    private static Logger log = LoggerFactory.getLogger(DBCredentials.class);

	protected static String HIBERNATE_CONFIGURATION_PROPERTY = "ords.hibernate.configuration";
	
	protected static String ORDS_DATABASE_NAME = "ords.database.name";
	protected static String ORDS_DATABASE_USER = "ords.database.user";
	protected static String ORDS_DATABASE_PASSWORD = "ords.database.password";
	protected static String ORDS_DATABASE_HOST = "ords.database.server.host";

	private String user;

	private String password;

	private String dbName;

	private String dbServer = null;

	private String JDBC_DRIVER;

	private String dbUrl;
	
	public void init(){
		
	}
    
    public DBCredentials(String dbServer, String dbName) {
    	if ( dbServer == null ) {
    		Configuration configuration = new Configuration();
    		String hibernateConfigLocation = MetaConfiguration.getConfiguration().getString(HIBERNATE_CONFIGURATION_PROPERTY);


    		if (hibernateConfigLocation == null) {
    			configuration.configure();
    		} else {
    			configuration.configure(new File(hibernateConfigLocation));
    		}


            JDBC_DRIVER = configuration.getProperty("hibernate.connection.driver_class");
            
            dbUrl = configuration.getProperty("hibernate.connection.url");
            dbServer = dbUrl.substring(dbUrl.lastIndexOf("//") + 2, dbUrl.lastIndexOf("/"));
    		
    	}
    	else {
    		Configuration configuration = new Configuration();
    		String hibernateConfigLocation = MetaConfiguration.getConfiguration().getString(HIBERNATE_CONFIGURATION_PROPERTY);


    		if (hibernateConfigLocation == null) {
    			configuration.configure();
    		} else {
    			configuration.configure(new File(hibernateConfigLocation));
    		}


            JDBC_DRIVER = configuration.getProperty("hibernate.connection.driver_class");
            
        	this.dbServer = dbServer;    		
    	}
    	
    	if ((dbName == null) || (dbName.isEmpty()) ) {
    		this.dbName = CommonVars.CONNECTABLE_DB;
    	}
    	else {
    		this.dbName = dbName;
    	}
        
        dbUrl = "jdbc:postgresql://" + dbServer + "/" + this.dbName;
        
		org.apache.commons.configuration.Configuration properties = MetaConfiguration.getConfiguration();
		
		/**
		 * The user that can connect to and read/write from/to the ords database. Used for hibernate.
		 */
		user = properties.getString(ORDS_DATABASE_USER);
		password = properties.getString(ORDS_DATABASE_PASSWORD);

    }

    public String getDbServer() {
        return dbServer;
    }

    public String getDbName() {
        return dbName;
    }

	public String getUser() {
	    return user;
	}

	public String getPassword() {
	    return password;
	}

	public String getJDBC_DRIVER() {
	    return JDBC_DRIVER;
	}

	public String getDbUrl() {
	    return dbUrl;
	}
}
