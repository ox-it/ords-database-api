/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package uk.ac.ox.it.ords.api.database.conf;


import org.hibernate.cfg.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author dave
 * @author kjpopat
 */
public class DBCredentials extends HibernateCreds {
    private static Logger log = LoggerFactory.getLogger(DBCredentials.class);
    
    public DBCredentials() {
    	super();
    }
    
    
    public DBCredentials(String dbServer, String dbName) {
    	if ( dbServer == null ) {
    		Configuration configuration = new Configuration();
    		String hibernateConfigLocation = MetaConfiguration
    				.getConfigurationLocation("hibernate");

    		if (hibernateConfigLocation == null) {
    			configuration.configure();
    		} else {
    			configuration.configure(hibernateConfigLocation);
    		}


            JDBC_DRIVER = configuration.getProperty("hibernate.connection.driver_class");
            
            dbUrl = configuration.getProperty("hibernate.connection.url");
            dbServer = dbUrl.substring(dbUrl.lastIndexOf("//") + 2, dbUrl.lastIndexOf("/"));
    		
    	}
    	else {
        	this.dbServer = dbServer;    		
    	}
        if (log.isDebugEnabled()) {
            log.debug(String.format("DBCredentials(%s, %s)", dbServer, dbName));
        }
    	
    	if ((dbName == null) || (dbName.isEmpty()) ) {
    		this.dbName = CommonVars.CONNECTABLE_DB;
    	}
    	else {
    		this.dbName = dbName;
    	}
        
        rootDbUrl = "jdbc:postgresql://" + dbServer + "/";
        dbUrl = "jdbc:postgresql://" + dbServer + "/" + this.dbName;
        if (log.isDebugEnabled()) {
            log.debug(String.format("Returning with rootDbUrl = <%s>, dbUrl = <%s>", rootDbUrl, dbUrl));
        }
        
        AuthenticationDetails ad = new AuthenticationDetails();
        user = ad.getRootDbUser();
        password = ad.getRootDbPassword();
    }
    
    public void logCreds() {
    	if (log.isDebugEnabled()) {
    		log.debug(String.format("Server: <%s>, Db:<%s>, User:<%s>", dbServer, dbName, user));
    		log.debug(String.format("DB URL: <%s>", dbUrl));
    	}
    }

    @Override
    public String getDbServer() {
        return dbServer;
    }

    @Override
    public void setDbServer(String dbServer) {
        this.dbServer = dbServer;
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}
