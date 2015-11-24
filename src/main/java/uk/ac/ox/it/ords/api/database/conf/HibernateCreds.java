/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.ox.it.ords.api.database.conf;

import java.io.File;

import org.hibernate.cfg.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author dave
 */
public class HibernateCreds {
    private static Logger log = LoggerFactory.getLogger(HibernateCreds.class);
    
    protected String user;
    protected String password;
    protected String dbName; // This will be the ords db name
    protected String dbServer = null; // This will be the ords database server
    protected File configFile;
    protected String JDBC_DRIVER;
    protected String dbUrl, rootDbUrl;
    
    
    protected HibernateCreds() {
        init();
    }
    
    
    private void init() {
        log.debug("HibernateCreds:init");
        
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
        rootDbUrl = dbUrl.substring(0, dbUrl.lastIndexOf("/") + 1);
        
        user = configuration.getProperty("hibernate.connection.username");
        password = configuration.getProperty("hibernate.connection.password");
        
        
        if (log.isDebugEnabled()) {
            log.debug("JDBC Driver value is " + JDBC_DRIVER);
            log.debug("dbUrl value is " + dbUrl);
            log.debug("ordsDbServer value is " + dbServer);
            log.debug("rootDbUrl value is " + rootDbUrl);
            log.debug("user value is " + user);
        }
        
        
        log.debug("init:return");
    }

    public String getDbServer() {
        return dbServer;
    }

    public void setDbServer(String ordsDbServer) {
        this.dbServer = ordsDbServer;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String ordsDbName) {
        this.dbName = ordsDbName;
    }

    public String getJDBC_DRIVER() {
        return JDBC_DRIVER;
    }

    public void setJDBC_DRIVER(String JDBC_DRIVER) {
        this.JDBC_DRIVER = JDBC_DRIVER;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public String getRootDbUrl() {
        return rootDbUrl;
    }

    public void setRootDbUrl(String rootDbUrl) {
        this.rootDbUrl = rootDbUrl;
    }
}
