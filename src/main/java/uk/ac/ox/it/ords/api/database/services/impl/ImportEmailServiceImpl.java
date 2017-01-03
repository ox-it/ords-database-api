package uk.ac.ox.it.ords.api.database.services.impl;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.services.ImportEmailService;

public class ImportEmailServiceImpl extends SendMailTLS implements ImportEmailService  {
	private Logger log = LoggerFactory.getLogger(ImportEmailServiceImpl.class);
	
	private String databaseName;
	
	public void setEmail ( String email ) {
		this.email = email;
	}
	
	
	public void setDatabaseName ( String databaseName ) {
		this.databaseName = databaseName;
	}
	
	public void sendStartImportMessage ( ) throws Exception {
		if ( !props.containsKey("ords.import.start.message")) {
			log.error ( "The database.properties file is not configured with the restore messages. Please contact an administrator");
			return;
		}
		String msg = String.format(props.getProperty("ords.restore.start.message"), this.databaseName);
		String subject = "Database import started";
		this.sendMail(subject, msg);
	}
	
	
	public void sendImportSuccessfulMessage ( ) throws Exception {
		if (!props.containsKey("ords.import.finish.message")) {
			log.error ( "The database.properites file is not configured correctly, please contact an administrator");
			return;
		}
		String msg = String.format(props.getProperty("ords.restore.finish.message"), this.databaseName);
		String subject = "Database import complete";
		this.sendMail(subject, msg);
		
	}
	
	
	public void sendImportUnsuccessfulMessage ( String error ) throws Exception {
		if (!props.containsKey("ords.import.error.message")) {
			log.error ( "The database.properites file is not configured correctly, please contact an administrator");			
			return;
		}
		String msg = String.format(props.getProperty("ords.restore.error.message"), this.databaseName, error);
		String subject = "Database import error";
		this.sendMail(subject, msg);
	}
}
