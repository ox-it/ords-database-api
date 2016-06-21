package uk.ac.ox.it.ords.api.database.services.impl;

import org.apache.commons.configuration.ConfigurationException;

import uk.ac.ox.it.ords.api.database.services.RestoreEmailService;

public class RestoreEmailServiceImpl extends SendMailTLS implements RestoreEmailService  {
	
	private String databaseName;
	
	public void setEmail ( String email ) {
		this.email = email;
	}
	
	
	public void setDatabaseName ( String databaseName ) {
		this.databaseName = databaseName;
	}
	
	public void sendStartRestoreMessage ( ) throws Exception {
		if ( !props.containsKey("ords.restore.start.message")) {
			throw new ConfigurationException ( "The database.properties file is not configured with the restore messages. Please contact an administrator");
		}
		String msg = String.format(props.getProperty("ords.restore.start.message"), this.databaseName);
		String subject = "Database restore started";
		this.sendMail(subject, msg);
	}
	
	
	public void sendRestoreSuccessfulMessage ( ) throws Exception {
		if (!props.containsKey("ords.restore.finish.message")) {
			throw new ConfigurationException ( "The database.properites file is not configured correctly, please contact an administrator");
		}
		String msg = String.format(props.getProperty("ords.restore.finish.message"), this.databaseName);
		String subject = "Database restore complete";
		this.sendMail(subject, msg);
		
	}
	
	
	public void sendRestoreUnsuccessfulMessage ( String error ) throws Exception {
		if (!props.containsKey("ords.restore.error.message")) {
			throw new ConfigurationException ( "The database.properites file is not configured correctly, please contact an administrator");			
		}
		String msg = String.format(props.getProperty("ords.restore.error.message"), this.databaseName, error);
		String subject = "Database restore error";
		this.sendMail(subject, msg);
	}
}
