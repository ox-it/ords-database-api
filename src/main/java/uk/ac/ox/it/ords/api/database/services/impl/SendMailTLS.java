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
package uk.ac.ox.it.ords.api.database.services.impl;

import java.util.Properties;

import org.apache.commons.configuration.ConfigurationConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import uk.ac.ox.it.ords.security.configuration.MetaConfiguration;

/**
 *
 * @author dave
 */
public class SendMailTLS {
	
	private Logger log = LoggerFactory.getLogger(SendMailTLS.class);
	protected Properties props;
	protected String email;

	public SendMailTLS() {
		props = ConfigurationConverter.getProperties(MetaConfiguration.getConfiguration());
	}
	
	public SendMailTLS(Properties properties){
		props = properties;
	}

	protected void sendMail(String subject, String messageText) throws Exception {
		
		//
		// Validate Mail server settings
		//
		if (
				!props.containsKey("mail.smtp.username")  ||
				!props.containsKey("mail.smtp.password")  ||
				!props.containsKey("mail.smtp.host")
		) {
			    log.error("Unable to send emails as email server configuration is missing");
			    //throw new Exception("Unable to send emails as email server configuration is missing");
			    return; // 
		}
		
		Session session = Session.getInstance(props,
				new javax.mail.Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(props.get("mail.smtp.username").toString(), props.get("mail.smtp.password").toString());
			}
		});

		try {
			Message message = new MimeMessage(session);
			message.setRecipients(Message.RecipientType.TO,
					InternetAddress.parse(email));
			message.setSubject(subject);
			message.setText(messageText);

			Transport.send(message);

			if (log.isDebugEnabled()) {
				log.debug(String.format("Sent email to %s", email));
				log.debug("with content: " + messageText);
			}
		}
		catch (MessagingException e) {
			log.error("Unable to send email to " + email, e);
			throw new Exception("Unable to send email to " + email, e);
		}
	}


}
