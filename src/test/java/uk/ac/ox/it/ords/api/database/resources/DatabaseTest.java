package uk.ac.ox.it.ords.api.database.resources;

import static org.junit.Assert.assertEquals;

import javax.ws.rs.ForbiddenException;

import org.junit.Test;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.transport.local.LocalConduit;

public class DatabaseTest extends AbstractResourceTest{


	@Test (expected=ForbiddenException.class)
	public void getDatabaseUnauthenticated() {
		WebClient client = WebClient.create(ENDPOINT_ADDRESS);
		WebClient.getConfig(client).getRequestContext().put(LocalConduit.DIRECT_DISPATCH, Boolean.TRUE);
		client.accept("application/json");
		client.path("database/2");
		client.get(String.class);
	}

	@Test
	public void getDatabasePermitted() {
		login("pingu", "pingu");
		WebClient client = WebClient.create(ENDPOINT_ADDRESS);
		WebClient.getConfig(client).getRequestContext().put(LocalConduit.DIRECT_DISPATCH, Boolean.TRUE); 
		client.accept("application/json");
		client.path("database/1");
		String response = client.get(String.class);
		assertEquals("Success", response);
	}
	
	@Test (expected=ForbiddenException.class)
	public void getDatabaseForbidden() {
		login("pinga", "pinga");
		WebClient client = WebClient.create(ENDPOINT_ADDRESS);
		WebClient.getConfig(client).getRequestContext().put(LocalConduit.DIRECT_DISPATCH, Boolean.TRUE); 
		client.accept("application/json");
		client.path("database/2");
		client.get(String.class);
	}


}
