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

package uk.ac.ox.it.ords.api.database.resources;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;

import javax.ws.rs.core.Response;

import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.jaxrs.ext.multipart.Attachment;
import org.apache.cxf.jaxrs.ext.multipart.ContentDisposition;
import org.apache.cxf.jaxrs.ext.multipart.MultipartBody;
import org.junit.After;
import org.junit.Test;

import uk.ac.ox.it.ords.api.database.data.TableData;

public class QueryTest extends AbstractDatabaseTestRunner{
	
	@After
	public void cleanup() throws Exception{
		logout();
	}
	
	@Test
	public void query() throws Exception{
		loginBasicUser();		
		
		//
		// Import a database
		//
		File accessFile = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachement;filename=mondial.accdb");
		Attachment att = new Attachment("dataFile", inputStream, cd);
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String path = response.getLocation().getPath();
		String id = path.substring(path.lastIndexOf('/')+1);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		
		//
		// Query
		//
		response = getClient(true).path("/"+id+"/query/").query("q", "select \"Code\", \"CountryName\" from country where \"CountryName\" LIKE '%Al%'").get();
		assertEquals(200, response.getStatus());
		TableData data = response.readEntity(TableData.class);
		// Albania and Algeria...
		assertEquals(2, data.rows.size());	
		
		//
		// Query with null
		//
		response = getClient(true).path("/"+id+"/query/").query("q", "").get();
		assertEquals(400, response.getStatus());
		
		//
		// Now again, without permission
		//
		logout();
		response = getClient(true).path("/"+id+"/query/").query("q", "select \"Code\", \"CountryName\" from country where \"CountryName\" LIKE '%Al%'").get();
		assertEquals(403, response.getStatus());		
		loginBasicUser();	
		
		// Cleanup
		AbstractResourceTest.databases.add(r);

		logout();
		
	}
	
	@Test
	public void queryNonExistingDb(){
		loginBasicUser();	
		Response response = getClient(true).path("/9999/query/").query("q", "select \"Code\", \"CountryName\" from country where \"CountryName\" LIKE '%Al%'").get();
		assertEquals(404, response.getStatus());		
	}
	
	@Test
	public void queryWithBadSQL() throws Exception{
		loginBasicUser();		
		
		//
		// Import a database
		//
		File accessFile = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachement;filename=mondial.accdb");
		Attachment att = new Attachment("dataFile", inputStream, cd);
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String path = response.getLocation().getPath();
		String id = path.substring(path.lastIndexOf('/')+1);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		
		// Prepare Cleanup
		AbstractResourceTest.databases.add(r);
		
		response = getClient(true).path("/"+id+"/query/").query("q", "select \"stuff\" from \"banana\"").get();
		assertEquals(400, response.getStatus());

		logout();
		
	}
	
	@Test
	public void queryWithInsert() throws Exception{
		loginBasicUser();		
		
		//
		// Import a database
		//
		File accessFile = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachement;filename=mondial.accdb");
		Attachment att = new Attachment("dataFile", inputStream, cd);
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String path = response.getLocation().getPath();
		String id = path.substring(path.lastIndexOf('/')+1);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		
		// Prepare Cleanup
		AbstractResourceTest.databases.add(r);
		
		//
		// Attempted insert using query service
		//
		response = getClient(true).path("/"+id+"/query/").query("q", "insert into country (\"Code\", \"CountryName\") values ('ZZ', 'Zebgonia')").get();
		assertEquals(400, response.getStatus());
		response = getClient(true).path("/"+id+"/query/").query("q", "select \"Code\", \"CountryName\" from country where \"CountryName\" LIKE '%Zebgon%'").get();
		assertEquals(200, response.getStatus());
		TableData data = response.readEntity(TableData.class);
		assertEquals(0, data.rows.size());	
		logout();
		
	}

}
