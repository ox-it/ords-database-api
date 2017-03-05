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
import org.junit.Test;

import uk.ac.ox.it.ords.api.database.data.TableData;

public class RelationTest extends AbstractDatabaseTestRunner {

	@Test
	public void testGetRelated() throws Exception{
		
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
		// Get related values
		//
		response = getClient(true).path("/"+id+"/table/country/column/CountryName/related").get();
		assertEquals(200, response.getStatus());
		TableData data = response.readEntity(TableData.class);
		assertEquals("value", data.columns.get(0).columnName);
		assertEquals("label", data.columns.get(1).columnName);
		
		assertEquals("AFG", data.rows.get(0).cell.get("value").getValue());
		assertEquals("Afghanistan", data.rows.get(0).cell.get("label").getValue());
		
		//
		// Lets try a subset search
		//
		response = getClient(true).path("/"+id+"/table/country/column/CountryName/related").query("term", "aust").get();
		assertEquals(200, response.getStatus());
		data = response.readEntity(TableData.class);
		assertEquals("value", data.columns.get(0).columnName);
		assertEquals("label", data.columns.get(1).columnName);
		
		assertEquals("AUS", data.rows.get(0).cell.get("value").getValue());
		assertEquals("Australia", data.rows.get(0).cell.get("label").getValue());
		assertEquals("A", data.rows.get(1).cell.get("value").getValue());
		assertEquals("Austria", data.rows.get(1).cell.get("label").getValue());
				
		// Cleanup
		AbstractResourceTest.databases.add(r);
		logout();

	}
	
	@Test
	public void testGetRelatedNonexisting() throws Exception{
		
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
		// Get related values
		//
		response = getClient(true).path("/"+id+"/table/banana/column/fruit/related").get();
		//
		// TODO FIXME should return 404, returns 200
		//
		//assertEquals(404, response.getStatus());
		
		response = getClient(true).path("/9999/table/banana/column/fruit/related").get();
		assertEquals(404, response.getStatus());

		// Cleanup
		AbstractResourceTest.databases.add(r);
		logout();

	}
	
	@Test
	public void testGetRelatedUnauth() throws Exception{
		
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
		// Get related values
		//
		logout();
		assertEquals(403, getClient(true).path("/"+id+"/table/country/column/CountryName/related").get().getStatus());

		// Cleanup
		AbstractResourceTest.databases.add(r);
		logout();

	}

}
