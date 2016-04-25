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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;

import javax.ws.rs.core.Response;

import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.jaxrs.ext.multipart.Attachment;
import org.apache.cxf.jaxrs.ext.multipart.ContentDisposition;
import org.apache.cxf.jaxrs.ext.multipart.MultipartBody;
import org.junit.Test;

import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.data.TableViewInfo;

public class DatasetTest extends AbstractDatabaseTestRunner {
	
	@Test
	public void getDataNonExisting() throws Exception{
		loginUsingSSO("pingu@nowhere.co", "");		
		
		//
		// Import a database
		//
		File accessFile = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachement;filename=mondial.accdb");
		Attachment att = new Attachment("databaseFile", inputStream, cd);
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String path = response.getLocation().getPath();
		String id = path.substring(path.lastIndexOf('/')+1);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		
		//
		// Get dataset
		//
		assertEquals(404, getClient(true).path("/"+id+"/datasetdata/9999").get().getStatus());
		
		// Cleanup
		AbstractResourceTest.databases.add(r);

		logout();
		
	}

	@Test
	public void createPrivateDataset() throws Exception{
		
		loginUsingSSO("pingu@nowhere.co", "");
		
		//
		// Import a database
		//
		File accessFile = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachement;filename=mondial.accdb");
		Attachment att = new Attachment("databaseFile", inputStream, cd);
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String path = response.getLocation().getPath();
		String id = path.substring(path.lastIndexOf('/')+1);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		
		//
		// Create the dataset
		//
		TableViewInfo dataset = new TableViewInfo();
		dataset.setViewName("test");
		dataset.setViewTable("City");
		dataset.setViewQuery("SELECT 'CityName', 'Population' from City");
		dataset.setViewDescription("test");
		dataset.setViewAuthorization("private");
		response = getClient(true).path("/"+id+"/dataset/").post(dataset);	
		assertEquals(201, response.getStatus());
		
		//
		// Get the metadata
		//
		path = response.getLocation().getPath();
		String viewId = path.substring(path.lastIndexOf('/')+1);

		response = getClient(true).path("/"+id+"/dataset/"+viewId).get();
		assertEquals(200, response.getStatus());
		dataset = response.readEntity(TableViewInfo.class);
		assertEquals("test", dataset.getViewName());
		
		//
		// Get the data
		//
		response = getClient(true).path("/"+id+"/datasetdata/"+viewId).get();
		assertEquals(200, response.getStatus());
		TableData data = response.readEntity(TableData.class);
		assertNotNull(data);
		assertEquals(50,data.getNumberOfRowsInEntireTable());
		
		//
		// Get the data not logged in - will not work as its not public
		//
		logout();
		response = getClient(true).path("/"+id+"/datasetdata/"+viewId).get();
		assertEquals(403, response.getStatus());		

		// Cleanup
		AbstractResourceTest.databases.add(r);

		logout();

	}
	
	@Test
	public void createDataset() throws Exception{
		
		loginUsingSSO("pingu@nowhere.co", "");
		
		//
		// Import a database
		//
		File accessFile = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachement;filename=mondial.accdb");
		Attachment att = new Attachment("databaseFile", inputStream, cd);
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String path = response.getLocation().getPath();
		String id = path.substring(path.lastIndexOf('/')+1);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		
		//
		// Create the dataset
		//
		TableViewInfo dataset = new TableViewInfo();
		dataset.setViewName("test");
		dataset.setViewTable("City");
		dataset.setViewQuery("SELECT 'CityName', 'Population' from City");
		dataset.setViewDescription("test");
		dataset.setViewAuthorization("public");
		response = getClient(true).path("/"+id+"/dataset/").post(dataset);	
		assertEquals(201, response.getStatus());
		
		//
		// Get the metadata
		//
		path = response.getLocation().getPath();
		String viewId = path.substring(path.lastIndexOf('/')+1);

		response = getClient(true).path("/"+id+"/dataset/"+viewId).get();
		assertEquals(200, response.getStatus());
		dataset = response.readEntity(TableViewInfo.class);
		assertEquals("test", dataset.getViewName());
		
		//
		// Get the data
		//
		response = getClient(true).path("/"+id+"/datasetdata/"+viewId).get();
		assertEquals(200, response.getStatus());
		TableData data = response.readEntity(TableData.class);
		assertNotNull(data);
		assertEquals(50,data.getNumberOfRowsInEntireTable());
		
		//
		// Get the data not logged in - will still work as its public
		//
		logout();
		response = getClient(true).path("/"+id+"/datasetdata/"+viewId).get();
		assertEquals(200, response.getStatus());
		data = response.readEntity(TableData.class);
		assertNotNull(data);
		assertEquals(50,data.getNumberOfRowsInEntireTable());
		

		// Cleanup
		AbstractResourceTest.databases.add(r);

		logout();

	}
	
	@Test
	public void createDatasetUnauth() throws Exception{
		
		//
		// Import a database
		//
		loginUsingSSO("pingu@nowhere.co", "");

		File accessFile = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachement;filename=mondial.accdb");
		Attachment att = new Attachment("databaseFile", inputStream, cd);
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		String path = response.getLocation().getPath();
		String id = path.substring(path.lastIndexOf('/')+1);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		logout();
		
		//
		// Create the dataset
		//
		TableViewInfo dataset = new TableViewInfo();
		dataset.setViewName("test");
		dataset.setViewTable("small_test");
		dataset.setViewQuery("select * from small_test");
		dataset.setViewDescription("test");
		dataset.setViewAuthorization("");
		response = getClient(true).path("/"+id+"/dataset/").post(dataset);	
		assertEquals(403, response.getStatus());
		

		// Clenaup
		AbstractResourceTest.databases.add(r);
		logout();
	}
	
	@Test
	public void createDatasetBadRequests() throws Exception{
		
		
		loginUsingSSO("pingu@nowhere.co", "");
		
		//
		// Import a database
		//
		File accessFile = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachement;filename=mondial.accdb");
		Attachment att = new Attachment("databaseFile", inputStream, cd);
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String path = response.getLocation().getPath();
		String id = path.substring(path.lastIndexOf('/')+1);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		AbstractResourceTest.databases.add(r);
		
		//
		// Create the dataset
		//
		TableViewInfo dataset = new TableViewInfo();
		dataset.setViewName("test");
		dataset.setViewTable("small_test");
		dataset.setViewQuery("select * from small_test");
		dataset.setViewDescription("test");
		dataset.setViewAuthorization("");
		
		//
		// POST null
		//
		assertEquals(400, getClient(true).path("/"+id+"/dataset/").post(null).getStatus());
		
		//
		// POST no DB
		//
		assertEquals(404, getClient(true).path("/9999/dataset/").post(dataset).getStatus());
		
		logout();
	}
	
	@Test
	public void deleteDataset(){
		
	}
	
	@Test
	public void deleteDatasetUnauth() throws Exception{
		
		//TODO
		
		logout();
	}
	
	@Test
	public void deleteDatasetNonexisting() throws Exception{
		loginUsingSSO("pingu@nowhere.co", "");
		
		//
		// Import a database
		//
		File accessFile = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachement;filename=mondial.accdb");
		Attachment att = new Attachment("databaseFile", inputStream, cd);
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String path = response.getLocation().getPath();
		String id = path.substring(path.lastIndexOf('/')+1);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		AbstractResourceTest.databases.add(r);
		
		assertEquals(404, getClient(true).path("/9999/dataset/999").delete().getStatus());
		// TODO FIXME
		//assertEquals(404, getClient(true).path("/"+id+"/dataset/999").delete().getStatus());
		logout();
	}

}
