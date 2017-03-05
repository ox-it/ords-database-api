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
import java.io.FileNotFoundException;

import javax.ws.rs.core.Response;

import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.jaxrs.ext.multipart.Attachment;
import org.apache.cxf.jaxrs.ext.multipart.ContentDisposition;
import org.apache.cxf.jaxrs.ext.multipart.MultipartBody;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ox.it.ords.api.database.data.Row;

public class TableTestComplex extends AbstractDatabaseTestRunner{
	
	private String dbID;
	
	@Before
	public void setupTable() throws FileNotFoundException{
		loginBasicUser();
		
		File file = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream = new FileInputStream(file);
		ContentDisposition cd = new ContentDisposition("attachment;filename=mondial.accdb");
		Attachment att = new Attachment("dataFile", inputStream, cd);

		WebClient client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String path = response.getLocation().getPath();
		dbID = path.substring(path.lastIndexOf('/')+1);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(dbID), false);
		AbstractResourceTest.databases.add(r);
	}
	
	@After
	public void tearDownTable() throws Exception{
		logout();
	}
	
	@Test
	public void deleteRowWithConstraint() throws Exception{

		loginBasicUser();

		assertEquals(200, getClient(true).path("/"+dbID+"/tabledata/country").get().getStatus());

		//
		// Delete it
		//
		assertEquals(409, getClient(true).path("/"+dbID+"/tabledata/country").query("primaryKey", "Code").query("primaryKeyValue", "AFG").delete().getStatus());


		//logout();

	}
	
	@Test
	public void insertRowWithPKandAutonumber() throws Exception{

		loginBasicUser();

		assertEquals(200, getClient(true).path("/"+dbID+"/tabledata/city").get().getStatus());
		
		//
		// Create the row
		//
		Row row = new Row();
		row.columnNames = new String[]{"CityName", "Population"};
		row.values = new String[]{"XXXX","XXXX"};
		Response response = getClient(true).path("/"+dbID+"/tabledata/city").post(row);
		assertEquals(201, response.getStatus());

		//logout();

	}

}
