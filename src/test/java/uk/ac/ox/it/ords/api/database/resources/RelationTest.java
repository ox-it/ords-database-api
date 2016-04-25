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
		// Get related values
		//
		response = getClient(true).path("/"+id+"/table/city/column/Country/related").get();
		assertEquals(200, response.getStatus());
		TableData data = response.readEntity(TableData.class);
		assertEquals("value", data.columns.get(0).columnName);
		assertEquals("label", data.columns.get(1).columnName);
		
		// To be honest this doesn't look complete to me, so I'll leave off testing for now
		
		// Cleanup
		AbstractResourceTest.databases.add(r);
		logout();

	}
	
}
