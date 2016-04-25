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
import static org.junit.Assert.assertTrue;

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

import uk.ac.ox.it.ords.api.database.data.DataCell;
import uk.ac.ox.it.ords.api.database.data.DataRow;
import uk.ac.ox.it.ords.api.database.data.Row;
import uk.ac.ox.it.ords.api.database.data.TableData;

public class TableTest extends AbstractDatabaseTestRunner {
	
	private String dbID;
	private String tableName = "small_test";
	
	@Before
	public void setupTable() throws FileNotFoundException{
		
		loginUsingSSO("pingu@nowhere.co", "");
				
		// create a csv file database
		File csvFile = new File(getClass().getResource("/small_test.csv").getFile());
		FileInputStream inputStream = new FileInputStream(csvFile);
		ContentDisposition cd = new ContentDisposition("attachment;filename=small_test.csv");
		Attachment att = new Attachment("databaseFile", inputStream, cd);

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
	public void tearDownTable(){
	}
	
	@Test
	public void addRowToTableNull() throws Exception{
		loginUsingSSO("pingu@nowhere.co", "");
		WebClient client = getClient(true);
		Response response = client.path("/"+dbID+"/tabledata/"+tableName).post(null);
		assertEquals(400, response.getStatus());
		logout();
	}
	
	@Test
	public void addRowToTableUnauth() throws Exception{
		logout();
		Row row = new Row();
		WebClient client = getClient(true);
		Response response = client.path("/"+dbID+"/tabledata/"+tableName).post(row);
		assertEquals(403, response.getStatus());
	}
	
	@Test
	public void getTableUnauth() throws Exception{
		logout();
		WebClient client = getClient(true);
		Response response = client.path("/"+dbID+"/tabledata/"+tableName).get();
		assertEquals(403, response.getStatus());
	}
	
	@Test
	public void getTableNonexisting() throws Exception{
		loginUsingSSO("pingu@nowhere.co", "");
		assertEquals(404, getClient(true).path("/"+dbID+"/tabledata/nosuchtable").get().getStatus());
		assertEquals(404, getClient(true).path("/9999/tabledata/"+tableName).get().getStatus());
	}
	
	@Test
	public void addRowToTable() throws Exception{		
		loginUsingSSO("pingu@nowhere.co", "");
		
		assertEquals(200, getClient(true).path("/"+dbID+"/tabledata/"+tableName).get().getStatus());

		//
		// Create the row
		//
		Row row = new Row();
		row.columnNames = new String[]{"id","n", "volume", "pubid","long"};
		row.values = new String[]{"XXXX","XXXX", "1", "1","1"};
		Response response = getClient(true).path("/"+dbID+"/tabledata/"+tableName).post(row);
		assertEquals(201, response.getStatus());
		
		//
		// Check it exists
		//
		response = getClient(true).path("/"+dbID+"/tabledata/"+tableName).query("length", 400).get();
		assertEquals(200, response.getStatus());

		TableData data = response.readEntity(TableData.class);
		
		boolean containsNewRow = false;
		for (DataRow currentRow : data.rows){
			for(Object value : currentRow.cell.values()){
				System.out.println(((DataCell)value).getValue());
			}

			if (
					((DataCell)currentRow.cell.get("id")).getValue().equals("XXXX") &&
					((DataCell)currentRow.cell.get("n")).getValue().equals("XXXX") &&
					((DataCell)currentRow.cell.get("volume")).getValue().equals("1") &&
					((DataCell)currentRow.cell.get("pubid")).getValue().equals("1") &&
					((DataCell)currentRow.cell.get("long")).getValue().equals("1")
			){
				containsNewRow = true;
			}
		}
		assertTrue(containsNewRow);
		logout();
	}

}
