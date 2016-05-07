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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;

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
	public void tearDownTable() throws Exception{
		logout();
	}
	
	@Test
	public void addRowToTableNull() throws Exception{
		loginUsingSSO("pingu@nowhere.co", "");
		WebClient client = getClient(true);
		Response response = client.path("/"+dbID+"/tabledata/"+tableName).post(null);
		assertEquals(400, response.getStatus());
		
		//
		// Empty object
		//
		Row row = new Row();
		response = getClient(true).path("/"+dbID+"/tabledata/"+tableName).post(row);
		assertEquals(400, response.getStatus());
		
		//
		// Cols, but no data
		//
		row.columnNames = new String[]{"id","n", "volume", "pubid","long"};
		response = getClient(true).path("/"+dbID+"/tabledata/"+tableName).post(row);
		assertEquals(400, response.getStatus());
		
		//
		// Cols, but data doesn't match up
		//
		row.columnNames = new String[]{"id","n", "volume", "pubid","long"};
		row.values = new String[1];
		response = getClient(true).path("/"+dbID+"/tabledata/"+tableName).post(row);
		assertEquals(400, response.getStatus());
		
		//
		// Cols, but data doesn't match up
		//
		row.columnNames = new String[]{"id","n", "volume", "pubid","long"};
		row.values = new String[]{"99", "99", "A", "B", "C", "D"};
		response = getClient(true).path("/"+dbID+"/tabledata/"+tableName).post(row);
		assertEquals(400, response.getStatus());
		
		//
		// if we supply empty values, it just uses defaults
		//
		row.columnNames = new String[]{"id","n", "volume", "pubid","long"};
		row.values = new String[]{};
		response = getClient(true).path("/"+dbID+"/tabledata/"+tableName).post(row);
		assertEquals(201, response.getStatus());
		
		logout();
	}
	
	@Test
	public void updateRowInTableNull() throws Exception{
		loginUsingSSO("pingu@nowhere.co", "");
		WebClient client = getClient(true);
		Response response = client.path("/"+dbID+"/tabledata/"+tableName).put(null);
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
	public void updateRowInTableUnauth() throws Exception{
		logout();
		ArrayList<Row> rows = new ArrayList<Row>();
		WebClient client = getClient(true);
		Response response = client.path("/"+dbID+"/tabledata/"+tableName).put(rows);
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
	public void deleteRowUnauth() throws Exception{
		logout();
		WebClient client = getClient(true);
		Response response = client.path("/"+dbID+"/tabledata/"+tableName).query("primaryKey", "id").query("primaryKeyValue", "V3a-36334").delete();
		assertEquals(403, response.getStatus());
	}
	
	@Test
	public void deleteRowNonexisting() throws Exception{
		loginUsingSSO("pingu@nowhere.co", "");
		
		assertEquals(404, getClient(true).path("/XXXX/tabledata/"+tableName).query("primaryKey", "id").query("primaryKeyValue", "V3a-36334").delete().getStatus());
		assertEquals(404, getClient(true).path("/9999/tabledata/"+tableName).query("primaryKey", "id").query("primaryKeyValue", "V3a-36334").delete().getStatus());
		assertEquals(404, getClient(true).path("/9999/tabledata/"+tableName).query("primaryKey", "id").query("primaryKeyValue", "YYYYYYYY").delete().getStatus());

		//
		// TODO FIXME when we have no matching rows, we ought to return 404
		//
		//assertEquals(404, getClient(true).path("/"+dbID+"/tabledata/"+tableName).query("primaryKey", "id").query("primaryKeyValue", "YYYYYYYY").delete().getStatus());
		
		assertEquals(404, getClient(true).path("/"+dbID+"/tabledata/"+tableName).query("primaryKey", "").query("primaryKeyValue", "").delete().getStatus());

		//
		// TODO FIXME when we have a bad table name, we should get 404, not 500
		//
		//assertEquals(404, getClient(true).path("/"+dbID+"/tabledata/nosuchtable").query("primaryKey", "id").query("primaryKeyValue", "V3a-36334").delete().getStatus());
	}
	
	@Test
	public void updateRowNonexisting() throws Exception{
		loginUsingSSO("pingu@nowhere.co", "");
		
		ArrayList<Row> rows = new ArrayList<Row>();
		Row row = new Row();
		row.columnNames = new String[]{"id","n", "volume", "pubid","long"};
		row.values = new String[]{"XXXX","XXXX", "1", "1","99"};
		row.lookupColumn ="id";
		row.lookupValue="XXXX";
		rows.add(row);
		
		assertEquals(404, getClient(true).path("/XXXX/tabledata/"+tableName).put(rows).getStatus());
		assertEquals(404, getClient(true).path("/9999/tabledata/"+tableName).put(rows).getStatus());
		assertEquals(404, getClient(true).path("/9999/tabledata/"+tableName).put(rows).getStatus());
		
		//
		// FIXME - this should be a 404
		//
		assertEquals(404, getClient(true).path("/"+dbID+"/tabledata/nosuchtable").put(rows).getStatus());
	}
	
	@Test
	public void getTableNonexisting() throws Exception{
		loginUsingSSO("pingu@nowhere.co", "");
		assertEquals(404, getClient(true).path("/"+dbID+"/tabledata/nosuchtable").get().getStatus());
		assertEquals(404, getClient(true).path("/9999/tabledata/"+tableName).get().getStatus());
	}
	
	@Test
	public void addRowToTableNonexisting(){
		loginUsingSSO("pingu@nowhere.co", "");
		
		Row row = new Row();
		row.columnNames = new String[]{"id","n", "volume", "pubid","long"};
		row.values = new String[]{"XXXX","XXXX", "1", "1","1"};
		
		assertEquals(404, getClient(true).path("/XXXX/tabledata/"+tableName).post(row).getStatus());
		assertEquals(404, getClient(true).path("/9999/tabledata/"+tableName).post(row).getStatus());
		assertEquals(404, getClient(true).path("/9999/tabledata/"+tableName).post(row).getStatus());
		
		//
		// FIXME - this should be a 404
		//
		assertEquals(500, getClient(true).path("/"+dbID+"/tabledata/nosuchtable").post(row).getStatus());

	}
	
	//
	// Check we actually get relationship metadata
	//
	@Test
	public void getTableWithRefs() throws Exception{
		
		loginUsingSSO("pingu@nowhere.co", "");
		
		// create a csv file database
		File csvFile = new File(getClass().getResource("/databases/invoice.mdb").getFile());
		FileInputStream inputStream = new FileInputStream(csvFile);
		ContentDisposition cd = new ContentDisposition("attachment;filename=invoice.mdb");
		Attachment att = new Attachment("databaseFile", inputStream, cd);

		WebClient client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String path = response.getLocation().getPath();
		String id = path.substring(path.lastIndexOf('/')+1);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		AbstractResourceTest.databases.add(r);
		
		response = getClient(true).path("/"+id+"/tabledata/tblinvdetail").get();
		assertEquals(200, response.getStatus());
		TableData data = response.readEntity(TableData.class);
		
		assertEquals(1, data.primaryKeys.size());
		assertEquals("tblinv", (data.columns.get(0).referencedTable));
		assertEquals("InvNum", (data.columns.get(0).referencedColumn));

		logout();
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
	
	@Test
	public void addRowToTableWithSomeNullValues() throws Exception{		
		loginUsingSSO("pingu@nowhere.co", "");
		
		assertEquals(200, getClient(true).path("/"+dbID+"/tabledata/"+tableName).get().getStatus());

		//
		// Create the row
		//
		Row row = new Row();
		row.columnNames = new String[]{"id","n", "volume", "pubid","long"};
		row.values = new String[]{"XXXX","YYYY", "[null value]", "",null};
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
			if (
					((DataCell)currentRow.cell.get("id")).getValue().equals("XXXX") &&
					((DataCell)currentRow.cell.get("n")).getValue().equals("YYYY") &&
					((DataCell)currentRow.cell.get("volume")).getValue() == null &&
					((DataCell)currentRow.cell.get("pubid")).getValue() == null &&
					((DataCell)currentRow.cell.get("long")).getValue() == null
			){
				containsNewRow = true;
			}
		}
		assertTrue(containsNewRow);
		logout();
	}
	
	@Test
	public void addRowToTableWithEmpties() throws Exception{		
		loginUsingSSO("pingu@nowhere.co", "");
		
		assertEquals(200, getClient(true).path("/"+dbID+"/tabledata/"+tableName).get().getStatus());

		//
		// Create the row
		//
		Row row = new Row();
		row.columnNames = new String[]{"id","n", "volume", "pubid","long"};
		row.values = new String[]{"","", "", "",""};
		Response response = getClient(true).path("/"+dbID+"/tabledata/"+tableName).post(row);
		assertEquals(201, response.getStatus());
		
		logout();
	}
	
	@Test
	public void deleteRowFromTable() throws Exception{		
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
		
		//
		// Delete it
		//
		assertEquals(200, getClient(true).path("/"+dbID+"/tabledata/"+tableName).query("primaryKey", "id").query("primaryKeyValue", "XXXX").delete().getStatus());

		//
		// Check it was deleted
		//
		response = getClient(true).path("/"+dbID+"/tabledata/"+tableName).query("length", 400).get();
		assertEquals(200, response.getStatus());
		data = response.readEntity(TableData.class);
		containsNewRow = false;
		for (DataRow currentRow : data.rows){
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
		assertFalse(containsNewRow);
		
		logout();
	}
	
	@Test
	public void updateRowInTable() throws Exception{		
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
		// Update the row
		//
		ArrayList<Row> rows = new ArrayList<Row>();
		row.columnNames = new String[]{"id","n", "volume", "pubid","long"};
		row.values = new String[]{"XXXX","XXXX", "1", "1","99"};
		row.lookupColumn ="id";
		row.lookupValue="XXXX";
		rows.add(row);
		response = getClient(true).path("/"+dbID+"/tabledata/"+tableName).put(rows);
		assertEquals(200, response.getStatus());
		
		//
		// Check it exists
		//
		response = getClient(true).path("/"+dbID+"/tabledata/"+tableName).query("length", 400).get();
		assertEquals(200, response.getStatus());

		TableData data = response.readEntity(TableData.class);
		
		boolean containsNewRow = false;
		for (DataRow currentRow : data.rows){
			if (
					((DataCell)currentRow.cell.get("id")).getValue().equals("XXXX") &&
					((DataCell)currentRow.cell.get("n")).getValue().equals("XXXX") &&
					((DataCell)currentRow.cell.get("volume")).getValue().equals("1") &&
					((DataCell)currentRow.cell.get("pubid")).getValue().equals("1") &&
					((DataCell)currentRow.cell.get("long")).getValue().equals("99")
			){
				containsNewRow = true;
			}
		}
		assertTrue(containsNewRow);
		logout();
	}
	
	@Test
	public void updateRowInTable2() throws Exception{		
		loginUsingSSO("pingu@nowhere.co", "");
		
		// create an access database
		File csvFile = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream = new FileInputStream(csvFile);
		ContentDisposition cd = new ContentDisposition("attachment;filename=mondial.accdb");
		Attachment att = new Attachment("databaseFile", inputStream, cd);

		WebClient client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String path = response.getLocation().getPath();
		String id = path.substring(path.lastIndexOf('/')+1);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		AbstractResourceTest.databases.add(r);
		
		assertEquals(200, getClient(true).path("/"+id+"/tabledata/country").get().getStatus());
		
		//
		// Create the row
		//
		Row row = new Row();
		row.columnNames = new String[]{"CountryName","Code", "Capital", "Province", "Area", "Population"};
		row.values = new String[]{"Bogovia","BGR", "Bogov", "Bogov", "400", "575000"};
		response = getClient(true).path("/"+id+"/tabledata/country").post(row);
		assertEquals(201, response.getStatus());

		//
		// Update the row
		//
		ArrayList<Row> rows = new ArrayList<Row>();
		row.columnNames = new String[]{"CountryName","Code", "Capital", "Province", "Area", "Population"};
		row.values = new String[]{"Bogovia","BGR", "Bogov", "Bogov", "401", "576000"};
		row.lookupColumn ="Code";
		row.lookupValue="BGR";
		rows.add(row);
		response = getClient(true).path("/"+id+"/tabledata/country").put(rows);
		assertEquals(200, response.getStatus());
		
		//
		// Check it exists
		//
		response = getClient(true).path("/"+id+"/tabledata/country").query("length", 400).get();
		assertEquals(200, response.getStatus());

		TableData data = response.readEntity(TableData.class);
		
		boolean containsNewRow = false;
		for (DataRow currentRow : data.rows){
			if (
					((DataCell)currentRow.cell.get("CountryName")).getValue().equals("Bogovia") &&
					((DataCell)currentRow.cell.get("Code")).getValue().equals("BGR") &&
					((DataCell)currentRow.cell.get("Capital")).getValue().equals("Bogov") &&
					((DataCell)currentRow.cell.get("Area")).getValue().equals("401") &&
					((DataCell)currentRow.cell.get("Population")).getValue().equals("576000")
			){
				containsNewRow = true;
			}
		}
		assertTrue(containsNewRow);
		logout();
	}
	
	@Test
	public void updateRowInTableWithPartialData() throws Exception{		
		loginUsingSSO("pingu@nowhere.co", "");
		
		// create an access database
		File csvFile = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream = new FileInputStream(csvFile);
		ContentDisposition cd = new ContentDisposition("attachment;filename=mondial.accdb");
		Attachment att = new Attachment("databaseFile", inputStream, cd);

		WebClient client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String path = response.getLocation().getPath();
		String id = path.substring(path.lastIndexOf('/')+1);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		AbstractResourceTest.databases.add(r);
		
		assertEquals(200, getClient(true).path("/"+id+"/tabledata/country").get().getStatus());
		
		//
		// Create the row
		//
		Row row = new Row();
		row.columnNames = new String[]{"CountryName","Code", "Capital", "Province", "Area", "Population"};
		row.values = new String[]{"Bogovia","BGR", "Bogov", "Bogov", "400", "575000"};
		response = getClient(true).path("/"+id+"/tabledata/country").post(row);
		assertEquals(201, response.getStatus());

		//
		// Update the row - we'll leave out some values to check these aren't affected by the update
		//
		ArrayList<Row> rows = new ArrayList<Row>();
		row.columnNames = new String[]{"CountryName","Code", "Capital", "Province"};
		row.values = new String[]{"Bogovia","BGR", "Bogov", "Bogov"};
		row.lookupColumn ="Code";
		row.lookupValue="BGR";
		rows.add(row);
		response = getClient(true).path("/"+id+"/tabledata/country").put(rows);
		assertEquals(200, response.getStatus());
		
		//
		// Check it exists
		//
		response = getClient(true).path("/"+id+"/tabledata/country").query("length", 400).get();
		assertEquals(200, response.getStatus());

		TableData data = response.readEntity(TableData.class);
		
		boolean containsNewRow = false;
		for (DataRow currentRow : data.rows){
			if (
					((DataCell)currentRow.cell.get("CountryName")).getValue().equals("Bogovia") &&
					((DataCell)currentRow.cell.get("Code")).getValue().equals("BGR") &&
					((DataCell)currentRow.cell.get("Capital")).getValue().equals("Bogov") &&
					((DataCell)currentRow.cell.get("Area")).getValue().equals("400") &&
					((DataCell)currentRow.cell.get("Population")).getValue().equals("575000")
			){
				containsNewRow = true;
			}
		}
		assertTrue(containsNewRow);
		logout();
	}
	@Test
	public void updateRowInTableWithInvalidData() throws Exception{		
		loginUsingSSO("pingu@nowhere.co", "");
		
		// create an access database
		File csvFile = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream = new FileInputStream(csvFile);
		ContentDisposition cd = new ContentDisposition("attachment;filename=mondial.accdb");
		Attachment att = new Attachment("databaseFile", inputStream, cd);

		WebClient client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String path = response.getLocation().getPath();
		String id = path.substring(path.lastIndexOf('/')+1);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		AbstractResourceTest.databases.add(r);
		
		assertEquals(200, getClient(true).path("/"+id+"/tabledata/country").get().getStatus());
		
		//
		// Create the row
		//
		Row row = new Row();
		row.columnNames = new String[]{"CountryName","Code", "Capital", "Province", "Area", "Population"};
		row.values = new String[]{"Bogovia","BGR", "Bogov", "Bogov", "400", "575000"};
		response = getClient(true).path("/"+id+"/tabledata/country").post(row);
		assertEquals(201, response.getStatus());

		//
		// Update the row - we've used strings instead of numbers
		//
		ArrayList<Row> rows = new ArrayList<Row>();
		row.columnNames = new String[]{"CountryName","Code", "Capital", "Province", "Area", "Population"};
		row.values = new String[]{"Bogovia","BGR", "Bogov", "Bogot", "Small", "Not many"};
		row.lookupColumn ="Code";
		row.lookupValue="BGR";
		rows.add(row);
		response = getClient(true).path("/"+id+"/tabledata/country").put(rows);
		assertEquals(400, response.getStatus());
		
		//
		// Check it exists
		//
		response = getClient(true).path("/"+id+"/tabledata/country").query("length", 400).get();
		assertEquals(200, response.getStatus());

		TableData data = response.readEntity(TableData.class);
		
		boolean containsNewRow = false;
		for (DataRow currentRow : data.rows){
			if (
					((DataCell)currentRow.cell.get("CountryName")).getValue().equals("Bogovia") &&
					((DataCell)currentRow.cell.get("Code")).getValue().equals("BGR") &&
					((DataCell)currentRow.cell.get("Capital")).getValue().equals("Bogov") &&
					((DataCell)currentRow.cell.get("Area")).getValue().equals("400") &&
					((DataCell)currentRow.cell.get("Population")).getValue().equals("575000")
			){
				containsNewRow = true;
			}
		}
		assertTrue(containsNewRow);
		logout();
	}

}
