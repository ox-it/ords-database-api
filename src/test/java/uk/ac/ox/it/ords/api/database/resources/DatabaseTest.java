package uk.ac.ox.it.ords.api.database.resources;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import javax.ws.rs.core.Response;

import org.junit.Test;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.jaxrs.ext.multipart.Attachment;
import org.apache.cxf.jaxrs.ext.multipart.ContentDisposition;
import org.apache.cxf.jaxrs.ext.multipart.MultipartBody;

import uk.ac.ox.it.ords.api.database.data.TableData;

public class DatabaseTest extends AbstractResourceTest{


	@Test ()
	public void uploadCSVFile() {
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		
		File csvFile = new File(getClass().getResource("/small_test.csv").getFile());
		File accessFile = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream;
		try {
			
			// create a csv file database
			inputStream = new FileInputStream(csvFile);
			ContentDisposition cd = new ContentDisposition("attachment;filename=small_test.csv");
			Attachment att = new Attachment("databaseFile", inputStream, cd);

			Response response = client.path("/10/MAIN/data/localhost").post(new MultipartBody(att));
			assertEquals(201, response.getStatus());
			
			String path = response.getLocation().getPath();
			String id = path.substring(path.lastIndexOf('/')+1);
			
			AbstractResourceTest.databaseIds.add(id);
			
			// get the data from the table created as small_test
			
			client = getClient(false);
			String tablePath = "/"+id+"/MAIN/tabledata/small_test/0/1000/none/none/none";
			response = client.path(tablePath).get();
			assertEquals(200, response.getStatus());
			TableData tableData = response.readEntity(TableData.class);
			//InputStream stream = (InputStream) response.getEntity();
			assertEquals(tableData.columns.size(), 6 );
			
			//this.getResponseFromInputStream(stream);
			
			// create an access database import
			inputStream = new FileInputStream(accessFile);
			cd = new ContentDisposition("attachement;filename=mondial.accdb");
			att = new Attachment("databaseFile", inputStream, cd);
			client = getClient(false);
			client.type("multipart/form-data");
			response = client.path("/11/MAIN/data/localhost").post(new MultipartBody(att));
			assertEquals(201, response.getStatus());
			
			path = response.getLocation().getPath();
			id = path.substring(path.lastIndexOf('/')+1);
			AbstractResourceTest.databaseIds.add(id);
			
			String sql = "SELECT * FROM city";
			String queryPath = "/"+id+"/MAIN/query";
			client = getClient(false);
			response = client.path(queryPath).query("q", sql).query("startindex", 0).query("rowsperpage", 100).get();
			assertEquals(200, response.getStatus());
			InputStream stream = (InputStream) response.getEntity();
			this.getResponseFromInputStream(stream, "mondial.json");
			//tableData = response.readEntity(TableData.class);
			
			//System.out.println("Number of Rows: "+tableData.getNumberOfRowsInEntireTable());
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	
	private void getResponseFromInputStream(InputStream is, String fileName) {
        try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                String next = null;
                PrintWriter writer = new PrintWriter("/tmp/"+fileName);
                while ((next = reader.readLine()) != null) {
                        writer.println(next);
                }
                writer.close();
        } catch (IOException e) {
                e.printStackTrace();
        }
}



}
