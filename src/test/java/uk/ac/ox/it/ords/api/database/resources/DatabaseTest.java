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
import uk.ac.ox.it.ords.api.database.data.TableViewInfo;

public class DatabaseTest extends AbstractDatabaseTestRunner{


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

			Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
			assertEquals(201, response.getStatus());
			
			String path = response.getLocation().getPath();
			String id = path.substring(path.lastIndexOf('/')+1);
			DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
			AbstractResourceTest.databases.add(r);
			
			// get the data from the table created as small_test
			
			client = getClient(false);
			String tablePath = "/"+id+"/tabledata/small_test";
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
			response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
			assertEquals(201, response.getStatus());
			
			path = response.getLocation().getPath();
			id = path.substring(path.lastIndexOf('/')+1);
			r = new DatabaseReference(Integer.parseInt(id), false);
			AbstractResourceTest.databases.add(r);
			
			
			// test view
			TableViewInfo viewInfo = new TableViewInfo();
			viewInfo.setViewQuery("SELECT CityName, Population from City");
			viewInfo.setViewName("Population City");
			viewInfo.setViewDescription("A view on the city table showing population and city name");
			viewInfo.setViewAuthorization("public");
			viewInfo.setViewTable("City");
			
			client = getClient(true);
			response = client.path("/"+id+"/dataset").post(viewInfo);
			assertEquals(201, response.getStatus());
			
			String viewPath = response.getLocation().getPath();
			String viewId = viewPath.substring(viewPath.lastIndexOf('/')+1);

			
			// view the view
			client = getClient(true);
			response = client.path("/"+id+"/dataset/"+viewId).get();
			assertEquals(200, response.getStatus());
			
			
			// update the view
			viewInfo.setViewName("The population of Cities");
			viewInfo.setViewAuthorization("authmembers");
			client = getClient(true);
			response = client.path("/"+id+"/dataset/"+viewId).put(viewInfo);
			assertEquals(200, response.getStatus());

			

			
			// delete the view
			client = getClient(true);
			response = client.path("/"+id+"/dataset/"+viewId).delete();
			assertEquals(200, response.getStatus());
			
			
			
			client = getClient(false);
			response = client.path("/"+id+"/tabledata/country").get();
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
