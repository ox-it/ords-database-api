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

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.activation.DataHandler;
import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.cxf.jaxrs.ext.multipart.Attachment;
import org.apache.cxf.jaxrs.ext.multipart.Multipart;
import org.apache.shiro.SecurityUtils;

import uk.ac.ox.it.ords.api.database.data.ColumnReference;
import uk.ac.ox.it.ords.api.database.data.DataRow;
import uk.ac.ox.it.ords.api.database.data.Row;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.exceptions.BadParameterException;
import uk.ac.ox.it.ords.api.database.permissions.DatabasePermissionSets;
import uk.ac.ox.it.ords.api.database.permissions.DatabasePermissions;
import uk.ac.ox.it.ords.api.database.services.DatabaseUploadService;
import uk.ac.ox.it.ords.api.database.services.QueryService;
import uk.ac.ox.it.ords.api.database.services.SQLService;
import uk.ac.ox.it.ords.api.database.services.TableViewService;
import uk.ac.ox.it.ords.security.model.Permission;
import uk.ac.ox.it.ords.security.services.PermissionsService;

@Path("/database/{id}/{instance}")
public class Database {

	@PostConstruct
	public void init() throws Exception {
		initializePermissions();
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("dataset/{datasetid}")
	public Response getDatabaseDataset(@PathParam("id") final int id,
			@PathParam("instance") String instance,
			@PathParam("datasetid") int datasetID) {
		if (!SecurityUtils.getSubject().isPermitted("database:view:" + id)) {
			return Response.status(Response.Status.FORBIDDEN)
					.entity("Unauthorized").build();
		}
		TableData data = null;
		try {
			 data = tableViewService().getStaticDataSet(id, instance,
					datasetID);
		} 
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage())
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e.getMessage()).build();
		}
		return Response.ok(data).build();
	}

	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	@Path("dataset/{datasetid}/{query}")
	public Response updateDatabaseDataset(@PathParam("id") final int id,
			@PathParam("instance") String instance,
			@PathParam("datasetid") int datasetID,
			@PathParam("query") String queryString) {
		if (!SecurityUtils.getSubject().isPermitted("database:modify" + id)) {
			return Response.status(Response.Status.FORBIDDEN)
					.entity("Unauthorized").build();
		}
		try {
			tableViewService().updateStaticDataSet(id, instance, queryString);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage())
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e.getMessage()).build();
		}
		return Response.status(Response.Status.OK).entity("Success").build();
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("dataset/{databaseid}/{query}")
	public Response createDatabaseDatasert(@PathParam("id") final int id,
			@PathParam("instance") String instance,
			@PathParam("datasetid") int datasetID,
			@PathParam("query") String queryString) {
		if (!SecurityUtils.getSubject().isPermitted("database:modify" + id)) {
			return Response.status(Response.Status.FORBIDDEN)
					.entity("Unauthorized").build();
		}
		int staticDataSetId = 0;
		try {
			staticDataSetId = tableViewService().createStaticDataSetOnQuery(id, instance, queryString);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage())
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e.getMessage()).build();
		}

		return Response.status(Response.Status.CREATED).entity(new Integer(staticDataSetId)).build();

	}
	
	

	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	@Path("dataset/{databaseid}")
	public Response deleteDatabaseDataset(@PathParam("id") final int id,
			@PathParam("instance") String instance,
			@PathParam("datasetid") int datasetID) {
		if (!SecurityUtils.getSubject().isPermitted("database:delete" + id)) {
			return Response.status(Response.Status.FORBIDDEN)
					.entity("Unauthorized").build();
		}
		try {
			tableViewService().deleteStaticDataSet(id, instance, datasetID);
		}
		catch ( NotFoundException nfe ) {
			return Response.status(Response.Status.GONE).build();
		}
		catch ( Exception e ) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}
		return Response.status(Response.Status.OK).entity("Success").build();

	}

	/*
	 * @startindex=0
	 * 
	 * @rowsperpage=100
	 * 
	 * @filter
	 * 
	 * @filterparams
	 * 
	 * @sort
	 * 
	 * @direction
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("table/{tablename}/data")
	public Response getTableData(@PathParam("id") final int id,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@QueryParam("startIndex") int startIndex,
			@QueryParam("rowsperpage") int rowsPerPage,
			@QueryParam("filter") String filter,
			@QueryParam("sort") String sort,
			@QueryParam("direction") String direction) {
		if (!SecurityUtils.getSubject().isPermitted("database:view" + id)) {
			return Response.status(Response.Status.FORBIDDEN)
					.entity("Unauthorized").build();
		}
		TableData tableData = null;
		try {
			tableData = tableViewService().getDatabaseRows(id, instance, tableName, startIndex, rowsPerPage, filter, sort, direction);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage())
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e.getMessage()).build();
		}		
		return Response.status(Response.Status.OK).entity(tableData).build();

	}

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("table/{tablename}/data")
	public Response appendTableData(@PathParam("id") final int id,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName, Row newData) {
		if (!SecurityUtils.getSubject().isPermitted("database:modify" + id)) {
			return Response.status(Response.Status.FORBIDDEN)
					.entity("Unauthorized").build();
		}
		try {
			tableViewService().appendTableData(id, instance, tableName, newData);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage())
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e.getMessage()).build();
		}
		return Response.status(Response.Status.CREATED).entity("Success").build();

	}

	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("table/{tablename}/data/{lookupCol}/{lookupValue}")
	public Response updateTableRow(@PathParam("id") final int id,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("lookupCol") String lookupColName,
			@PathParam("lookupValue") String lookupValue, DataRow rowData) {
		if (!SecurityUtils.getSubject().isPermitted("database:modify" + id)) {
			return Response.status(Response.Status.FORBIDDEN)
					.entity("Unauthorized").build();
		}
		try {
			tableViewService().updateTableRow(id, instance, tableName, lookupColName, lookupValue, rowData);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage())
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e.getMessage()).build();
		}
		return Response.status(Response.Status.OK).entity("Success").build();

	}

	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("table/{tablename}/data")
	public Response deleteTableRow(@PathParam("id") final int id,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName, Row rowToRemove) {
		if (!SecurityUtils.getSubject().isPermitted("database:modify" + id)) {
			return Response.status(Response.Status.FORBIDDEN)
					.entity("Unauthorized").build();
		}
		try {
			tableViewService().deleteTableData(id, instance, tableName, rowToRemove);
		}
		catch ( NotFoundException nfe ) {
			return Response.status(Response.Status.GONE).build();
		}
		catch ( Exception e ) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}
		return Response.status(Response.Status.OK).entity("Success").build();

	}

	/*
	 * @referencedtablecolumn
	 * 
	 * @startindex=0
	 * 
	 * @rowsperpage=100
	 * 
	 * @filter
	 * 
	 * @filterparams
	 * 
	 * @sort
	 * 
	 * @direction
	 * 
	 * @query don't need these
	 * 
	 * need to return triples (id/value/label)
	 */

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("table/{tablename}/column/{foreignkeycolumn}/related/")
	public Response getReferencedColumns(@PathParam("id") final int id,
			@PathParam("instance") String instance,
			@PathParam("tablename") String tableName,
			@PathParam("foreignkeycolumn") String foreignKeyColumn) {

		if (!SecurityUtils.getSubject().isPermitted("database:modify" + id)) {
			return Response.status(Response.Status.FORBIDDEN)
					.entity("Unauthorized").build();
		}
		List<ColumnReference> cols = new ArrayList<ColumnReference>();
		try {
			 cols = queryService().getColumnsFromRelation(id, instance, tableName, foreignKeyColumn);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage())
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e.getMessage()).build();
		}
		
		return Response.status(Response.Status.OK).entity(cols).build();
	}

	/*
	 * @q
	 * 
	 * @startindex=0
	 * 
	 * @rowsperpage=100
	 * 
	 * @filter
	 * 
	 * @order
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("query")
	public Response doQuery(@PathParam("id") final int id,
			@PathParam("instance") String instance,
			@QueryParam("q") String theQuery,
			@QueryParam("startindex") int startIndex,
			@QueryParam("rowsperpage") int rowsPerPage,
			@QueryParam("filter") String filter,
			@QueryParam("order") String order) {
		if (!SecurityUtils.getSubject().isPermitted("database:modify" + id)) {
			return Response.status(Response.Status.FORBIDDEN)
					.entity("Unauthorized").build();
		}
		TableData data = null;
		try {
			data = queryService().performQuery(id, instance, theQuery, startIndex, rowsPerPage, filter, order);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage())
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e.getMessage()).build();
		}

		return Response.status(Response.Status.OK).entity(data).build();

	}

	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("data")
	public Response exportDataBase(@PathParam("id") final int id,
			@PathParam("instance") String instance) {
		if (!SecurityUtils.getSubject().isPermitted("database:modify" + id)) {
			return Response.status(Response.Status.FORBIDDEN)
					.entity("Unauthorized").build();
		}
		String sql = "";
		try {
			sql = SQLService.Factory.getInstance().buildSQLExportForDatabase(id, instance);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage())
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e.getMessage()).build();
		}
		return Response.status(Response.Status.OK).entity(sql).build();
	}
	

	@POST
	@Path("data")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response handleFileUpload(
			@PathParam("id") int dbId,
			@PathParam("instance") String instance, 
			@Multipart("databaseFile") Attachment fileAttachment,
			@Context HttpServletRequest request) {
		if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_CREATE)) {
			return Response.status(Response.Status.FORBIDDEN)
					.entity("Unauthorized").build();
		}
		//MediaType contentType = fileAttachment.getContentType();
		//contentType.
		MultivaluedMap<String, String> map = fileAttachment.getHeaders();
		String fileName = getFileName(map);
		String extension = getFileExtension(fileName);
		if ( extension == null || (!extension.equalsIgnoreCase("csv") &&
				!extension.equalsIgnoreCase("mdb") &&
				!extension.equalsIgnoreCase("accdb"))) {
			return Response.status(Response.Status.UNSUPPORTED_MEDIA_TYPE).build();
		}
		ServletContext context = request.getSession().getServletContext();
		File tempDirectory = (File)context.getAttribute("javax.servlet.context.tmpdir");
		if (tempDirectory == null ) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("temp directory not set on server").build();
		}
		File dbFile = new File(tempDirectory, fileName);
		
		DataHandler handler = fileAttachment.getDataHandler();
		try {
			InputStream stream = handler.getInputStream();
			saveFile(dbFile, stream);
			DatabaseUploadService.Factory.getInstance().createNewDatabaseFromFile(dbId, dbFile, extension);		}
		catch (Exception e ) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}
		return Response.ok().build();
	}

	private QueryService queryService() {
		return QueryService.Factory.getInstance();
	}

	private TableViewService tableViewService() {
		return TableViewService.Factory.getInstance();
	}
	
    private String getFileName(MultivaluedMap<String, String> header) {
    	 
        String[] contentDisposition = header.getFirst("Content-Disposition").split(";");
 
        for (String filename : contentDisposition) {
            if ((filename.trim().startsWith("filename"))) {
 
                String[] name = filename.split("=");
 
                String finalFileName = name[1].trim().replaceAll("\"", "");
                return finalFileName;
            }
        }
        return "unknown";
    }

    private String getFileExtension(String fileName ) {
    	if ( fileName.lastIndexOf('.') == -1 ) {
    		return null;
    	}
    	return fileName.substring(fileName.lastIndexOf('.')+1);
    }
    
    
    private void saveFile ( File f, InputStream stream ) throws Exception {
        OutputStream out = new FileOutputStream(f);
        
        int read = 0;
        byte[] bytes = new byte[1024];
        while ((read = stream.read(bytes)) != -1) {
            out.write(bytes, 0, read);
        }
        stream.close();
        out.flush();
        out.close();

    }
    
    
    private void initializePermissions() throws Exception {
		PermissionsService service = PermissionsService.Factory.getInstance();
		//
		// Anyone with the "User" role can contribute to projects
		//
		for (String permission : DatabasePermissionSets.getPermissionsForUser()){
			Permission permissionObject = new Permission();
			permissionObject.setRole("user");
			permissionObject.setPermission(permission);
			service.createPermission(permissionObject);
		}
		
		//
		// Anyone with the "LocalUser" role can create new trial projects
		//
		for (String permission : DatabasePermissionSets.getPermissionsForLocalUser()){
			Permission permissionObject = new Permission();
			permissionObject.setRole("localuser");
			permissionObject.setPermission(permission);
			service.createPermission(permissionObject);
		}
		
		//
		// Anyone with the "Administrator" role can create new full
		// projects and upgrade projects to full, and update any
		// user projects
		//
		for (String permission : DatabasePermissionSets.getPermissionsForSysadmin()){
			Permission permissionObject = new Permission();
			permissionObject.setRole("administrator");
			permissionObject.setPermission(permission);
			service.createPermission(permissionObject);
		}

		//
		// "Anonymous" can View public projects
		//
		for (String permission : DatabasePermissionSets.getPermissionsForAnonymous()){
			Permission permissionObject = new Permission();
			permissionObject.setRole("anonymous");
			permissionObject.setPermission(permission);
			service.createPermission(permissionObject);
		}

    }
}
