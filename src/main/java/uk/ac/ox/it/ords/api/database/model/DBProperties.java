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
package uk.ac.ox.it.ords.api.database.model;





public class DBProperties {
	private int id;
	public String projectName;
	public String workspaceHome;
	public String driverJar;
	public String serverLocation;
	public String userName;
	public String password;
	public String connectionString;
	public int databaseId;

	
	public DBProperties() {
	}


	public int getId() {
		return id;
	}


	public void setId(int id) {
		this.id = id;
	}


	public String getProjectName() {
		return projectName;
	}


	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}


	public String getWorkspaceHome() {
		return workspaceHome;
	}


	public void setWorkspaceHome(String workspaceHome) {
		this.workspaceHome = workspaceHome;
	}


	public String getDriverJar() {
		return driverJar;
	}


	public void setDriverJar(String driverJar) {
		this.driverJar = driverJar;
	}


	public String getServerLocation() {
		return serverLocation;
	}


	public void setServerLocation(String serverLocation) {
		this.serverLocation = serverLocation;
	}


	public String getUserName() {
		return userName;
	}


	public void setUserName(String userName) {
		this.userName = userName;
	}


	public String getPassword() {
		return password;
	}


	public void setPassword(String password) {
		this.password = password;
	}


	public String getConnectionString() {
		return connectionString;
	}


	public void setConnectionString(String connectionString) {
		this.connectionString = connectionString;
	}

	public int getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(int databaseId) {
		this.databaseId = databaseId;
	}
}