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