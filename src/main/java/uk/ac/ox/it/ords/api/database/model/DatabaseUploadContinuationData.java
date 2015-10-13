package uk.ac.ox.it.ords.api.database.model;

import java.util.UUID;




public class DatabaseUploadContinuationData {
	private int id;
	private String uuid;
	private int physicalDatabaseId;
	private String schemaDir;
	private String schema;
	private String csvDir;
	
	public DatabaseUploadContinuationData() {
		setUuid(UUID.randomUUID().toString());
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public int getPhysicalDatabaseId() {
		return physicalDatabaseId;
	}

	public void setPhysicalDatabaseId(int physicalDatabaseId) {
		this.physicalDatabaseId = physicalDatabaseId;
	}

	public String getSchemaDir() {
		return schemaDir;
	}

	public void setSchemaDir(String schemaDir) {
		this.schemaDir = schemaDir;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getCsvDir() {
		return csvDir;
	}

	public void setCsvDir(String csvDir) {
		this.csvDir = csvDir;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
}
