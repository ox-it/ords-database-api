package uk.ac.ox.it.ords.api.database.resources;

public class DatabaseReference {
	public int id;
	public boolean staging;
	
	public DatabaseReference ( int id, boolean staging) {
		this.id = id;
		this.staging = staging;
	}
}
