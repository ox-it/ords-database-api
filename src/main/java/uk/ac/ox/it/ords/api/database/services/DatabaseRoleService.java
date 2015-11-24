package uk.ac.ox.it.ords.api.database.services;

import java.util.List;
import java.util.ServiceLoader;

import uk.ac.ox.it.ords.api.database.services.impl.hibernate.DatabaseRoleServiceImpl;
import uk.ac.ox.it.ords.security.model.UserRole;

public interface DatabaseRoleService {
	/**
	 * Update a role
	 * @param userRole
	 */
	public void updateDatabseRole(UserRole userRole, int dbId) throws Exception;
	
	/**
	 * Gets the project owner, if any
	 */
	public UserRole getDatabaseOwner(int dbId) throws Exception;
	
	/**
	 * Create the Owner role and their permissions; called once when a new project is created
	 * @param groupId
	 * @throws Exception
	 */
	public void createInitialPermissions(int groupId) throws Exception;
	
	/**
	 * Delete all the permissions and roles associated with a project; called once when a project is deleted
	 * @param groupId
	 * @throws Exception
	 */
	public void deletePermissions(int dbId) throws Exception;
	
	/**
	 * Return all the UserRoles that match the pattern of the project
	 * @param groupId
	 * @return a List of UserRole objects
	 * @throws Exception
	 */
	public List<UserRole> getUserRolesForDatabase(int dbId) throws Exception;
	

	/**
	 * Return the specified UserRole instance
	 * @param roleId 
	 * @return the UserRole specified, or null if there is no match
	 * @throws Exception
	 */
	public UserRole getUserRole(int roleId) throws Exception;
	
	/**
	 * Create the UserRole 
	 * @param groupId
	 * @param userRole
	 * @return the UserRole that has been persisted
	 * @throws Exception
	 */
	public UserRole addUserRoleToDatabase(int dbId, UserRole userRole) throws Exception;
	
	/**
	 * Remove the UserRole
	 * @param projectid
	 * @param roleId
	 * @throws Exception
	 */
	public void removeUserFromRoleInDatabase(int dbId, int roleId) throws Exception;	

	/**
	 * The enumeration of valid UserRole types
	 */
    public enum DatabaseRole {
        owner, databaseadministrator, contributor, viewer, deleted
    };

	   public static class Factory {
			private static DatabaseRoleService provider;
		    public static DatabaseRoleService getInstance() {
		    	//
		    	// Use the service loader to load an implementation if one is available
		    	// Place a file called uk.ac.ox.oucs.ords.utilities.csv in src/main/resources/META-INF/services
		    	// containing the classname to load as the CsvService implementation. 
		    	// By default we load the Hibernate implementation.
		    	//
		    	if (provider == null){
		    		ServiceLoader<DatabaseRoleService> ldr = ServiceLoader.load(DatabaseRoleService.class);
		    		for (DatabaseRoleService service : ldr) {
		    			// We are only expecting one
		    			provider = service;
		    		}
		    	}
		    	//
		    	// If no service provider is found, use the default
		    	//
		    	if (provider == null){
		    		provider = new DatabaseRoleServiceImpl();
		    	}
		    	
		    	return provider;
		    }
	   }

	String getPrivateUserRole(String role, int projectId);

	String getPublicUserRole(String role);

}
