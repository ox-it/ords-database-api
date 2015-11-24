package uk.ac.ox.it.ords.api.database.conf;

public class CommonVars {
    public enum DataTypesSupported {
        ACCESS, DB_DUMP, CSV
    }
    
    /**
     * If useDifferentSchema is set to true, then a new schema is created to be used in a database
     * in place of the default public one. If false, then the database server's root user needs to be
     * available so that permissions can change on the public schema. 
     */
    public static boolean useDifferentSchema = false;
    public static final String SCHEMA_NAME = "public";
    
	public static final boolean developmentPhase = true;
	public static final String projectNameAppender = "ordsProject";


	/**
	 * This variable is useful for development. When set, the user does not need to log in to work with projects and
	 * databases. Obviously this should be set to false when the site is live.
	 */
	public static final boolean disableLogins = false;
    
    
    public static final String CONNECTABLE_DB = "postgres";//"template1"

	/**
	 * If this is set, users may have projects with the same name, else each project name must be
	 * unique. All spaces are stripped from the project name upon test and tests are case insensitive, so "My project"
	 * and "My Project" will be classed as the same name.
	 */
	public static boolean allowUserToHaveProjectsWithSameName = true;

    // TODO this can't stay like this
	public static final String ADDRESS_OF_IAM_WEBAPP = "http://129.67.241.37:8080/iam/ProjectRoleServlet";
    
    public static final String MessageServer = "129.67.241.43";
    
    
    public static final String PROJECT_PAGE = "projects.jsp";
    public static final String PREFS_PAGE = "preferences.jsp";
    public static final String DATAVIEW_PAGE = "datasetView.jsp";
    public static final String DATABASE_PAGE = "database.jsp";
    public static final String HOME_PAGE = "index.jsp";
    public static final String ERROR_PAGE = "unknownError.jsp";
    public static final String PROJECT_NODE_TABLES_PAGE = "table.jsp";
    public static final String PROJECT_NODE_QUERY_PAGE = "queryResult.jsp";
    
    public static final String configurationFolder = "/etc/ordsConfig/";
    public static String serverConfig = configurationFolder + "serverConfig.xml";
    public static String DB_PROPS = "./src/test/resources/props.properties";
    
    public static String hibernateFile = configurationFolder + "/hibernateSubset.cfg.xml";
    public static String mainHibernateFile = configurationFolder + "/hibernate.cfg.xml";
    public static String mainPropertiesFile = configurationFolder + "/db.properties";
    
    //public static String NULL_VALUE = "<null>"; - depricated
    public static boolean USE_ODBC = true;
    
    
    public static long ALLOWABLE_AGE_OF_ADDRESS_FOR_ODBC_ACCESS = 30*24*60*60*1000;//One month
    
    
    public static String BAD_CHARACTER_REPLACEMENT = "_";
    
    
    // Trial projects
    public static final int MAX_RECORDS_PER_TABLE_IN_FINAL_PROJECT = 100;
    public static final int MAX_DATA_SPACE_FOR_TRIAL_MB = 200;
    
    // Normal projects
    public static final int MAX_DATA_SPACE_MB = 10000;
}
