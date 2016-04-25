package uk.ac.ox.it.ords.api.database.conf;

public class CommonVars {
    
    /**
     * If useDifferentSchema is set to true, then a new schema is created to be used in a database
     * in place of the default public one. If false, then the database server's root user needs to be
     * available so that permissions can change on the public schema. 
     */
    public static final String SCHEMA_NAME = "public";
        
    public static final String CONNECTABLE_DB = "postgres";//"template1"

    public static String BAD_CHARACTER_REPLACEMENT = "_";

}
