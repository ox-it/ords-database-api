package uk.ac.ox.it.ords.api.database.queries;

import static org.junit.Assert.*;

import org.junit.Test;

import uk.ac.ox.it.ords.api.database.data.DataCell.DataType;

public class FilteredQueryTest {
	
	private static String createFilterFromParameters(String filter, String params) throws Exception{
		return createFilterFromParameters(filter, params, true);
	}
	
	/**
	 * Create a single static SQL statement from a filter and param string. This should
	 * only ever be used for testing!
	 * @param filter
	 * @param params
	 * @return
	 * @throws Exception 
	 */
	private static String createFilterFromParameters(String filter, String params, boolean isCaseSensitive) throws Exception{
		ParameterList parameters = new FilteredQuery(filter, params, isCaseSensitive).getParameterList(params);
		String query = new FilteredQuery().createFilterFromParameters(filter, parameters, isCaseSensitive);
	    //
        // Substitute params for placeholders in SQL
        //
		if (parameters != null && query != null && parameters.size() > 0) {
			query = String.format(query.replaceAll("\\?", "'%s'"), parameters.toArray());
		}
		return query;
	}
	
	@Test
	public void normaliseFilter(){
		String filter = "SELECT * FROM \"documents\" INNER JOIN \"sites\" \"x0\" ON \"docsite\" = \"x0\".\"id\" WHERE (\"x0\".\"sitename\" = ?)";
		String expected = "SELECT * FROM \"documents\" INNER JOIN \"sites\" \"sites\" ON \"docsite\" = \"sites\".\"id\" WHERE (\"sites\".\"sitename\" = ?)"; 
		assertEquals(expected, new FilteredQuery().normaliseFilterQuery(filter));
		
		filter="SELECT * FROM \"documents\" INNER JOIN \"sites\" \"x2\" ON \"docsite\" = \"x2\".\"id\" WHERE (\"x2\".\"sitename\" CONTAINS ?)";
		expected = "SELECT * FROM \"documents\" INNER JOIN \"sites\" \"sites\" ON \"docsite\" = \"sites\".\"id\" WHERE (\"sites\".\"sitename\" CONTAINS ?)";
		assertEquals(expected, new FilteredQuery().normaliseFilterQuery(filter));
		
		filter="SELECT * FROM \"x0x1x2\" INNER JOIN \"sites\" \"x2\" ON \"docsite\" = \"x2\".\"id\" WHERE (\"x2\".\"sitename\" CONTAINS ?)";
		expected = "SELECT * FROM \"x0x1x2\" INNER JOIN \"sites\" \"sites\" ON \"docsite\" = \"sites\".\"id\" WHERE (\"sites\".\"sitename\" CONTAINS ?)";
		assertEquals(expected, new FilteredQuery().normaliseFilterQuery(filter));
		
		filter="SELECT \"x0\".* FROM \"city\" \"x0\" WHERE (\"x0\".\"population\" > ?)";
		expected = "SELECT \"city\".* FROM \"city\" \"city\" WHERE (\"city\".\"population\" > ?)";
		assertEquals(expected, new FilteredQuery().normaliseFilterQuery(filter));
	}

	@Test
	public void integerCasting() throws Exception{
		String filter = "SELECT * FROM \"documents\" WHERE \"docid\" = ?";
		String params = "[{'type':'number', 'value':'4'}]";
		String output = createFilterFromParameters(filter, params);
		String expected = "SELECT * FROM \"documents\" WHERE \"docid\" = '4'";
		assertEquals(expected, output);
		ParameterList parameterList = new FilteredQuery().getParameterList(params);
		assertEquals(DataType.INTEGER, parameterList.getParameter(0).dt);
		assertEquals(4, parameterList.getParameter(0).intValue);
	}

	@Test
	public void integerCastingWithoutQuotes() throws Exception{
		String filter = "SELECT * FROM \"documents\" WHERE \"docid\" = ?";
		String params = "[{\"type\":\"number\", \"value\":4}]";
		String output = createFilterFromParameters(filter, params);
		String expected = "SELECT * FROM \"documents\" WHERE \"docid\" = '4'";
		assertEquals(expected, output);
		ParameterList parameterList = new FilteredQuery().getParameterList(params);
		assertEquals(DataType.INTEGER, parameterList.getParameter(0).dt);
		assertEquals(4, parameterList.getParameter(0).intValue);
	}
	
	@Test
	public void booleans() throws Exception{
		String filter = "SELECT * FROM \"documents\" WHERE \"isgood\" = ?";
		String params = "[{'type':'boolean', 'value':true}]";
		String output = createFilterFromParameters(filter, params);
		String expected = "SELECT * FROM \"documents\" WHERE \"isgood\" = 't'";
		assertEquals(expected, output);
		ParameterList parameterList = new FilteredQuery().getParameterList(params);
		assertEquals(DataType.BOOLEAN, parameterList.getParameter(0).dt);
		assertEquals(1, parameterList.getParameter(0).intValue);
 	}
	
	@Test
	public void invalidIntegerCasting() throws Exception{
		String filter = "SELECT * FROM \"documents\" WHERE \"docid\" = ?";
		String params = "[{'type':'number', 'value':'banana'}]";
		ParameterList parameterList = new FilteredQuery().getParameterList(params);
		assertEquals(DataType.NULL, parameterList.getParameter(0).dt);
		assertEquals(-1, parameterList.getParameter(0).intValue);
		assertEquals("NULL", parameterList.getParameter(0).stringValue);
		String output = createFilterFromParameters(filter, params);
		String expected = "SELECT * FROM \"documents\" WHERE \"docid\" = 'NULL'";
		assertEquals(expected, output);
 	}
	
	@Test
	public void getParameterList() throws Exception{
		String params =  null;
		ParameterList parameters = new FilteredQuery().getParameterList(params);
		assertNotNull(parameters);
		assertTrue(parameters.size() == 0);
		
		params = "[{\"type\":\"date\",\"value\":\"2015-05-31T23:00:00.000Z\"}]";
		parameters = new FilteredQuery().getParameterList(params);
		assertNotNull(parameters);
		assertEquals(1, parameters.size());
		assertEquals("2015-05-31 23:00:00.000", parameters.getParameter(0).stringValue);
		assertEquals(DataType.TIMESTAMP, parameters.getParameter(0).dt);		
		
		params = "[{&quot;type&quot;:&quot;string&quot;,&quot;value&quot;:&quot;apple&quot;}]";
		parameters = new FilteredQuery().getParameterList(params);
		assertNotNull(parameters);
		assertEquals(1, parameters.size());
		assertEquals("apple", parameters.getParameter(0).stringValue);
		assertEquals(DataType.VARCHAR, parameters.getParameter(0).dt);
		
		params = "[{'type':'string', 'value':'apple'}, {'type':'string', 'value':'banana'}]";
		parameters = new FilteredQuery().getParameterList(params);
		assertNotNull(parameters);
		assertTrue(parameters.size() == 2);
		assertEquals("apple", parameters.getParameter(0).stringValue);
		assertEquals(DataType.VARCHAR, parameters.getParameter(0).dt);
		
		params = "[{\"type\":\"string\", \"value\":\"apple\"}, {\"type\":\"string\", \"value\":\"banana\"}]";
		parameters = new FilteredQuery().getParameterList(params);
		assertNotNull(parameters);
		assertTrue(parameters.size() == 2);
		assertEquals("apple", parameters.getParameter(0).stringValue);
		assertEquals(DataType.VARCHAR, parameters.getParameter(0).dt);
	}
	
	
	
    /**
     * Tests filter construction
     * @throws Exception 
     */
    @Test
    public void filtering() throws Exception{
    	
    	//
    	// Good input
    	//
    	String sql = "SELECT * FROM &quot;documents&quot; WHERE docsite = ?";
    	String params = "[{'type':'string', 'value':'apple'}]";
    	String output = createFilterFromParameters(sql, params);
    	String expected = "SELECT * FROM \"documents\" WHERE docsite = 'apple'";
    	assertEquals(expected, output);
    	
    	//
    	// NOT input
    	//
    	sql = "SELECT * FROM &quot;documents&quot; WHERE docsite <> ?";
    	params = "[{'type':'string', 'value':'apple'}]";
    	output = createFilterFromParameters(sql, params);
    	expected = "SELECT * FROM \"documents\" WHERE docsite <> 'apple'";
    	assertEquals(expected, output);
    	
    	// 
    	// Null filter
    	//
    	sql = null;
    	output = createFilterFromParameters(sql, params);
    	assertNull(output);
    	
    	//
    	// Both null
    	//
    	params = null;
       	output = createFilterFromParameters(sql, params);
    	assertNull(output);
    	
    	//
    	// Null params
    	//
    	sql = "SELECT * FROM &quot;documents&quot; WHERE docsite = ?";
    	params = null;
       	output = createFilterFromParameters(sql, params);
    	assertNull(output);
    	
    	//
    	// "null" strings
    	//
    	sql = "null";
    	params = "null";
       	output = createFilterFromParameters(sql, params);
    	assertNull(output);
    	
    	//
    	// No entities
    	//
    	sql = "SELECT * FROM \"documents\" WHERE docsite = ?";
    	params = "[{'type':'string', 'value':'apple'}]";
    	output = createFilterFromParameters(sql, params);
        expected = "SELECT * FROM \"documents\" WHERE docsite = 'apple'";
    	assertEquals(expected, output);
    	
    	//
    	// Empty Strings
    	//
    	sql = "";
    	params = "";
    	output = createFilterFromParameters(sql, params);
    	assertNull(output);
    	
    	sql = " ";
    	params = " ";
    	output = createFilterFromParameters(sql, params);
    	assertNull(output);
    	
    	sql = "SELECT * FROM \"documents\" WHERE docsite = ?";
    	params = "";
    	output = createFilterFromParameters(sql, params);
    	assertNull(output);
    	
    	sql = "  ";
    	params = "['apple']";
    	output = createFilterFromParameters(sql, params);
    	assertNull(output);
    	
    	//
    	// No params given
    	//
    	sql = "SELECT * FROM &quot;documents&quot;";
    	params = "[]";
    	output = createFilterFromParameters(sql, params);
        expected = "SELECT * FROM \"documents\"";
    	assertEquals(expected, output);
    	
    	//
    	// Mismatching parameters
    	//
    	sql = "SELECT * FROM \"documents\" WHERE docsite = ?";
    	params = "['apple','banana']";
    	output = createFilterFromParameters(sql, params);
    	assertNull(output);
    	
    	sql = "SELECT * FROM \"documents\" WHERE docsite = ? AND doclang = ?";
    	params = "['apple']";
    	output = createFilterFromParameters(sql, params);
    	assertNull(output);
    	
    	//
    	// CONTAINS replacement
    	//
    	sql = "SELECT * FROM &quot;documents&quot; WHERE (name CONTAINS ?)";
    	params = "[{'type':'string', 'value':'banana'}]";
    	output = createFilterFromParameters(sql, params);
        expected = "SELECT * FROM \"documents\" WHERE (name LIKE '%banana%')";
    	assertEquals(expected, output);
    	
    	//
    	// Multiple CONTAINS replacement
    	//
    	sql = "SELECT * FROM &quot;documents&quot; WHERE (name CONTAINS ?) AND (type CONTAINS ?)";
    	params = "[{'type':'string', 'value':'banana'}, {'type':'string', 'value':'fruit'}]";
    	output = createFilterFromParameters(sql, params);
        expected = "SELECT * FROM \"documents\" WHERE (name LIKE '%banana%') AND (type LIKE '%fruit%')";
    	assertEquals(expected, output);
    }
    
    @Test
    public void caseSensitivty() throws Exception{
    	String sql = "SELECT * FROM &quot;documents&quot; WHERE (name LIKE ?)";
    	String params = "[{'type':'string', 'value':'%banana%'}]";
    	String output = createFilterFromParameters(sql, params, true);
        String expected = "SELECT * FROM \"documents\" WHERE (name LIKE '%banana%')";
    	assertEquals(expected, output);
    	
    	output = createFilterFromParameters(sql, params, false);
        expected = "SELECT * FROM \"documents\" WHERE (name ILIKE '%banana%')";
    	assertEquals(expected, output);
    	
    	sql = "SELECT * FROM &quot;documents&quot; WHERE (name ILIKE ?)";
    	output = createFilterFromParameters(sql, params, true);
    	expected = "SELECT * FROM \"documents\" WHERE (name LIKE '%banana%')";
    	assertEquals(expected, output);
    }

}
