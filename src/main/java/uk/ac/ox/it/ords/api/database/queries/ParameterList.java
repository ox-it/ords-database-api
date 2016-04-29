package uk.ac.ox.it.ords.api.database.queries;

import java.util.ArrayList;

import uk.ac.ox.it.ords.api.database.data.DataTypeMap;
import uk.ac.ox.it.ords.api.database.data.DataCell.DataType;


/**
 * Wrapper for a set of parameters to pass into a Postgres query
 * We do this as Postgres is strongly typed, so we need to know
 * the types of the parameters being set when executing a 
 * PreparedStatement.
 * 
 * @author scottw
 *
 */
public class ParameterList {
	

	private ArrayList<DataTypeMap> parameters;
	
	public ParameterList() {
		parameters = new ArrayList<DataTypeMap>();
	}
	
	public DataTypeMap getParameter(int index){
		return parameters.get(index);
	}
	
	public void addParameter(String param){
		DataTypeMap parameter = new DataTypeMap();
		parameter.dt = DataType.VARCHAR;
		parameter.stringValue = param;
		parameters.add(parameter);
	}
	
	public void addParameter(DataTypeMap param){
		parameters.add(param);
	}
	
	public DataType getTypeForNull(int index){
		DataTypeMap parameter = getParameter(index);
		if (parameter.intValue == -1 && parameter.dt.equals(DataType.NULL)){
			return DataType.INTEGER;
		} else {
			return DataType.VARCHAR;
		}
	}
	
	public void addParameter(boolean value){
		DataTypeMap parameter = new DataTypeMap();
		parameter.dt = DataType.BOOLEAN;
		parameter.intValue = value ? 1 : 0;
		parameter.stringValue = value ? "t" : "f";
		parameters.add(parameter);	
	}
		
	public void addNull(){
		DataTypeMap parameter = new DataTypeMap();
		parameter.dt = DataType.NULL;
		parameter.stringValue = "NULL";
		parameter.intValue = -1;
		parameters.add(parameter);
	}
	
	public void addParameter(int value){
		DataTypeMap parameter = new DataTypeMap();
		parameter.dt = DataType.INTEGER;
		parameter.intValue = value;
		parameter.stringValue = String.valueOf(value);
		parameters.add(parameter);
	}
	
	public void addParameter(Object object){
		DataTypeMap parameter = new DataTypeMap();
		parameter.dt = DataType.JAVA_OBJECT;
		parameter.stringValue = (String)object;
		parameters.add(parameter);
	}
	
	public void setParameterValue(int index, String param){
		DataTypeMap parameter = getParameter(index);
		parameter.stringValue = param;
	}
	
	public int size(){
		return parameters.size();
	}
	
	public String[] toArray(){
		String[] params = new String[parameters.size()];
		for (int i=0; i<parameters.size(); i++){
			params[i] = getParameter(i).stringValue;
		}
		return params;
	}
	
}
