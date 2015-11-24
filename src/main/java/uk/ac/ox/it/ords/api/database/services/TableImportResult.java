package uk.ac.ox.it.ords.api.database.services;

import java.util.ArrayList;


/**
 * Basic reporting class for import result statistics.
 *
 */
public class TableImportResult {
	
	public static final int NOT_STARTED = 0;
	public static final int SUCCESSFUL = 1;
	public static final int FAILED = 2;
	public static final int PARTIAL = 3;
	
	private int tableCreateResult;
	private int tableConstraintResult;
	private int dataImportResult;
	
	private int rowsReceived;
	private int rowsImported;
	private int rowsVerified;
	private int constraintsAdded;
	
	private ArrayList<Exception> exceptions;

	public TableImportResult() {
		exceptions = new ArrayList<Exception>();
		rowsReceived = -1;
		rowsImported = -1;	
		rowsVerified = -1;
		constraintsAdded = 0;
		tableCreateResult = NOT_STARTED;
		tableConstraintResult = NOT_STARTED;
		dataImportResult = NOT_STARTED;
	}
	
	
	
	public int getTableCreateResult() {
		return tableCreateResult;
	}



	public void setTableCreateResult(int tableCreateResult) {
		this.tableCreateResult = tableCreateResult;
	}



	public int getTableConstraintResult() {
		return tableConstraintResult;
	}



	public void setTableConstraintResult(int tableConstraintResult) {
		this.tableConstraintResult = tableConstraintResult;
	}



	public int getDataImportResult() {
		return dataImportResult;
	}



	public void setDataImportResult(int dataImportResult) {
		this.dataImportResult = dataImportResult;
	}



	public int getRowsReceived() {
		return rowsReceived;
	}



	public void setRowsReceived(int rowsReceived) {
		this.rowsReceived = rowsReceived;
	}



	public int getRowsImported() {
		return rowsImported;
	}



	public void setRowsImported(int rowsImported) {
		this.rowsImported = rowsImported;
	}



	public int getRowsVerified() {
		return rowsVerified;
	}



	public void setRowsVerified(int rowsVerified) {
		this.rowsVerified = rowsVerified;
	}



	public ArrayList<Exception> getExceptions() {
		return exceptions;
	}



	public void addException(Exception ex) {
		this.exceptions.add(ex);
	}



	public void merge(TableImportResult result){
		if (result.tableCreateResult > 0) this.tableCreateResult = result.tableCreateResult;
		if (result.tableConstraintResult > 0) this.tableConstraintResult = result.tableConstraintResult;
		if (result.dataImportResult > 0) this.dataImportResult = result.dataImportResult;
		if (result.rowsReceived > -1) this.rowsReceived = result.rowsReceived;
		if (result.rowsImported > -1) this.rowsImported = result.rowsImported;
		if (result.rowsVerified > -1) this.rowsVerified = result.rowsVerified;
		if (result.constraintsAdded > 0) this.constraintsAdded = result.constraintsAdded;
		exceptions.addAll(result.getExceptions());
	}
	
	public String toString(){
		String out = "";
		
		out += "Table Created: " + getResultString(this.tableCreateResult);
		out += "\nConstraints Added: " + this.getConstraintsAdded() + " " + getResultString(this.tableConstraintResult);
		out += "\nData Imported: " + getResultString(this.dataImportResult); 
		out += "\nRows Received: " + this.rowsReceived;
		out += "\nRows Imported: " + this.rowsImported;
		out += "\nRows Verified: " + this.rowsVerified;
		out += "\nExceptions: " + this.exceptions.size();
		
		return out;
	}
	
	private String getResultString(int status){
		if (status == FAILED) return "failed";
		if (status == SUCCESSFUL) return "successful";
		if (status == PARTIAL) return "partial success";
		return "unknown";
	}



	public int getConstraintsAdded() {
		return constraintsAdded;
	}



	public void setConstraintsAdded(int constraintsAdded) {
		this.constraintsAdded = constraintsAdded;
	}

}
