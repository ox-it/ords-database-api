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


package uk.ac.ox.it.ords.api.database.data;


import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.queries.QueryRunner;


public class TableData implements Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = -4799016850834809340L;

	public enum DataTypesSupported {
        ACCESS, DB_DUMP, CSV
    }

    private Logger log = LoggerFactory.getLogger(TableData.class);
    public String tableName;
    public List<String> primaryKeys = new ArrayList<String>();
    public Map<String, OrdsTableColumn> columns = new ConcurrentHashMap<String, OrdsTableColumn>();
    //public List<OrdsTableColumn> columns = new ArrayList<OrdsTableColumn>();
    public Map<Integer, DataRow> rows = new ConcurrentHashMap<Integer, DataRow>();
    //public List<DataRow>rows = new ArrayList<DataRow>
    public List<String> sequences = new ArrayList<String>();
    public String comment;
    private int numberOfRowsInEntireTable = 0;
    private int currentRow = 0;
    private boolean showTableDumps = false;
    private DataTypesSupported originalDbType;

    public TableData() {
    }

    public List<OrdsTableColumn> getColumnsByIndex() {
        if (log.isDebugEnabled()) {
            log.debug("getColumnsByIndex");
            log.debug(String.format("Looping through %d column entries", columns.size()));
        }

        List<OrdsTableColumn> result = new ArrayList<OrdsTableColumn>();
        for (int index = 1; index <= columns.size(); index++) {
            for (OrdsTableColumn c : columns.values()) {
                if (c.orderIndex == index) {
                    result.add(c);
                    break;
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("getColumnsByIndex:return");
            log.debug(String.format("Returning %d column entries", result.size()));
        }
        return result;
    }

    public TableData(TableData another) {
        this.tableName = another.tableName;
        this.primaryKeys = another.primaryKeys;
        this.columns = another.columns;
        this.rows = another.rows;
        this.numberOfRowsInEntireTable = another.numberOfRowsInEntireTable;
        this.currentRow = another.currentRow;
        this.comment = another.comment;
    }

    /**
     * Used when table reference data is to be overlayed with the current data.
     *
     * @param dbName The containing database
     * @param tableNameToBeOverlayed the name of the table to be overlayed
     * @param colToBeChanged the name of the local column whose contents are to
     * be overwritten
     * @param changedRefCol the name of the column in the 'table to be
     * overlayed' whose contents are to be written here
     * @param projectId
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Deprecated
    public void merge(String dbName, String tableNameToBeOverlayed, String colToBeChanged, String changedRefCol, int projectId, String user, String password) {
        if ((colToBeChanged == null) || (changedRefCol == null) || (tableNameToBeOverlayed == null)) {
            log.debug("No changes here");
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug(String.format("About to merge data (%s, %s) from table %s to table %s", colToBeChanged, changedRefCol,
                tableNameToBeOverlayed, tableName));
        }

        // First check each column to see if the referenced column in it is different
        //for (OrdsTableColumn otc : columns.values()) {
        OrdsTableColumn otc = columns.get(colToBeChanged);
        if (otc.referencedColumnIndex == null) {
            otc.referencedColumnIndex = otc.referencedColumn;
        }

        /*
         * Check is we have an existance of the referenced column in the data to be merged
         */
        String referencedTable = otc.referencedTable;
        if (referencedTable.equals(tableNameToBeOverlayed)) {
            log.debug("The tables are the same - processing");

            /*
             * Quick sanity check
             */
            boolean found = false;
            for (String column : otc.alternateColumns) {
                if (column.equals(changedRefCol)) {
                    found = true;
                    break;
                }
            }
            if (found) {
                otc.referencedColumn = changedRefCol;
                try {
                    /*
                     * Now we have the changed ref column we need to populate the relevant data
                     */
                    for (DataRow localDataRow : rows.values()) {
                        DataCell localCell = localDataRow.cell.get(colToBeChanged);
                        DataCell targetCell = null;
                        //DataCell replacementCell = null;
                        // Look through the rows of the external data
                        String queryToRun = String.format("Select \"%s\" from \"%s\" where \"%s\" = '%s'", changedRefCol, tableNameToBeOverlayed, otc.referencedColumnIndex, localCell.getValue());

                        QueryRunner qr = new QueryRunner(null, dbName, user, password);
                        qr.runDBQuery(queryToRun);
                        TableData queriedData = qr.getTableData();
                        if (queriedData == null) {
                            log.error("Null queried data");
                        }
                        else {
                            if (queriedData.rows.size() != 1) {
                                log.error("Suspect row count in queried data " + queriedData.rows.size());
                            }
                            for (DataRow dr2 : queriedData.rows.values()) {
                                if (dr2.cell.size() != 1) {
                                    log.error("Suspect cell count in queried data " + dr2.cell.size());
                                }
                                targetCell = dr2.cell.get(changedRefCol);
                            }
                        }
                        if (targetCell == null) {
                            log.error("Unable to find target data.");
                        }
                        else {
                            localCell.setValue(targetCell.getValue());
                            localCell.setDefaultValue(targetCell.getDefaultValue());
                            localCell.setType(targetCell.getType());
                            localDataRow.cell.put(colToBeChanged, localCell);
                        }
                    }
                }
                catch (SQLException e) {
                    log.error("Unable to comply - exception ", e);
                }
                catch (ClassNotFoundException e) {
                    log.error("Unable to comply - exception ", e);
                }
            }
            else {
                log.error(String.format("Internal data integrity - cannot find column %s in local table %s", changedRefCol, tableName));
            }
        }
        else {
            log.error(String.format("Likely coding error. Input table name of %s is not the same as local table %s", otc.referencedTable, tableName));
        }
    }

    /**
     * Used when table reference data is to be overlayed with the current data
     *
     * @param freshData
     */
    @Deprecated
    public void merge2(TableData freshData, String colToBeChanged, String changedRefCol) {
        if ((colToBeChanged == null) || (changedRefCol == null) || (freshData == null)) {
            log.debug("No changes here");
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug(String.format("About to merge data (%s, %s) from table %s to table %s", colToBeChanged, changedRefCol,
                freshData.tableName, tableName));
        }

        // First check each column to see if the referenced column in it is different
        //for (OrdsTableColumn otc : columns.values()) {
        OrdsTableColumn otc = columns.get(colToBeChanged);
        if (otc.referencedColumnIndex == null) {
            otc.referencedColumnIndex = otc.referencedColumn;
        }

        /*
         * Check is we have an existance of the referenced column in the data to be merged
         */
        String referencedTable = otc.referencedTable;
        if (referencedTable.equals(freshData.tableName)) {
            log.debug("The tables are the same - processing");

            /*
             * Quick sanity check
             */
            boolean found = false;
            for (String column : otc.alternateColumns) {
                if (column.equals(changedRefCol)) {
                    found = true;
                    break;
                }
            }
            if (found) {
                otc.referencedColumn = changedRefCol;

                /*
                 * Now we have the changed ref column we need to populate the relevant data
                 */
                for (DataRow localDataRow : rows.values()) {
                    DataCell localCell = localDataRow.cell.get(colToBeChanged);
                    DataCell targetCell;
                    DataCell replacementCell = null;
                    found = false;
                    // Look through the rows of the external data
                    for (DataRow dr2 : freshData.rows.values()) {
                        targetCell = dr2.cell.get(otc.referencedColumnIndex);
                        if (targetCell.getValue().equals(localCell.getValue())) {
                            found = true;
                            replacementCell = dr2.cell.get(changedRefCol);
                            break;
                        }
                    }
                    if (found && (replacementCell != null)) {
                        localCell.setValue(replacementCell.getValue());
                        localCell.setDefaultValue(replacementCell.getDefaultValue());
                        localCell.setType(replacementCell.getType());
                        localDataRow.cell.put(colToBeChanged, localCell);
                    }
                    else {
                        log.error("Unable to find target data. Found = " + found);
                    }
                }
            }
            else {
                log.error(String.format("Internal data integrity - cannot find column %s in local table %s", changedRefCol, tableName));
            }
        }
        else {
            log.error(String.format("Likely coding error. Input table name of %s is not the same as local table %s", otc.referencedTable, tableName));
        }
    }

    public void logData() {
        logData(false);
    }

    /**
     * A logging function to view the contents of the class. Useful for
     * debugging purposes.
     */
    public void logData(boolean pageData) {
        if (!showTableDumps) {
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Table name: %s", tableName));
            log.debug(String.format("Primary keys ..."));
            for (String s : primaryKeys) {
                log.debug(String.format("\tKey: %s", s));
            }
            log.debug(String.format("Columns ..."));
            for (OrdsTableColumn otc : columns.values()) {
                //OrdsTableColumn otc = columns.get(counter);
                log.debug(String.format("\tColumn: %s", otc.columnName));
                log.debug(String.format("\t\tColumn type is ", otc.columnType.toString()));
                log.debug(String.format("\t\tReferenced table is ", otc.referencedTable));
                log.debug(String.format("\t\tReferenced column is ", otc.referencedColumn));
                if ((otc.alternateColumns != null) && (!otc.alternateColumns.isEmpty())) {
                    for (String s : otc.alternateColumns) {
                        log.debug(String.format("\t\t\tAlternative column: ", s));
                    }
                }
                if ((otc.alternativeOptions != null) && (!otc.alternativeOptions.isEmpty())) {
                    for (String s : otc.alternativeOptions) {
                        log.debug(String.format("\t\t\tAlternative option: ", s));
                    }
                }
            }

            log.debug(String.format("Rows ..."));

            if (rows != null) {
                for (int counter = 0; counter < rows.size(); counter++) {
                    log.debug(String.format("\tnew row"));
                    for (String s : rows.get(counter).cell.keySet()) {
                        //for (int counter2 = 0; counter2 < rows.get(counter).cell.size(); counter2++) {
                        log.debug(String.format("\t\t%s", rows.get(counter).cell.get(s).getValue()));
                    }
                }
            }
            else {
                log.debug("No row data here");
            }
        }
    }

    public int getNumberOfRowsInEntireTable() {
        return numberOfRowsInEntireTable;
    }

    public void setNumberOfRowsInEntireTable(int i) {
        numberOfRowsInEntireTable = i;
    }

    public int getCurrentRow() {
        if (currentRow == 0) {
            currentRow = 1;
        }
        return currentRow;
    }

    public void setCurrentRow(int i) {
        currentRow = i;
    }

    public DataTypesSupported getOriginalDbType() {
        return originalDbType;
    }

    public void setOriginalDbType(DataTypesSupported originalDbType) {
        if (originalDbType == null) {
            return;
        }
        this.originalDbType = originalDbType;
    }

    public void setOriginalDbType(String originalDbType) {
        if (originalDbType == null) {
            return;
        }
        if (DataTypesSupported.ACCESS.toString().equals(originalDbType)) {
            this.originalDbType = DataTypesSupported.ACCESS;
        }
        else if (DataTypesSupported.CSV.toString().equals(originalDbType)) {
            this.originalDbType = DataTypesSupported.CSV;
        }
        else if (DataTypesSupported.DB_DUMP.toString().equals(originalDbType)) {
            this.originalDbType = DataTypesSupported.DB_DUMP;
        }
    }
}
