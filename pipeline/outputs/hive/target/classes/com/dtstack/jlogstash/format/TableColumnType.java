package com.dtstack.jlogstash.format;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: ggg1235
 * @date 2019-07-10 10:38
 */
public class TableColumnType {
    private List<String> columns;
    private List<String> columnTypes;

    public TableColumnType(int columnSize) {
        columns = new ArrayList<>(columnSize);
        columnTypes = new ArrayList<>(columnSize);
    }

    public void addColumnAndType(String columnName, String columnType) {
        columns.add(columnName);
        columnTypes.add(columnType);
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<String> getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(List<String> columnTypes) {
        this.columnTypes = columnTypes;
    }

    @Override
    public String toString() {
        return "TableColumnType{" +
                "columns=" + columns +
                ", columnTypes=" + columnTypes +
                '}';
    }
}
