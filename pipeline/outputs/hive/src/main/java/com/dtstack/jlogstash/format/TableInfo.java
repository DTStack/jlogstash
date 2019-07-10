package com.dtstack.jlogstash.format;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: ggg1235
 * @date 2019-07-10 10:38
 */
public class TableInfo {

    private String database;
    private List<String> columns;
    private List<String> columnTypes;
    private String createTableSql;
    private String tableName;
    private String tablePath;
    private String path;
    private final static String PATH_TEMPLATE = "/user/hive/warehouse/%s.db/%s";

    public TableInfo(int columnSize) {
        columns = new ArrayList<>(columnSize);
        columnTypes = new ArrayList<>(columnSize);
    }

    public void addColumnAndType(String columnName, String columnType) {
        columns.add(columnName);
        columnTypes.add(columnType);
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
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

    public String getCreateTableSql() {
        return createTableSql;
    }

    public void setCreateTableSql(String createTableSql) {
        this.createTableSql = createTableSql;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTablePath() {
        return tablePath;
    }

    public void setTablePath(String tablePath) {
        this.tablePath = tablePath;
    }

    public String getPath() {
        if (tableName == null) {
            throw new RuntimeException("tableName must be not null");
        }
        if (path == null) {
            return String.format(PATH_TEMPLATE, database, tablePath);
        }
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
