package com.dtstack.jlogstash.format.util;

import java.io.IOException;
import java.sql.Connection;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.dtstack.jlogstash.format.StoreEnum;
import com.dtstack.jlogstash.format.TableInfo;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hbase.TableExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: haisi
 * @date 2019-07-02 17:47
 */
public class HiveUtil {

    private static Logger logger = LoggerFactory.getLogger(HiveUtil.class);


    private static final String PATTERN_STR = "Storage\\(Location: (.*), InputFormat: (.*), OutputFormat: (.*) Serde: (.*) Properties: \\[(.*)\\]";
    private static final Pattern PATTERN = Pattern.compile(PATTERN_STR);

    private static final Pattern DELIMITER_PATTERN = Pattern.compile("field\\.delim=(.*), ");

    private static final String TEXT_FORMAT = "TextOutputFormat";
    private static final String ORC_FORMAT = "OrcOutputFormat";
    private static final String PARQUET_FORMAT = "MapredParquetOutputFormat";
    private static final String NoSuchTableException = "NoSuchTableException";
    private static final String TableExistException = "TableExistsException";
    private static final String TableAlreadyExistsException = "TableAlreadyExistsException";

    public final static String TABLE_COLUMN_KEY = "key";
    public final static String TABLE_COLUMN_TYPE = "type";

    private String jdbcUrl;
    private String username;
    private String password;

    /**
     * 抛出异常,直接终止hive
     */
    public HiveUtil(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public void createHiveTableWithTableInfo(TableInfo tableInfo) {
        Connection connection = null;
        try {
            connection = DBUtil.getConnection(jdbcUrl, username, password);
            createTable(connection, tableInfo);
            fillTableInfo(connection, tableInfo);
        } catch (Exception e) {
            logger.error("", e);
        } finally {
            DBUtil.closeDBResources(null, null, connection);
        }
    }

    /**
     * 表如果存在不要删除之前的表因为可能是用户的表，所以也不需要再创建，也不用 throw exception，暂时只有日志
     *
     * @param connection
     * @param tableInfo
     */
    private void createTable(Connection connection, TableInfo tableInfo) {
        String sql = String.format(tableInfo.getCreateTableSql(), tableInfo.getTablePath());
        try {
            DBUtil.executeSqlWithoutResultSet(connection, sql);
        } catch (Exception e) {
            logger.error("{}", e);
            if (!e.getMessage().contains(TableExistException) && !e.getMessage().contains(TableAlreadyExistsException)) {
                throw e;
            }
        }
    }

    private void fillTableInfo(Connection connection, TableInfo tableInfo) {
        try {
            List<Map<String, Object>> result = DBUtil.executeQuery(connection, "desc extended " + tableInfo.getTablePath());
            Iterator<Map<String, Object>> iter = result.iterator();
            String colName;
            String detail;
            while (iter.hasNext()) {
                Map<String, Object> row = iter.next();
                colName = (String) row.get("col_name");
                detail = (String) row.get("data_type");
                if (colName.equals("# Detailed Table Information")) {
                    if (detail != null) {
                        detail = detail.replaceAll("\n", " ");
                        Matcher matcher = PATTERN.matcher(detail);
                        if (matcher.find()) {
                            tableInfo.setPath(matcher.group(1));
                            if (matcher.group(3).contains(TEXT_FORMAT)) {
                                tableInfo.setStore(StoreEnum.TEXT.name());
                                Matcher delimiterMatcher = DELIMITER_PATTERN.matcher(matcher.group(5));
                                if (delimiterMatcher.find()) {
                                    tableInfo.setDelimiter(delimiterMatcher.group(1));
                                }
                            } else if (matcher.group(3).contains(ORC_FORMAT)) {
                                tableInfo.setStore(StoreEnum.ORC.name());
                            } else {
                                throw new RuntimeException("Unsupported fileType:" + matcher.group(3));
                            }
                        }
                    }
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("{}", e);
            if (e.getMessage().contains(NoSuchTableException)) {
                throw new RuntimeException(String.format("表%s不存在", tableInfo.getTablePath()));
            } else {
                throw e;
            }
        }
    }


    public static String getCreateTableHql(List<Map<String, Object>> tablesColumn, String delimiter, String store) {
        //不要使用create table if not exist，可能以后会在业务逻辑中判断表是否已经存在
        StringBuilder fieldsb = new StringBuilder("CREATE TABLE %s (");
        for (int i = 0; i < tablesColumn.size(); i++) {
            Map<String, Object> fieldColumn = tablesColumn.get(i);
            fieldsb.append(String.format("%s %s", MapUtils.getString(fieldColumn, TABLE_COLUMN_KEY), convertType(MapUtils.getString(fieldColumn, TABLE_COLUMN_TYPE))));
            if (i != tablesColumn.size() - 1) {
                fieldsb.append(",");
            }
        }
        if (StoreEnum.TEXT.name().equalsIgnoreCase(store)) {
            fieldsb.append(") ROW FORMAT DELIMITED FIELDS TERMINATED BY '");
            fieldsb.append(delimiter);
            fieldsb.append("' LINES TERMINATED BY '\\n' STORED AS TEXTFILE ");
        } else {
            fieldsb.append(") STORED AS ORC ");
        }
        return fieldsb.toString();
    }

    private static String convertType(String type) {
        switch (type.toUpperCase()) {
            case "TINYINT":
                type = "TINYINT";
                break;
            case "SMALLINT":
                type = "SMALLINT";
                break;
            case "INT":
                type = "INT";
                break;
            case "BIGINT":
                type = "BIGINT";
                break;
            case "FLOAT":
                type = "FLOAT";
                break;
            case "DOUBLE":
                type = "DOUBLE";
                break;
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                type = "STRING";
                break;
            case "BOOLEAN":
                type = "BOOLEAN";
                break;
            case "DATE":
            case "TIMESTAMP":
                type = "TIMESTAMP";
                break;
            default:
                type = "STRING";
        }
        return type;
    }
}
