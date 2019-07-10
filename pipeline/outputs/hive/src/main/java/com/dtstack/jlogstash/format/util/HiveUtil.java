package com.dtstack.jlogstash.format.util;

import java.sql.Connection;
import java.util.*;
import java.util.regex.Pattern;

import com.dtstack.jlogstash.format.StoreEnum;
import org.apache.commons.collections.MapUtils;
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

    private final static String TABLE_COLUMN_KEY = "key";
    private final static String TABLE_COLUMN_TYPE = "type";

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

    public void createTableForPath(String tablePath, String createSql) {
        Connection connection = null;
        try {
            connection = DBUtil.getConnection(jdbcUrl, username, password);
            String sql = String.format(createSql, tablePath);
            DBUtil.executeSqlWithoutResultSet(connection, sql);
        } catch (Exception e) {
            logger.error("", e);
            throw new RuntimeException(e);
        } finally {
            DBUtil.closeDBResources(null, null, connection);
        }
    }

//    private void getTargetSyncInfo(String jdbcUrl, String username, String password, String tableName) {
//        Connection connection = null;
//        try {
//            connection = DBUtil.getConnection(jdbcUrl, username, password);
//            List<Map<String, Object>> result = DBUtil.executeQuery(connection, "desc extended " + tableName);
//            Iterator<Map<String, Object>> iter = result.iterator();
//            String colName;
//            String detail;
//            while (iter.hasNext()) {
//                Map<String, Object> row = iter.next();
//                colName = (String) row.get("col_name");
//                detail = (String) row.get("data_type");
//                if (colName.equals("# Detailed Table Information")) {
//                    if (detail != null) {
//                        detail = detail.replaceAll("\n", " ");
//                        Matcher matcher = PATTERN.matcher(detail);
//                        if (matcher.find()) {
//                            writer.setPath(matcher.group(1));
//                            if (matcher.group(3).contains(TEXT_FORMAT)) {
//                                writer.setFileType(FileType.TEXTFILE.getVal());
//                                Matcher delimiterMatcher = DELIMITER_PATTERN.matcher(matcher.group(5));
//                                if (delimiterMatcher.find()) {
//                                    writer.setFieldDelimiter(delimiterMatcher.group(1));
//                                }
//                            } else if (matcher.group(3).contains(ORC_FORMAT)) {
//                                writer.setFileType(FileType.ORCFILE.getVal());
//                            } else if (matcher.group(3).contains(PARQUET_FORMAT)) {
//                                writer.setFileType(FileType.PARQUET.getVal());
//                            } else {
//                                throw new RuntimeException("Unsupported fileType:" + matcher.group(3));
//                            }
//                        }
//                    }
//                    break;
//                }
//            }
//            return writer;
//        } catch (Exception e) {
//            logger.error("", e);
//            if (e.getMessage().contains(NoSuchTableException)) {
//                throw new RuntimeException(String.format("表%s不存在", tableName));
//            } else {
//                throw e;
//            }
//        } finally {
//            DBUtil.closeDBResources(null, null, connection);
//        }
//    }


    public static String getCreateTableHql(List<Map<String, Object>> tablesColumn, String delimiter, String store) {
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
