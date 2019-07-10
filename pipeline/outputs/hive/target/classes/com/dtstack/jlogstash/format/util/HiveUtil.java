package com.dtstack.jlogstash.format.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: haisi
 * @date 2019-07-02 17:47
 */
public class HiveUtil {

    private static Logger logger = LoggerFactory.getLogger(HiveUtil.class);

    private String tablesColumn;

    private Connection conn;
    private String analyticalRules;
    private String store;
    private String delimiter;

    /**
     * 抛出异常,直接终止hive
     */
    public HiveUtil(String driver,String url,String user,String password,String analyticalRules,String tablesColumn,String store,String delimiter) throws SQLException, ClassNotFoundException {
        Class.forName(driver);
        this.conn = DriverManager.getConnection(url, user, password);
        this.analyticalRules = analyticalRules;
        this.tablesColumn=tablesColumn;
        this.store=store;
        this.delimiter=delimiter;
    }

    public void run(String tablesColumn,Map output){
        try {
            Statement stmt = this.conn.createStatement();
            try {
                Map tables = getStructure(tablesColumn);
//                String sql = createHql(tables, output, analyticalRules);
//                stmt.execute(sql);
                List<String> hqls =createHqls(tables,output,analyticalRules);
                for (String hql:hqls){
                    try{
                        stmt.execute(hql);
                    }catch (SQLException e){
                        logger.error("",e);
                    }
                }
                stmt.close();
            } catch (Exception e) {
                logger.error("", e);
            }
        } catch (SQLException e){
            logger.error("",e);
        }
    }

    /**
     * Json字符串转换为Json->Map对象
     */
    public static Map getStructure(String jsonStr){
        try{
            Map map = (Map) JSON.parse(jsonStr);
            return map;
        } catch (Exception e){
            logger.error("",e);
        }
        return null;
    }

    /**
     * 建表HQL
     */
    public List<String> createHqls(Map tablesColumn, Map output, String analyticalRules){
        List<String> res=new ArrayList<>();
        try{
            String s="";
            for (Object key:tablesColumn.keySet()){
                String tableName = (String) key;
                tableName =HiveConverter.regaxByRules(output,analyticalRules)+tableName;
                Object tableFields = tablesColumn.get(key);
                if (!(tableFields instanceof List)){
                    throw new Exception("TypeError:tablesColumn->tableFields");
                }
                for (Object field:(List)tableFields){
                    if (!(field instanceof Map)){
                        throw new Exception("TypeError:tablesColumn->field");
                    }
                    String type = (String) ((Map) field).get("type");
                    type=checkType(type);
                    s += String.format(",`%s` %s",((Map) field).get("key"),type);

                }
                s=s.substring(1,s.length());
                if ("orc".equals(this.store)){
                    res.add(String.format("CREATE TABLE IF NOT EXISTS %s (%s) stored as orcfile",tableName,s));
                } else if ("text".equals(this.store)){
                    res.add(String.format("CREATE TABLE IF NOT EXISTS %s (%s) row format delimited fields terminated by '%s' lines terminated by '\\n' stored as textfile",tableName,s,delimiter));
                } else {
                    res.add(String.format("CREATE TABLE IF NOT EXISTS %s (%s)  stored as orcfile",tableName,s));
                }
                s="";
            }
            return res;
        }catch (Exception e){
            logger.error("",e);
        }
        return res;
    }
    
    
    public String createHql(Map tablesColumn, Map output, String analyticalRules){
        String res = "";
        try{
            String s="";
            for (Object key:tablesColumn.keySet()){
                String tableName = (String) key;
                tableName =HiveConverter.regaxByRules(output,analyticalRules)+tableName;
                Object tableFields = tablesColumn.get(key);
                if (!(tableFields instanceof List)){
                    throw new Exception("TypeError:tablesColumn->tableFields");
                }
                for (Object field:(List)tableFields){
                    if (!(field instanceof Map)){
                        throw new Exception("TypeError:tablesColumn->field");
                    }
                    String type = (String) ((Map) field).get("type");
                    type=checkType(type);
                    s += String.format("`%s` %s,",((Map) field).get("key"),type);

                }
                s=s.substring(0,s.length()-1);
                if ("orc".equals(this.store)){
                    res +=String.format("CREATE TABLE IF NOT EXISTS %s (%s) stored as orcfile\073",tableName,s);
                } else if ("text".equals(this.store)){
                    res +=String.format("CREATE TABLE IF NOT EXISTS %s (%s) row format delimited fields terminated by '%s' lines terminated by '\n' stored as textfile\073",tableName,s,delimiter);
                } else {
                    res +=String.format("CREATE TABLE IF NOT EXISTS %s (%s)  stored as orcfile\073",tableName,s);
                }
            }
            return res;
        }catch (Exception e){
            logger.error("",e);
        }
        return res;
    }

    private static String checkType(String type){
        switch(type.toUpperCase()) {
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
