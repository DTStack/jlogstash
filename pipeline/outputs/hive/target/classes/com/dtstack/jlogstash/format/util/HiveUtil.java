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

    private static String driver = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:Hive2://djt11:10000";
    private static String user = "root";
    private static String password = "";
    private static String schema;
    private static String analyticalRules;
    private static String tablesColumn;
    private Connection conn;
    private Statement stmt;

    /**
     * 抛出异常,直接终止hive
     */
    public HiveUtil() throws SQLException, ClassNotFoundException {
        Class.forName(driver);
        this.conn = DriverManager.getConnection(url, user, password);
        this.stmt = this.conn.createStatement();
    }

    public void createDatabase(String schema) throws SQLException {
        String createHql=String.format("CREATE DATABASE IF NOT EXISTS %s;",schema);
        this.stmt.execute(createHql);
        String useHql = String.format("USE %s;",schema);
        this.stmt.execute(useHql);
    }

    public void  createTables(String hql){
        try{
            this.stmt.execute(hql);
        }catch (Exception e){
            logger.error("",e);
        }
    }


    public void run(String tablesColumn,String schema){
        try{
            Map tables = getStructure(tablesColumn);
            String sql = createHql(tables);
            this.createDatabase(schema);
            this.createTables(sql);
        }catch (Exception e){
            logger.error("",e);
        }
    }
    /**
     * Json字符串转换为Json<->Map的List对象
     */
    public static List<Map> getStructures(List<String> binlogList){
        try{
            List<Map> jsonList = new ArrayList<>();
            for (String s : binlogList) {
                try {
                    Map map = (Map) JSON.parse(s);
                    jsonList.add(map);
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
            return jsonList;
        }catch (Exception e){
            logger.error("",e);
        }
        return null;
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
     * 平铺Json
     */
    public static Map tileJSON(Map event){
        Map res = new HashMap(16);
        try {
            Object message = event.get("message");
            if (!(message instanceof Map)){
                throw new Exception("TypeError:event");
            }
            Map after = (Map)((Map) message).get("after");
            for(Object key:after.keySet()){
                res.put("after_"+key,after.get(key));
            }
            Map before = (Map) ((Map) message).get("before");
            for(Object key:before.keySet()){
                res.put("before_"+key,after.get(key));
            }
            for (Object key:((Map) message).keySet()){
                if ("after".equals(key) || "before".equals(key)){
                    continue;
                }
                res.put(key,((Map) message).get(key));
            }
            return res;
        } catch (Exception e){
            logger.error("",e);
        }
        return null;
    }

    /**
     * 建表HQL
     */
    public static String createHql(Map tablesColumn){
        String res = "";
        try{
            String s="";
            for (Object key:tablesColumn.keySet()){
                String tableName = (String) key;
                Object tableFields = tablesColumn.get(key);
                if (!(tableFields instanceof List)){
                    throw new Exception("TypeError:tablesColumn->tableFields");
                }
                for (Object field:(List)tableFields){
                    if (!(field instanceof Map)){
                        throw new Exception("TypeError:tablesColumn->field");
                    }
                    String type = (String) ((Map) field).get("type");
                    if ("varchar".equals(type.toLowerCase())){
                        type="string";
                    }
                    s += String.format("`%s` %s,",((Map) field).get("key"),type);

                }
                s=s.substring(0,s.length()-1);
                res +=String.format("CREATE TABLE IF NOT EXISTS %s (%s);",tableName,s);
            }
            System.out.println(res);
        }catch (Exception e){
            logger.error("",e);
        }
        return res;
    }



    public static void main(String[] args) {
        try {
            HiveUtil hiveUtil = new HiveUtil();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
//        String s="select * from where s=?";
//        System.out.println(s);
//        List<String> binlogLisgt = new ArrayList<>();
//        binlogLisgt.add("{data:[{item_id:1},{item_id:2}],t1:{t2:{t3:10086}}}");
//        binlogLisgt.add("{1:123,lists:[1,2,3]}");
//        List mapList = HiveUtil.getStructures(binlogLisgt);
        Map map = (Map) JSON.parse("{\n" +
                "              \"tablesColumn\":\n" +
                "          {\n" +
                "              \"date_test\":[{\"key\":\"id\",\"type\":\"Int\",\"comment\":\"\"},{\"key\":\"name\",\"type\":\"String\",\"comment\":\"\"},{\"key\":\"time\",\"type\":\"DataTime\",\"comment\":\"\"}]\n" +
                "          ,\n" +
                "      \n" +
                "          \"es_sink\":[{\"key\":\"id\",\"type\":\"Int\",\"comment\":\"\"},{\"key\":\"name\",\"type\":\"String\",\"comment\":\"\"},{\"key\":\"time\",\"type\":\"DataTime\",\"comment\":\"\"}]\n" +
                "          \n" +
                "      }\n" +
                "      \n" +
                "    }");
        Object tablesColumn = map.get("tablesColumn");
        if(!(tablesColumn instanceof Map)){
            System.out.println("Error");
        } else {
            String tables =  createHql((Map) tablesColumn);
        }
    }

}
