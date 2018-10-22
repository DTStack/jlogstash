package com.dtstack.jlogstash.inputs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.Properties;

/**
 * 关系型库Client工具类
 *
 * @author zxb
 * @version 1.0.0
 *          2016年12月24日 08:12
 * @since Jdk1.6
 */
public class JdbcClientHelper {

    private static final Logger logger = LoggerFactory.getLogger(JdbcClientHelper.class);

    /**
     * 获取连接
     *
     * @return
     */
    public static Connection getConnection(ConnectionConfig config) throws MalformedURLException, ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException {
        Thread.currentThread().setContextClassLoader(null);

        // 加载驱动包
        URL file_url = new URL("file:/" + config.getJdbc_driver_library());
        URLClassLoader loader = new URLClassLoader(new URL[]{file_url}, JdbcClientHelper.class.getClassLoader());

        // 用户名、密码
        Properties properties = new Properties();
        properties.put("user", config.getJdbc_user());
        properties.put("password", config.getJdbc_password());

        // 返回连接
        Class<?> driverClass = Class.forName(config.getJdbc_driver_class(), true, loader);
        Driver driver = (Driver) driverClass.newInstance();
        return driver.connect(config.getJdbc_connection_string(), properties);
    }


    /**
     * 释放数据库连接
     *
     * @param connection
     * @param preparedStatement
     * @param resultSet
     */
    public static void releaseConnection(Connection connection, Statement preparedStatement, ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                logger.error("close resultSet error!", e);
            }
        }
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                logger.error("close preparedStatement error!", e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error("close connection error!", e);
            }
        }
    }
}
