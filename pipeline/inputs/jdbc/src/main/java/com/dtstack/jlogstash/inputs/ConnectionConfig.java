package com.dtstack.jlogstash.inputs;

/**
 * @author zxb
 * @version 1.0.0
 *          2017年03月23日 15:47
 * @since Jdk1.6
 */
public class ConnectionConfig {

    private String jdbc_connection_string;

    private String jdbc_driver_class;

    private String jdbc_driver_library;

    private String jdbc_user;

    private String jdbc_password;

    public String getJdbc_connection_string() {
        return jdbc_connection_string;
    }

    public void setJdbc_connection_string(String jdbc_connection_string) {
        this.jdbc_connection_string = jdbc_connection_string;
    }

    public String getJdbc_driver_class() {
        return jdbc_driver_class;
    }

    public void setJdbc_driver_class(String jdbc_driver_class) {
        this.jdbc_driver_class = jdbc_driver_class;
    }

    public String getJdbc_driver_library() {
        return jdbc_driver_library;
    }

    public void setJdbc_driver_library(String jdbc_driver_library) {
        this.jdbc_driver_library = jdbc_driver_library;
    }

    public String getJdbc_user() {
        return jdbc_user;
    }

    public void setJdbc_user(String jdbc_user) {
        this.jdbc_user = jdbc_user;
    }

    public String getJdbc_password() {
        return jdbc_password;
    }

    public void setJdbc_password(String jdbc_password) {
        this.jdbc_password = jdbc_password;
    }

}
