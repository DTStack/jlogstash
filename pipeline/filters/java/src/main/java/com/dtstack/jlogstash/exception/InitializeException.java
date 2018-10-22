package com.dtstack.jlogstash.exception;

/**
 * @author zxb
 * @version 1.0.0
 *          2017年03月23日 16:02
 * @since Jdk1.6
 */
public class InitializeException extends RuntimeException {
    public InitializeException() {
        super();
    }

    public InitializeException(String message) {
        super(message);
    }

    public InitializeException(String message, Throwable cause) {
        super(message, cause);
    }

    public InitializeException(Throwable cause) {
        super(cause);
    }
}
