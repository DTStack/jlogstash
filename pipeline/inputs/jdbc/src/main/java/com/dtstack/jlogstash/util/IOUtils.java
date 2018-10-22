package com.dtstack.jlogstash.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author zxb
 * @version 1.0.0
 *          2017年03月24日 22:10
 * @since Jdk1.6
 */
public class IOUtils {

    public static long copy(InputStream input, OutputStream output, byte[] buffer) throws IOException {
        long count;
        int n;
        for(count = 0L; -1 != (n = input.read(buffer)); count += (long)n) {
            output.write(buffer, 0, n);
        }

        return count;
    }

    public static void closeQuietly(Closeable... closeables) {
        if(closeables != null) {
            Closeable[] arr = closeables;
            int len = closeables.length;

            for(int i = 0; i < len; i++) {
                Closeable closeable = arr[i];
                closeQuietly(closeable);
            }
        }
    }

    public static void closeQuietly(Closeable closeable) {
        try {
            if(closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
        }
    }
}
