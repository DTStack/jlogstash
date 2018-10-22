package com.dtstack.jlogstash.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * Blob、Clob转byte[]工具类
 * @author zxb
 * @version 1.0.0
 *          2017年03月24日 22:00
 * @since Jdk1.6
 */
public class BlobClobUtil {

    private static Logger logger = LoggerFactory.getLogger(BlobClobUtil.class);

    public static byte[] convertBlob2Bytes(Blob blob) {
        if(blob == null)
            return null;
        InputStream inputStream = null;
        ByteArrayOutputStream baos = null;
        try {
            inputStream = blob.getBinaryStream();
            baos = new ByteArrayOutputStream();
            IOUtils.copy(inputStream, baos, new byte[2048]);
            return baos.toByteArray();
        } catch (SQLException e) {
            logger.error("convertBlob2Bytes error!", e);
        } catch (IOException e) {
            logger.error("convertBlob2Bytes error!", e);
        } finally {
            IOUtils.closeQuietly(baos);
            IOUtils.closeQuietly(inputStream);
        }
        return null;
    }

    public static String convertClob2String(Clob clob) {
        if(clob == null)
            return null;

        Reader reader = null;
        try {
            reader = clob.getCharacterStream();
            int len = 2048;
            char[] charBuf = new char[len];

            StringWriter stringWriter = new StringWriter();
            try {
                while ((len = reader.read(charBuf)) != -1) {
                    stringWriter.write(charBuf, 0, len);
                }
                return stringWriter.toString();
            } catch (IOException e) {
                logger.error("convertClob2String", e);
            } finally {
                try {
                    stringWriter.close();
                } catch (IOException e) {
                    logger.error("close string writer error!", e);
                }
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        logger.error("close reader error!", e);
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("convertClob2String error!", e);
        }
        return null;
    }
}
