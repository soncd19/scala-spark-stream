package com.msb.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;

public class StringUtils {
    private static final Logger logger = LoggerFactory.getLogger(StringUtils.class);

    public static String convertclobToString(java.sql.Clob data) throws Exception {
        final StringBuilder sb = new StringBuilder();
        if (data == null) {
            return sb.append("{}").toString();
        }

        try {
            final Reader reader = data.getCharacterStream();
            final BufferedReader br = new BufferedReader(reader);

            int b;
            while (-1 != (b = br.read())) {
                sb.append((char) b);
            }

            br.close();
        } catch (SQLException e) {
            logger.info("SQL. Could not convert CLOB to string" + e);
            return "";
        } catch (IOException e) {

            logger.info("IO. Could not convert CLOB to string" + e);
            return "";
        }

        return sb.toString();
    }
}
