//package com.msb.call;
//
//import com.msb.utils.PropertiesFileReader;
//import com.zaxxer.hikari.HikariConfig;
//import com.zaxxer.hikari.HikariDataSource;
//import oracle.jdbc.internal.OracleCallableStatement;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.*;
//import java.sql.Clob;
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.sql.Types;
//import java.util.Properties;
//
//public class CallProduceV2 implements Serializable {
//
//    private static final HikariConfig config = new HikariConfig();
//    private static HikariDataSource ds;
//
//
//    public Connection getConnection() throws SQLException {
//        return ds.getConnection();
//    }
//
//    public void close() throws SQLException {
//        ds.getConnection().close();
//    }
//
//    public CallProduceV2(String configFile) {
//        Properties properties = PropertiesFileReader.readConfig(configFile);
//
//        config.setJdbcUrl(properties.getProperty("dataSource.uri"));
//        config.setUsername(properties.getProperty("dataSource.user"));
//        config.setPassword(properties.getProperty("dataSource.password"));
//        config.setDriverClassName("oracle.jdbc.driver.OracleDriver");
//        config.addDataSourceProperty("cachePrepStmts", "true");
//        config.addDataSourceProperty("prepStmtCacheSize", "250");
//        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
//        ds = new HikariDataSource(config);
//    }
//
//    private static final Logger logger = LoggerFactory.getLogger(CallProduceV2.class);
//
//    public String callProduce(String pAction, String pClob) throws Exception {
//
//        long l_err_level;
//
//        java.sql.Clob l_clob002;
//        String l_Final_Str = "";
//        String l_Sql = "begin PR_OUPUT_SERVICES(?,?,?); end;";
//
//        l_clob002 = null;
//        l_err_level = 0l;
//
//        try {
//            logger.info("performClob:Prepare Call at:");
//            OracleCallableStatement stmt = getConnection().prepareCall(l_Sql).unwrap(OracleCallableStatement.class);
//
//            stmt.setString(1, pAction);
//            setClobAsString(stmt, 2, pClob);
//            logger.info("performClob:After setClobAsString at:");
//            stmt.registerOutParameter(3, Types.CLOB);
//
//            logger.info("performClob:Begin execute at:");                                                            // CURSOR
//            stmt.execute();
//            logger.info("performClob:End execute at:");
//            l_clob002 = stmt.getClob(3);
//            logger.info("performClob:Got Clob at:");
//            if (stmt != null) {
//                stmt.close();
//                stmt = null;
//            }
//
//        } catch (SQLException e) {
//            logger.info("performClob:my Json error:" + e + "~ at level:" + l_err_level);
//        }
//        logger.info("performClob:Before convertclobToString at:");
//        String l_temp_str = convertclobToString(l_clob002);
//        logger.info("performClob:After convertclobToString at:");
//        l_Final_Str = l_temp_str;
//        return l_Final_Str;
//    }
//
//
//    public String convertclobToString(java.sql.Clob data) throws Exception {
//        final StringBuilder sb = new StringBuilder();
//
//        try {
//            final Reader reader = data.getCharacterStream();
//            final BufferedReader br = new BufferedReader(reader);
//
//            int b;
//            while (-1 != (b = br.read())) {
//                sb.append((char) b);
//            }
//
//            br.close();
//        } catch (SQLException e) {
//            logger.info("SQL. Could not convert CLOB to string" + e.toString());
//            return "";
//        } catch (IOException e) {
//
//            logger.info("IO. Could not convert CLOB to string" + e.toString());
//            return "";
//        }
//
//        return sb.toString();
//    }
//
//    public void setClobAsString(oracle.jdbc.OracleCallableStatement ps, int paramIndex, String content) throws SQLException {
//
//        if (content != null) {
//            ps.setClob(paramIndex, new StringReader(content), content.length());
//        } else {
//            ps.setClob(paramIndex, (Clob) null);
//        }
//
//    }
//}
