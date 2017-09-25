package edu.umich.verdict.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class MaprHiveJDBCTest {

    @Test
    public void test() throws ClassNotFoundException, SQLException {
        Class.forName("edu.umich.verdict.jdbc.Driver");
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String url = "jdbc:hive2://bfd-main-relay00.sv.walmartlabs.com:10000/default;user=abcd;password=masked;ssl=true;sslTrustStore=/home/gaddepa/trust-bfd-main.jks;trustStorePassword=abcde";
        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
    }

    @Test
    public void test2() throws ClassNotFoundException, SQLException {
        Class.forName("edu.umich.verdict.jdbc.Driver");
        String url = "jdbc:verdict:hive2://bfd-main-relay00.sv.walmartlabs.com:10000/default;user=abcd;password=masked;ssl=true;sslTrustStore=/home/gaddepa/trust-bfd-main.jks;trustStorePassword=abcde";
        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
    }

}
