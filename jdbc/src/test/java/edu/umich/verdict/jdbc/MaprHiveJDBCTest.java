/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.jdbc;

import static org.junit.Assert.*;

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
