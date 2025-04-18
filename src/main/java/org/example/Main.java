package org.example;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.

import org.apache.hive.jdbc.HiveDriver;

import java.sql.*;
import java.util.Properties;

public class Main {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private static String sparkJdbcUrl = "jdbc:hive2://127.0.0.1:9090/default?user=maishuguang;password=123456;region=beijing";

//    public static void main(String[] args) throws SQLException  {
//        Driver driver = new HiveDriver();
//        // 构造 Properties 对象
//        Properties info = new Properties();
//        try (Connection conn = driver.connect(sparkJdbcUrl, info)) {                // OpenSession
//            try (Statement stmt = conn.createStatement()) {
//                try (ResultSet rs = stmt.executeQuery("show databases")) {    // ExecuteStatement
//                    while (rs.next()) {                                       // FetchResults
//                        System.out.println(rs.getString(1));
//                    }
//                }
//            }
//        }                                                              // CloseSession (AutoClosable)
//    }
    public static void main(String[] args) {
        try {
            // 注册 JDBC 驱动
            Class.forName(driverName);

            // 连接 HiveServer2
            String url = "jdbc:hive2://tjm-test416.suanchang-test.com:9090/default;ssl=true?warehouseId=aaa"; // 根据实际情况修改
            String user = ""; // Hive 用户名（通常为 hive 或空字符串）
            String password = ""; // Hive 密码（如果没有可以留空）

            Connection conn = DriverManager.getConnection(url, user, password);
            Statement stmt = conn.createStatement();

            // 创建数据库（如果不存在）
            stmt.execute("CREATE DATABASE IF NOT EXISTS testdb");

            // 切换数据库
            stmt.execute("USE testdb");

            // 创建表（如果不存在）
            String createTable = "CREATE TABLE IF NOT EXISTS test_table (id INT, name STRING)";
            stmt.execute(createTable);

            // 插入数据（仅适用于 Hive 支持 INSERT 的版本）
            stmt.execute("INSERT INTO TABLE test_table VALUES (1, 'Alice'), (2, 'Bob')");

            // 查询数据
            ResultSet res = stmt.executeQuery("SELECT * FROM test_table");
            while (res.next()) {
                System.out.println("ID: " + res.getInt(1) + ", Name: " + res.getString(2));
            }

            // 关闭连接
            res.close();
            stmt.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}