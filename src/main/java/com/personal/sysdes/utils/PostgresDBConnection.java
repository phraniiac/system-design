package com.personal.sysdes.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class PostgresDBConnection {

    public static Connection getDBConnection() {
        Connection c = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/mydb",
                            "postgres", "Welcome4$");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");
        return c;
    }
}
