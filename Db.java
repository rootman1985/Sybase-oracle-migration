package fr.migration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

final class Db {
    static Connection open(String url, String user, String password, String driver) throws Exception {
        Class.forName(driver);
        return DriverManager.getConnection(url, user, password);
    }
    static String q(String id) {
        if (id == null || !id.matches("[A-Za-z_][A-Za-z0-9_$#]*")) {
            throw new IllegalArgumentException("Identifiant SQL non sûr: " + id);
        }
        return id;
    }
    static String table(String schema, String table) {
        return q(schema) + "." + q(table);
    }
}