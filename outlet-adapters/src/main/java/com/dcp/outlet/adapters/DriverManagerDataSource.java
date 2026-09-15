package com.dcp.outlet.adapters;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

/** 极简 DataSource(仅取连接),用于适配层自检 / 无连接池场景。 */
public class DriverManagerDataSource implements DataSource {

    private final String url;
    private final String user;
    private final String password;

    public DriverManagerDataSource(String url, String user, String password) {
        this.url = url; this.user = user; this.password = password;
    }

    @Override public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    @Override public Connection getConnection(String u, String p) throws SQLException {
        return DriverManager.getConnection(url, u, p);
    }

    @Override public PrintWriter getLogWriter() { return null; }
    @Override public void setLogWriter(PrintWriter out) { }
    @Override public void setLoginTimeout(int seconds) { }
    @Override public int getLoginTimeout() { return 0; }
    @Override public Logger getParentLogger() { return Logger.getLogger("dcp"); }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException("unsupported"); }
    @Override public boolean isWrapperFor(Class<?> iface) { return false; }
}
