package edu.umich.verdict.jdbc;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.VerdictException;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.processing.ParsedStatement;
import edu.umich.verdict.processing.SelectStatement;
import edu.umich.verdict.processing.VerdictStatement;
import edu.umich.verdict.transformation.Parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VConnection implements Connection {
    private final DbConnector connector;
    private final Connection innerConnection;
    private final Configuration conf;

    public VConnection(String url, Properties info) throws SQLException {
        // jdbc:verdict:dbms://host:port[/schema][?config=/path/to/conf]
        Pattern p = Pattern.compile("^jdbc:verdict:(?<dbms>\\w+)://(?<host>[\\.a-zA-Z0-9]+):(?<port>\\d+)(?<schema>/(\\w+))?(\\?config=(?<config>.*))?$");
        try {
            Matcher m = p.matcher(url);
            if (!m.find())
                throw new SQLException("Invalid URL.");
            if (info.contains("config"))
                conf = new Configuration(new File(info.getProperty("config")));
            else if (m.group("config")!=null)
                conf = new Configuration(new File(m.group("config")));
            else
                conf = new Configuration(info);
            String dbms = m.group("dbms");
            if (info.contains("user"))
                conf.set(dbms + ".user", info.getProperty("user", null));
            if (info.contains("password"))
                conf.set(dbms + ".password", info.getProperty("password", null));
            conf.set("dbms", dbms);
            conf.set(dbms + ".host", m.group("host"));
            conf.set(dbms + ".port", m.group("port"));
            conf.set(dbms + ".schema", m.group("schema"));
            connector = DbConnector.createConnector(conf);
            innerConnection = connector.getConnection();
        } catch (VerdictException e) {
            throw e.toSQLException();
        } catch (FileNotFoundException e) {
            throw new SQLException("Config file '" + info.getProperty("config") + "' not found.", e);
        }
    }

    DbConnector getConnector() {
        return connector;
    }

    ParsedStatement getParsed(String sql) throws SQLException {
        try {
            return Parser.parse(sql);
        } catch (Exception e) {
            throw new SQLException("Parse Error: " + e.getMessage(), e);
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new VStatement(innerConnection.createStatement(), this, conf);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        ParsedStatement q = getParsed(sql);
        if (q instanceof SelectStatement)
            return innerConnection.prepareStatement(((SelectStatement) q).getTransformed(conf, connector));
        else if (q instanceof VerdictStatement)
            throw new SQLException("At the moment, this statement is only processable using createStatement() method.");
        else
            return innerConnection.prepareStatement(q.toString());
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        ParsedStatement q = getParsed(sql);
        if (q instanceof SelectStatement)
            return innerConnection.prepareCall(((SelectStatement) q).getTransformed(conf, connector));
        else if (q instanceof VerdictStatement)
            throw new SQLException("At the moment, this statement is only processable using createStatement() method.");
        else
            return innerConnection.prepareCall(q.toString());
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        ParsedStatement q = getParsed(sql);
        return q.toString();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        innerConnection.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return innerConnection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        innerConnection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        innerConnection.rollback();
    }

    @Override
    public void close() throws SQLException {
        connector.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return connector.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return innerConnection.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        innerConnection.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return innerConnection.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        innerConnection.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return innerConnection.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        innerConnection.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return innerConnection.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return innerConnection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        innerConnection.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new VStatement(innerConnection.createStatement(resultSetType, resultSetConcurrency), this, conf);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        ParsedStatement q = getParsed(sql);
        if (q instanceof SelectStatement)
            return innerConnection.prepareStatement(((SelectStatement) q).getTransformed(conf, connector), resultSetType, resultSetConcurrency);
        else if (q instanceof VerdictStatement)
            throw new SQLException("At the moment, this statement is only processable using createStatement() method.");
        else
            return innerConnection.prepareStatement(q.toString(), resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        ParsedStatement q = getParsed(sql);
        if (q instanceof SelectStatement)
            return innerConnection.prepareCall(((SelectStatement) q).getTransformed(conf, connector), resultSetType, resultSetConcurrency);
        else if (q instanceof VerdictStatement)
            throw new SQLException("At the moment, this statement is only processable using createStatement() method.");
        else
            return innerConnection.prepareCall(q.toString(), resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return innerConnection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        innerConnection.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        innerConnection.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return innerConnection.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return innerConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        innerConnection.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        innerConnection.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new VStatement(innerConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability), this, conf);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        ParsedStatement q = getParsed(sql);
        if (q instanceof SelectStatement)
            return innerConnection.prepareStatement(((SelectStatement) q).getTransformed(conf, connector), resultSetType, resultSetConcurrency, resultSetHoldability);
        else if (q instanceof VerdictStatement)
            throw new SQLException("At the moment, this statement is only processable using createStatement() method.");
        else
            return innerConnection.prepareStatement(q.toString(), resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        ParsedStatement q = getParsed(sql);
        if (q instanceof SelectStatement)
            return innerConnection.prepareCall(((SelectStatement) q).getTransformed(conf, connector), resultSetType, resultSetConcurrency, resultSetHoldability);
        else if (q instanceof VerdictStatement)
            throw new SQLException("At the moment, this statement is only processable using createStatement() method.");
        else
            return innerConnection.prepareCall(q.toString(), resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        ParsedStatement q = getParsed(sql);
        if (q instanceof SelectStatement)
            return innerConnection.prepareStatement(((SelectStatement) q).getTransformed(conf, connector), autoGeneratedKeys);
        else if (q instanceof VerdictStatement)
            throw new SQLException("At the moment, this statement is only processable using createStatement() method.");
        else
            return innerConnection.prepareStatement(q.toString(), autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        ParsedStatement q = getParsed(sql);
        if (q instanceof SelectStatement)
            return innerConnection.prepareStatement(((SelectStatement) q).getTransformed(conf, connector), columnIndexes);
        else if (q instanceof VerdictStatement)
            throw new SQLException("At the moment, this statement is only processable using createStatement() method.");
        else
            return innerConnection.prepareStatement(q.toString(), columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        ParsedStatement q = getParsed(sql);
        if (q instanceof SelectStatement)
            return innerConnection.prepareStatement(((SelectStatement) q).getTransformed(conf, connector), columnNames);
        else if (q instanceof VerdictStatement)
            throw new SQLException("At the moment, this statement is only processable using createStatement() method.");
        else
            return innerConnection.prepareStatement(q.toString(), columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        return innerConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return innerConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return innerConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return innerConnection.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return innerConnection.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        innerConnection.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        innerConnection.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return innerConnection.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return innerConnection.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return innerConnection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return innerConnection.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        innerConnection.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return innerConnection.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        innerConnection.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        innerConnection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return innerConnection.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return innerConnection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return innerConnection.isWrapperFor(iface);
    }
}
