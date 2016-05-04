package edu.umich.verdict.jdbc;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.VerdictException;
import edu.umich.verdict.processing.ParsedStatement;
import edu.umich.verdict.processing.SelectStatement;
import edu.umich.verdict.processing.ShowSamplesStatement;
import edu.umich.verdict.processing.VerdictStatement;

import java.sql.*;
import java.util.ArrayList;

public class VStatement implements Statement {
    private final VConnection connection;
    private final Statement innerStatement;
    private final Configuration conf;
    private final ArrayList<String> batch = new ArrayList<>();

    public VStatement(Statement statement, VConnection connection, Configuration conf) throws SQLException {
        this.connection = connection;
        this.conf = conf;
        innerStatement = statement;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        ParsedStatement q = connection.getParsed(sql);
        if (q instanceof SelectStatement)
            return innerStatement.executeQuery(((SelectStatement) q).getTransformed(conf, connection.getConnector()));
        if (q instanceof ShowSamplesStatement)
            return innerStatement.executeQuery(((ShowSamplesStatement) q).getQuery(connection.getConnector()));
        else if (q instanceof VerdictStatement)
            try {
                return q.run(conf, connection.getConnector());
            } catch (VerdictException e) {
                throw e.toSQLException();
            }
        else
            return innerStatement.executeQuery(q.toString());
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return innerStatement.executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        innerStatement.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return innerStatement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        innerStatement.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return innerStatement.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        innerStatement.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        innerStatement.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return innerStatement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        innerStatement.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        innerStatement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return innerStatement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        innerStatement.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        innerStatement.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        ParsedStatement q = connection.getParsed(sql);
        if (q instanceof SelectStatement)
            return innerStatement.execute(((SelectStatement) q).getTransformed(conf, connection.getConnector()));
        if (q instanceof ShowSamplesStatement)
            return innerStatement.execute(((ShowSamplesStatement) q).getQuery(connection.getConnector()));
        else if (q instanceof VerdictStatement)
            try {
                q.run(conf, connection.getConnector());
                return false;
            } catch (VerdictException e) {
                throw e.toSQLException();
            }
        else
            return innerStatement.execute(q.toString());
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return innerStatement.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return innerStatement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return innerStatement.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        innerStatement.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return innerStatement.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        innerStatement.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return innerStatement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return innerStatement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return innerStatement.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        batch.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        batch.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        int[] results = new int[batch.size()];
        for (int i = 0; i <= batch.size(); i++) {
            execute(batch.get(i));
            results[i] = SUCCESS_NO_INFO;
        }
        return results;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return innerStatement.getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return innerStatement.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return innerStatement.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return innerStatement.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        ParsedStatement q = connection.getParsed(sql);
        if (q instanceof SelectStatement)
            return innerStatement.execute(((SelectStatement) q).getTransformed(conf, connection.getConnector()));
        if (q instanceof ShowSamplesStatement)
            return innerStatement.execute(((ShowSamplesStatement) q).getQuery(connection.getConnector()));
        else if (q instanceof VerdictStatement)
            try {
                q.run(conf, connection.getConnector());
                return false;
            } catch (VerdictException e) {
                throw e.toSQLException();
            }
        else
            return innerStatement.execute(q.toString(), autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        ParsedStatement q = connection.getParsed(sql);
        if (q instanceof SelectStatement)
            return innerStatement.execute(((SelectStatement) q).getTransformed(conf, connection.getConnector()));
        if (q instanceof ShowSamplesStatement)
            return innerStatement.execute(((ShowSamplesStatement) q).getQuery(connection.getConnector()));
        else if (q instanceof VerdictStatement)
            try {
                q.run(conf, connection.getConnector());
                return false;
            } catch (VerdictException e) {
                throw e.toSQLException();
            }
        else
            return innerStatement.execute(q.toString(), columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        ParsedStatement q = connection.getParsed(sql);
        if (q instanceof SelectStatement)
            return innerStatement.execute(((SelectStatement) q).getTransformed(conf, connection.getConnector()));
        if (q instanceof ShowSamplesStatement)
            return innerStatement.execute(((ShowSamplesStatement) q).getQuery(connection.getConnector()));
        else if (q instanceof VerdictStatement)
            try {
                q.run(conf, connection.getConnector());
                return false;
            } catch (VerdictException e) {
                throw e.toSQLException();
            }
        else
            return innerStatement.execute(q.toString(), columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return innerStatement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return innerStatement.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        innerStatement.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return innerStatement.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        innerStatement.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return innerStatement.isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return innerStatement.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return innerStatement.isWrapperFor(iface);
    }
}
