package edu.umich.verdict.connectors;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.InvalidConfigurationException;

import java.sql.*;

public abstract class DbConnector {
    protected Connection connection;
    private MetaDataManager metaDataManager = null;
    protected String udfBin;

    public static DbConnector createConnector(Configuration conf) throws DbmsNotSupportedException, CannotConnectException, InvalidConfigurationException {
        String dbms = conf.get("dbms");
        String clsName = "edu.umich.verdict.connectors." + dbms + "." + (dbms.charAt(0) + "").toUpperCase() + dbms.substring(1)
                .toLowerCase() + "Connector";
        try {
            Class<DbConnector> cls = (Class<DbConnector>) Class.forName(clsName);
            return cls.getConstructor(Configuration.class).newInstance(conf);
        } catch (ClassNotFoundException e) {
            throw new DbmsNotSupportedException(dbms, e);
        } catch (ReflectiveOperationException e) {
            if (e.getCause() instanceof InvalidConfigurationException)
                throw (InvalidConfigurationException) e.getCause();
            throw new CannotConnectException(dbms, e.getCause());
        }
    }

    protected DbConnector(Configuration conf) throws SQLException, ClassNotFoundException, InvalidConfigurationException {
        String name = this.getDbmsName().toLowerCase();
        initialize(conf);
        connect(getConnectionString(conf.get(name + ".host"), conf.get(name + ".port"), conf.get(name + ".schema")), conf.get(name + ".user"), conf.get(name + ".password"));
        this.metaDataManager = createMetaDataManager();
    }

    protected void initialize(Configuration conf) throws InvalidConfigurationException {
        udfBin = conf.get("udf_bin");
        if (udfBin == null) {
            throw new InvalidConfigurationException("Configuration udf_bin is not set.");
        }
    }

    protected abstract MetaDataManager createMetaDataManager() throws SQLException;

    protected abstract String getDriverClassPath();

    protected abstract String getProtocolName();

    protected abstract String getDbmsName();

    protected String getConnectionString(String host, String port, String schema) {
        return "jdbc:" + getProtocolName() + "://" + host + ":" + port + (schema == null ? "" : "/" + schema);
    }

    protected void connect(String connectionString, String user, String password) throws SQLException, ClassNotFoundException {

        if (connection != null && !connection.isClosed())
            return;
        Class.forName(getDriverClassPath());
        connection = DriverManager.getConnection(connectionString, user, password);
    }

    public Connection getConnection() {
        return connection;
    }

    public ResultSet executeQuery(String q) throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute(q);
        return stmt.getResultSet();
    }

    public boolean executeStatement(String q) throws SQLException {
        Statement stmt = connection.createStatement();
        return stmt.execute(q);
    }

    public void close() throws SQLException {
        if (connection != null && !connection.isClosed())
            connection.close();
    }

    public MetaDataManager getMetaDataManager() {
        return this.metaDataManager;
    }

    public boolean isClosed() throws SQLException {
        return connection.isClosed();
    }
}