package edu.umich.verdict.connectors;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.impala.ImpalaConnector;

import java.sql.*;

public abstract class DbConnector {
    public static DbConnector createConnector(Configuration conf) throws DbmsNotSupportedException, CannotConnectException {
        String dbms = conf.get("dbms");
        String clsName = "edu.umich.verdict.connectors." + dbms + "." + (dbms.charAt(0) + "").toUpperCase() + dbms.substring(1)
                .toLowerCase() + "Connector";
        try {
            Class<DbConnector> cls = (Class<DbConnector>) Class.forName(clsName);
            return cls.getConstructor(Configuration.class).newInstance(conf);
        } catch (ClassNotFoundException e) {
            throw new DbmsNotSupportedException(dbms, e);
        } catch (ReflectiveOperationException e) {
            throw new CannotConnectException(dbms, (SQLException) e.getCause());
        }
    }

    protected Connection connection;
    private MetaDataManager metaDataManager = null;

    protected DbConnector(Configuration conf) throws SQLException,
            ClassNotFoundException {
        String name = this.getDbmsName().toLowerCase();
        connect(getConnectionString(conf.get(name + ".host"), conf.get(name + ".port")), conf.get(name + ".user"), conf.get(name + ".password"));
        this.metaDataManager = createMetaDataManager();
    }

    protected MetaDataManager createMetaDataManager() throws SQLException {
        return new MetaDataManager(this);
    }

    protected abstract String getDriverClassPath();

    protected abstract String getProtocolName();

    protected abstract String getDbmsName();

    protected String getConnectionString(String host, String port) {
        return "jdbc:" + getProtocolName() + "://" + host + ":" + port;
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
}