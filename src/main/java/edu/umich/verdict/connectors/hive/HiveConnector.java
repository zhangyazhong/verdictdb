package edu.umich.verdict.connectors.hive;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.InvalidConfigurationException;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.connectors.impala.ImpalaMetaDataManager;

import java.sql.SQLException;

public class HiveConnector extends DbConnector {
    private boolean forImpala = false;

    public HiveConnector(Configuration conf) throws SQLException, ClassNotFoundException, InvalidConfigurationException {
        super(conf);
    }

    @Override
    protected MetaDataManager createMetaDataManager() throws SQLException {
        if(forImpala)
            return null;
        MetaDataManager metaDataManager = new HiveMetaDataManager(this, udfBin);
        metaDataManager.initialize();
        return metaDataManager;
    }

    @Override
    protected void initialize(Configuration conf) throws InvalidConfigurationException {
        super.initialize(conf);
        forImpala = conf.getBoolean("hive.for_impala");
    }

    @Override
    protected String getConnectionString(String host, String port) {
        return super.getConnectionString(host, port) + "/default";
    }

    @Override
    protected String getDriverClassPath() {
        return "com.cloudera.hive.jdbc41.HS2Driver";
    }

    @Override
    protected String getProtocolName() {
        return "hive2";
    }

    @Override
    protected String getDbmsName() {
        return "Hive";
    }

    @Override
    protected void connect(String connectionString, String user, String password) throws SQLException, ClassNotFoundException {
        super.connect(connectionString, user, password);
    }
}