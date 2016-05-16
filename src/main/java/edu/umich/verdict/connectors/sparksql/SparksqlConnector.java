package edu.umich.verdict.connectors.sparksql;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.InvalidConfigurationException;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.connectors.hive.HiveConnector;
import edu.umich.verdict.connectors.hive.HiveMetaDataManager;
import edu.umich.verdict.models.Sample;

import java.sql.SQLException;

public class SparksqlConnector extends HiveConnector {
    public SparksqlConnector(Configuration conf) throws SQLException, ClassNotFoundException, InvalidConfigurationException {
        super(conf);
    }

    @Override
    protected MetaDataManager createMetaDataManager() throws SQLException {
        MetaDataManager metaDataManager = new SparksqlMetaDataManager(this, udfBin);
        metaDataManager.initialize();
        return metaDataManager;
    }

    @Override
    protected String getDriverClassPath() {
        return "org.apache.hive.jdbc.HiveDriver";
    }

    @Override
    protected String getDbmsName() {
        return "SparkSQL";
    }
}
