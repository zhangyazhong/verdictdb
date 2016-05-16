package edu.umich.verdict.connectors.sparksql;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.InvalidConfigurationException;
import edu.umich.verdict.connectors.hive.HiveConnector;

import java.sql.SQLException;

public class SparksqlConnector extends HiveConnector {
    public SparksqlConnector(Configuration conf) throws SQLException, ClassNotFoundException, InvalidConfigurationException {
        super(conf);
    }

    @Override
    protected String getDbmsName() {
        return "SparkSQL";
    }
}
