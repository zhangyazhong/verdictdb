package edu.umich.verdict.connectors.sparksql;

import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.connectors.hive.HiveMetaDataManager;

import java.sql.SQLException;

public class SparksqlMetaDataManager extends HiveMetaDataManager {
    public SparksqlMetaDataManager(DbConnector connector, String udfBin) throws SQLException {
        super(connector, udfBin);
    }

    protected void setupMetaDataDatabase() throws SQLException {
        super.setupMetaDataDatabase();
        try {
            executeQuery("select " + getUdfFullName("poisson") + "(1)");
        } catch (SQLException e) {
            System.out.println("Installing UDFs...");
            String jar = udfBin + "/verdict-hive-udf.jar";
            String initStatements = "delete jar if exists " + jar + ";" +
                    "add jar " + jar + ";\n" +
                    "create temporary function " + getUdfFullName("poisson") + " as 'edu.umich.tajik.verdict.hive.udf.Poisson';\n" +
                    "create temporary function " + getUdfFullName("poisson_sum") + " as 'edu.umich.tajik.verdict.hive.uda.Sum';\n" +
                    "create temporary function " + getUdfFullName("poisson_count") + " as 'edu.umich.tajik.verdict.hive.uda.Count';\n" +
                    "create temporary function " + getUdfFullName("poisson_avg") + " as 'edu.umich.tajik.verdict.hive.uda.Avg';\n" +
                    "create temporary function " + getUdfFullName("poisson_wcount") + " as 'edu.umich.tajik.verdict.hive.uda.WeightedCount';\n" +
                    "create temporary function " + getUdfFullName("poisson_wavg") + " as 'edu.umich.tajik.verdict.hive.uda.WeightedAvg'";
            for (String q : initStatements.split(";"))
                if (!q.trim().isEmpty())
                    executeStatement(q);

            // dummy table is used for inserting values into sample table!
            if (!executeQuery("show tables in " + METADATA_DATABASE + " like 'dummy'").next()) {
                executeQuery("create table " + METADATA_DATABASE + ".dummy (id int)");
                executeQuery("insert into " + METADATA_DATABASE + ".dummy values (0)");
            }
        }
    }

    protected boolean supportsSchemaUdf(){
        return false;
    }
}
