package edu.umich.verdict.connectors.hive;

import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.models.Sample;
import edu.umich.verdict.models.StratifiedSample;

import java.sql.SQLException;

public class HiveMetaDataManager extends MetaDataManager {

    private final String udfBin;

    public HiveMetaDataManager(DbConnector connector, String udfBin) throws SQLException {
        super(connector);
        this.udfBin = udfBin;
    }

    protected void setupMetaDataDatabase() throws SQLException {
        super.setupMetaDataDatabase();
        System.out.println("Installin UDFs...");
        String jar = udfBin + "/verdict-hive-udf.jar";
        String initStatements = "delete jar if exists " + jar + ";" +
                "add jar " + jar + ";\n" +
                "drop function if exists verdict.poisson; create function verdict.poisson as 'edu.umich.tajik.verdict.hive.udf.Poisson';\n" +
                "drop function if exists verdict.conf_int; create function verdict.conf_int as 'edu.umich.tajik.verdict.hive.udf.ConfidenceInterval';\n" +
                "drop function if exists verdict.my_sum; create function verdict.my_sum as 'edu.umich.tajik.verdict.hive.uda.Sum';\n" +
                "drop function if exists verdict.my_count; create function verdict.my_count as 'edu.umich.tajik.verdict.hive.uda.Count';\n" +
                "drop function if exists verdict.my_avg; create function verdict.my_avg as 'edu.umich.tajik.verdict.hive.uda.Avg'";
        for (String q : initStatements.split(";"))
            if (!q.trim().isEmpty())
                executeStatement(q);
    }
}