package edu.umich.verdict.connectors.sparksql;

import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.connectors.hive.HiveMetaDataManager;
import edu.umich.verdict.models.Sample;

import java.sql.SQLException;

public class SparksqlMetaDataManager extends HiveMetaDataManager {
    public SparksqlMetaDataManager(DbConnector connector, String udfBin) throws SQLException {
        super(connector, udfBin);
    }

    protected void installUdfs() throws SQLException {
        System.out.println("Installing UDFs...");
        String jar = udfBin + "/verdict-hive-udf.jar";
        String initStatements = "add jar " + jar + ";\n" +
                "create temporary function " + getUdfFullName("poisson") + " as 'edu.umich.tajik.verdict.hive.udf.Poisson';\n" +
                "create temporary function " + getUdfFullName("poisson_sum") + " as 'edu.umich.tajik.verdict.hive.uda.Sum';\n" +
                "create temporary function " + getUdfFullName("poisson_count") + " as 'edu.umich.tajik.verdict.hive.uda.Count';\n" +
                "create temporary function " + getUdfFullName("poisson_avg") + " as 'edu.umich.tajik.verdict.hive.uda.Avg';\n" +
                "create temporary function " + getUdfFullName("poisson_wcount") + " as 'edu.umich.tajik.verdict.hive.uda.WeightedCount';\n" +
                "create temporary function " + getUdfFullName("poisson_wavg") + " as 'edu.umich.tajik.verdict.hive.uda.WeightedAvg'";
        for (String q : initStatements.split(";"))
            if (!q.trim().isEmpty())
                executeStatement(q);
    }

    protected boolean supportsSchemaUdf(){
        return false;
    }

    @Override
    protected void createUniformSample(Sample sample) throws SQLException {
        StringBuilder buf = new StringBuilder();
        buf.append("create table ")
                .append(getSampleFullName(sample))
                .append(" as select *");
        for (int i = 1; i <= sample.getPoissonColumns(); i++)
            buf.append(",").append(getUdfFullName("poisson")).append("(cast(rand() * ").append(i).append(" as int)) as v__p").append(i);
        buf.append(" from ").append(sample.getTableName()).append(" where rand() <= ").append(sample.getCompRatio());
        executeStatement(buf.toString());
    }
}
