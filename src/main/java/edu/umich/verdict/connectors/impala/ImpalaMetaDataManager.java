package edu.umich.verdict.connectors.impala;

import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.connectors.hive.HiveConnector;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.models.Sample;
import edu.umich.verdict.models.StratifiedSample;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ImpalaMetaDataManager extends MetaDataManager {
    private HiveConnector hiveConnector;
    private final String udfBinHdfs;

    public ImpalaMetaDataManager(DbConnector connector, HiveConnector hiveConnector, String udfBinHdfs) throws SQLException {
        super(connector);
        this.udfBinHdfs = udfBinHdfs;
        this.hiveConnector = hiveConnector;
    }

    protected void setupMetaDataDatabase() throws SQLException {
        super.setupMetaDataDatabase();

        executeStatement("invalidate metadata");

        try {
            executeQuery("select " + METADATA_DATABASE + ".poisson(1)");
        } catch (SQLException e) {
            System.out.println("Installing UDFs...");
            String lib = udfBinHdfs + "/verdict-impala-udf.so";
            String initStatements = "drop function if exists " + METADATA_DATABASE + ".poisson(int); create function " + METADATA_DATABASE + ".poisson (int) returns tinyint location '" + lib + "' symbol='Poisson';" +
                    "drop aggregate function if exists " + METADATA_DATABASE + ".poisson_count(int, double); create aggregate function " + METADATA_DATABASE + ".poisson_count(int, double) returns bigint location '" + lib + "' update_fn='CountUpdate';" +
                    "drop aggregate function if exists " + METADATA_DATABASE + ".poisson_sum(int, int); create aggregate function " + METADATA_DATABASE + ".poisson_sum(int, int) returns bigint location '" + lib + "' update_fn='SumUpdate';" +
                    "drop aggregate function if exists " + METADATA_DATABASE + ".poisson_sum(int, double); create aggregate function " + METADATA_DATABASE + ".poisson_sum(int, double) returns double location '" + lib + "' update_fn='SumUpdate';" +
                    "drop aggregate function if exists " + METADATA_DATABASE + ".poisson_avg(int, double); create aggregate function " + METADATA_DATABASE + ".poisson_avg(int, double) returns double intermediate string location '" + lib + "' init_fn=\"AvgInit\" merge_fn=\"AvgMerge\" update_fn='AvgUpdate' finalize_fn=\"AvgFinalize\";";
            for (String q : initStatements.split(";"))
                if (!q.trim().isEmpty())
                    executeStatement(q);
        }
    }

    protected void createStratifiedSample(StratifiedSample sample, long tableSize) throws SQLException {
        String tmp1 = METADATA_DATABASE + ".temp1", tmp2 = METADATA_DATABASE + ".temp2", tmp3 = METADATA_DATABASE + ".temp3";
        executeStatement("drop table if exists " + tmp1);
        String strataCols = sample.getStrataColumnsString();
        System.out.println("Collecting strata stats...");
        executeStatement("create table  " + tmp1 + " as (select " + strataCols + ", count(*) as cnt from " + sample.getTableName() + " group by " + strataCols + ")");
        computeTableStats(tmp1);
        long groups = getTableSize(tmp1);
        long groupLimit = (long) ((tableSize * sample.getCompRatio()) / groups);
        executeStatement("drop table if exists " + tmp2);
        StringBuilder buf = new StringBuilder();
        for (String s : getTableCols(sample.getTableName()))
            buf.append(",").append(s);
        buf.delete(0, 1);
        String cols = buf.toString();
        System.out.println("Creating sample using Hive... (This can take minutes)");
        hiveConnector.executeStatement("create table " + tmp2 + " as select " + cols + " from (select " + cols + ", rank() over (partition by " + strataCols + " order by rand()) as rnk from " + sample.getTableName() + ") s where rnk <= " + groupLimit + "");
        executeStatement("invalidate metadata");
        executeStatement("drop table if exists " + tmp3);
        executeStatement("create table  " + tmp3 + " as (select " + strataCols + ", count(*) as cnt from " + tmp2 + " group by " + strataCols + ")");
        String joinConds = sample.getJoinCond("s", "t");
        System.out.println("Calculating group weights...");
        executeStatement("create table " + getWeightsTable(sample) + " as (select s." + strataCols.replaceAll(",", ",s.") + ", t.cnt/s.cnt as ratio, t.cnt/" + tableSize + " as weight from " + tmp1 + " as t join " + tmp3 + " as s on " + joinConds + ")");
        executeStatement("drop table if exists " + tmp1);
        executeStatement("drop table if exists " + tmp3);
        executeStatement("invalidate metadata");
        addPoissonCols(sample, tmp2);
        executeStatement("drop table if exists " + tmp2);

    }

    protected void createUniformSample(Sample sample) throws SQLException {
        long buckets = Math.round(1 / sample.getCompRatio());
        String tmp1 = METADATA_DATABASE + ".temp_sample";
        System.out.println("Creating sample with Hive... (This can take minutes)");
        hiveConnector.executeStatement("drop table if exists " + tmp1);
        String create = "create table " + tmp1 + " as select * from " + sample.getTableName() + " tablesample(bucket 1 out of " + buckets + " on rand())";
        hiveConnector.executeStatement(create);
        executeStatement("invalidate metadata");
        addPoissonCols(sample, tmp1);
        executeStatement("drop table if exists " + tmp1);

    }

    private void addPoissonCols(Sample sample, String fromTable) throws SQLException {
        System.out.println("Adding " + sample.getPoissonColumns() + " Poisson random number columns to the sample...");
        StringBuilder buf = new StringBuilder("create table " + getSampleFullName(sample) + " stored as parquet as (select *");
        for (int i = 1; i <= sample.getPoissonColumns(); i++)
            buf.append("," + METADATA_DATABASE + ".poisson(").append(i).append(") as v__p").append(i);
        buf.append(" from ").append(fromTable).append(")");
        executeStatement(buf.toString());
    }

    public void deleteSample(String name) throws SQLException {
        Sample sample = null;
        for (Sample s : samples)
            if (s.getName().equals(name)) {
                sample = s;
                break;
            }
        if (sample == null)
            throw new SQLException("No sample with this name exists.");
        executeStatement("drop table if exists " + getSampleFullName(sample));
        if (sample instanceof StratifiedSample)
            executeStatement("drop table if exists " + getWeightsTable((StratifiedSample) sample));
        executeStatement("drop table if exists " + METADATA_DATABASE + ".oldSample");
        executeStatement("alter table " + METADATA_DATABASE + ".sample rename to " + METADATA_DATABASE + ".oldSample");
        executeStatement("create table " + METADATA_DATABASE + ".sample as (select * from " + METADATA_DATABASE + ".oldSample where name <> '" + name + "')");
        executeStatement("drop table if exists " + METADATA_DATABASE + ".oldSample");
        samples.remove(sample);
    }

    @Override
    public long getTableSize(String name) throws SQLException {
        ResultSet rs = executeQuery("show table stats " + name);
        rs.next();
        long size = rs.getLong(1);
        if(size==-1) {
            computeTableStats(name);
            rs = executeQuery("show table stats " + name);
            rs.next();
            size = rs.getLong(1);
        }
        return size;
    }

    @Override
    protected void computeTableStats(String name) throws SQLException {
        executeStatement("compute stats " + name);
    }
}