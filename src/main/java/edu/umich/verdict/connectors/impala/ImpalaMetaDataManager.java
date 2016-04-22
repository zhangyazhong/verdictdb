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

    protected void createStratifiedSample(StratifiedSample sample) throws SQLException {
        long tableSize = getTableSize(sample.getTableName());
        String originalStrataCounts = getRandomTempTableName(), sampleWithoutWeights = getRandomTempTableName(), strataRatios = getRandomTempTableName();
        try {
            String strataCols = sample.getStrataColumnsString(getIdentifierWrappingChar());
            System.out.println("Collecting strata stats...");
            executeStatement("create table  " + originalStrataCounts + " as (select " + strataCols + ", count(*) as cnt from " + sample.getTableName() + " group by " + strataCols + ")");
            computeTableStats(originalStrataCounts);
            long strata = getTableSize(originalStrataCounts);
            long rowPerStratum = (long) ((tableSize * sample.getCompRatio()) / strata);
            if (rowPerStratum < MIN_ROW_FOR_STRATA)
                System.err.println("WARNING: With this sample size, each stratum will have at most " + rowPerStratum + " rows which is too small for accurate estimations in the future.");
            StringBuilder buf = new StringBuilder();
            for (String s : getTableCols(sample.getTableName()))
                buf.append(",").append(getIdentifierWrappingChar()).append(s).append(getIdentifierWrappingChar());
            buf.delete(0, 1);
            String cols = buf.toString();
            System.out.println("Creating sample using Hive... (This can take minutes)");
            hiveConnector.executeStatement("create table " + sampleWithoutWeights + " as select " + cols + " from (select " + cols + ", rank() over (partition by " + strataCols + " order by rand()) as rnk from " + sample.getTableName() + ") s where rnk <= " + rowPerStratum + "");
            executeStatement("invalidate metadata");
            System.out.println("Calculating strata weights...");
            executeStatement("create table  " + strataRatios + " as (select tw." + strataCols.replaceAll(",", ",tw.") + ", tw.cnt/sw.cnt as v__ratio from (select " + strataCols + ", count(*) as cnt from " + sampleWithoutWeights + " group by " + strataCols + ") as sw join " + originalStrataCounts + " as tw on " + sample.getJoinCond("sw", "tw", getIdentifierWrappingChar()) + ")");
            buf = new StringBuilder();
            buf.append("create table ")
                    .append(getSampleFullName(sample))
                    .append(" stored as parquet as (select s.")
                    .append(cols.replaceAll(",", ",s."))
                    .append(", r.v__ratio");
            for (int i = 1; i <= sample.getPoissonColumns(); i++)
                buf.append("," + METADATA_DATABASE + ".poisson(").append(i).append(") as v__p").append(i);
            buf.append(" from ")
                    .append(strataRatios)
                    .append(" as r join ")
                    .append(sampleWithoutWeights)
                    .append(" as s on ")
                    .append(sample.getJoinCond("s", "r", getIdentifierWrappingChar()))
                    .append(")");
            executeStatement(buf.toString());
            executeStatement("invalidate metadata");
        } finally {
            executeStatement("drop table if exists " + originalStrataCounts);
            executeStatement("drop table if exists " + strataRatios);
            executeStatement("drop table if exists " + sampleWithoutWeights);
        }

    }

    protected void createUniformSample(Sample sample) throws SQLException {
        long buckets = Math.round(1 / sample.getCompRatio());
        System.out.println("Creating sample with Hive... (This can take minutes)");
        StringBuilder buf = new StringBuilder();
        if (sample.getPoissonColumns() == 0) {
            buf.append("create table ")
                    .append(getSampleFullName(sample))
                    .append(" stored as parquet as select *")
                    .append(" from ")
                    .append(sample.getTableName())
                    .append(" tablesample(bucket 1 out of ")
                    .append(buckets)
                    .append(" on rand())");
            hiveConnector.executeStatement(buf.toString());
            executeStatement("invalidate metadata");
        } else {
            // Poisson UDF may not be installed in Hive, so we create a sample without Poisson columns with Hive and add Poisson columns later with Impala
            String sampleWithoutPoissonCols = getRandomTempTableName();
            try {
                buf.append("create table ")
                        .append(sampleWithoutPoissonCols)
                        .append(" as select *")
                        .append(" from ")
                        .append(sample.getTableName())
                        .append(" tablesample(bucket 1 out of ")
                        .append(buckets)
                        .append(" on rand())");
                hiveConnector.executeStatement(buf.toString());
                executeStatement("invalidate metadata");
                buf = new StringBuilder();
                buf.append("create table ")
                        .append(getSampleFullName(sample))
                        .append(" stored as parquet as (select *");
                for (int i = 1; i <= sample.getPoissonColumns(); i++)
                    buf.append("," + METADATA_DATABASE + ".poisson(").append(i).append(") as v__p").append(i);
                buf.append(" from ")
                        .append(sampleWithoutPoissonCols)
                        .append(")");
                executeQuery(buf.toString());
            } finally {
                executeStatement("drop table if exists " + sampleWithoutPoissonCols);
            }
        }
    }


    @Override
    protected void deleteSampleRecord(Sample sample) throws SQLException {
//        executeStatement("insert overwrite table " + METADATA_DATABASE + ".sample select * from " + METADATA_DATABASE + ".sample where name not in (\"" + sample.getName() + "\")");
        executeStatement("drop table if exists " + METADATA_DATABASE + ".oldSample");
        executeStatement("alter table " + METADATA_DATABASE + ".sample rename to " + METADATA_DATABASE + ".oldSample");
        executeStatement("create table " + METADATA_DATABASE + ".sample as (select * from " + METADATA_DATABASE + ".oldSample where name <> \"" + sample.getName() + "\")");
        executeStatement("drop table if exists " + METADATA_DATABASE + ".oldSample");
        samples.remove(sample);
    }

    @Override
    public long getTableSize(String name) throws SQLException {
        ResultSet rs = executeQuery("show table stats " + name);
        long size = -1;
        while (rs.next())
            size = rs.getLong("#Rows");
        if (size == -1) {
            computeTableStats(name);
            rs = executeQuery("show table stats " + name);
            while (rs.next())
                size = rs.getLong("#Rows");
        }
        return size;
    }

    @Override
    protected void computeTableStats(String name) throws SQLException {
        executeStatement("compute stats " + name);
    }

    @Override
    public char getIdentifierWrappingChar() {
        return '`';
    }
}