package edu.umich.verdict.connectors.hive;

import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.models.Sample;
import edu.umich.verdict.models.StratifiedSample;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveMetaDataManager extends MetaDataManager {

    private final String udfBin;

    public HiveMetaDataManager(DbConnector connector, String udfBin) throws SQLException {
        super(connector);
        this.udfBin = udfBin;
    }

    protected void setupMetaDataDatabase() throws SQLException {
        super.setupMetaDataDatabase();
        try {
            executeQuery("select " + METADATA_DATABASE + ".poisson(1)");
        } catch (SQLException e) {
            System.out.println("Installing UDFs...");
            String jar = udfBin + "/verdict-hive-udf.jar";
            String initStatements = "delete jar if exists " + jar + ";" +
                    "add jar " + jar + ";\n" +
                    "drop function if exists " + METADATA_DATABASE + ".poisson; create function " + METADATA_DATABASE + ".poisson as 'edu.umich.tajik.verdict.hive.udf.Poisson';\n" +
                    "drop function if exists " + METADATA_DATABASE + ".poisson_sum; create function " + METADATA_DATABASE + ".poisson_sum as 'edu.umich.tajik.verdict.hive.uda.Sum';\n" +
                    "drop function if exists " + METADATA_DATABASE + ".poisson_count; create function " + METADATA_DATABASE + ".poisson_count as 'edu.umich.tajik.verdict.hive.uda.Count';\n" +
                    "drop function if exists " + METADATA_DATABASE + ".poisson_avg; create function " + METADATA_DATABASE + ".poisson_avg as 'edu.umich.tajik.verdict.hive.uda.Avg'" +
                    "drop function if exists " + METADATA_DATABASE + ".poisson_wcount; create function " + METADATA_DATABASE + ".poisson_count as 'edu.umich.tajik.verdict.hive.uda.WeightedCount';\n" +
                    "drop function if exists " + METADATA_DATABASE + ".poisson_wavg; create function " + METADATA_DATABASE + ".poisson_avg as 'edu.umich.tajik.verdict.hive.uda.WeightedAvg'";
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

    protected void createStratifiedSample(StratifiedSample sample) throws SQLException {
        long tableSize = getTableSize(sample.getTableName());
        String originalStrataCounts = getRandomTempTableName(), sampleWithoutWeights = getRandomTempTableName(), strataWeights = getRandomTempTableName();
        try {
            String strataCols = sample.getStrataColumnsString(getIdentifierWrappingChar());
            System.out.println("Collecting strata stats...");
            executeStatement("create table  " + originalStrataCounts + " as select " + strataCols + ", count(*) as cnt from " + sample.getTableName() + " group by " + strataCols);
            computeTableStats(originalStrataCounts);
            long strata = getTableSize(originalStrataCounts);
            long rowPerStratum = (long) ((tableSize * sample.getCompRatio()) / strata);
            if (rowPerStratum < MIN_ROW_FOR_STRATA)
                System.err.println("WARNING: With this sample size, each stratum will have at most " + rowPerStratum + " rows which is small low for accurate estimations in the future.");
            StringBuilder buf = new StringBuilder();
            for (String s : getTableCols(sample.getTableName()))
                buf.append(",").append(getIdentifierWrappingChar()).append(s).append(getIdentifierWrappingChar());
            buf.delete(0, 1);
            String cols = buf.toString();
            System.out.println("Creating sample... (This can take minutes)");
            executeStatement("create table " + sampleWithoutWeights + " as select " + cols + " from (select " + cols + ", rank() over (partition by " + strataCols + " order by rand()) as rnk from " + sample.getTableName() + ") s where rnk <= " + rowPerStratum + "");
            System.out.println("Calculating strata weights...");
            executeStatement("create table  " + strataWeights + " as select tw." + strataCols.replaceAll(",", ",tw.") + ", tw.cnt/sw.cnt as " + getWeightColumn() + " from (select " + strataCols + ", count(*) as cnt from " + sampleWithoutWeights + " group by " + strataCols + ") as sw join " + originalStrataCounts + " as tw on " + sample.getJoinCond("sw", "tw", getIdentifierWrappingChar()));
            buf = new StringBuilder();
            buf.append("create table ")
                    .append(getSampleFullName(sample))
                    .append(" stored as parquet as select s.")
                    .append(cols.replaceAll(",", ",s."))
                    .append(", r.")
                    .append(getWeightColumn());
            for (int i = 1; i <= sample.getPoissonColumns(); i++)
                buf.append("," + METADATA_DATABASE + ".poisson(cast(rand() * ").append(i).append(" as int)) as v__p").append(i);
            buf.append(" from ")
                    .append(strataWeights)
                    .append(" as r join ")
                    .append(sampleWithoutWeights)
                    .append(" as s on ")
                    .append(sample.getJoinCond("s", "r", getIdentifierWrappingChar()));
            executeStatement(buf.toString());
        } finally {
            executeStatement("drop table if exists " + originalStrataCounts);
            executeStatement("drop table if exists " + strataWeights);
            executeStatement("drop table if exists " + sampleWithoutWeights);
        }

    }

    protected void createUniformSample(Sample sample) throws SQLException {
        long buckets = Math.round(1 / sample.getCompRatio());
        StringBuilder buf = new StringBuilder();
        buf.append("create table ")
                .append(getSampleFullName(sample))
                .append(" as select *");
        for (int i = 1; i <= sample.getPoissonColumns(); i++)
            buf.append("," + METADATA_DATABASE + ".poisson(cast(rand() * ").append(i).append(" as int)) as v__p").append(i);
        buf.append(" from ").append(sample.getTableName()).append(" tablesample(bucket 1 out of ").append(buckets).append(" on rand())");
        executeStatement(buf.toString());
    }

    @Override
    protected void deleteSampleRecord(Sample sample) throws SQLException {
        executeStatement("drop table if exists " + METADATA_DATABASE + ".oldSample");
        executeStatement("alter table " + METADATA_DATABASE + ".sample rename to " + METADATA_DATABASE + ".oldSample");
        executeStatement("create table " + METADATA_DATABASE + ".sample as (select * from " + METADATA_DATABASE + ".oldSample where name <> \"" + sample.getName() + "\")");
        executeStatement("drop table if exists " + METADATA_DATABASE + ".oldSample");
        samples.remove(sample);
    }

    @Override
    public char getIdentifierWrappingChar() {
        return '`';
    }

    @Override
    protected void computeTableStats(String name) throws SQLException {
        executeStatement("analyze table " + name + " compute statistics");
    }

    @Override
    public long getTableSize(String name) throws SQLException {
        // first try using DESCRIBE command
        ResultSet rs = executeQuery("describe extended " + name);
        String description = null;
        while (rs.next())
            description = rs.getString(2);
        try {
            Matcher m = Pattern.compile("numRows=(\\d+)").matcher(description);
            m.find();
            return Long.parseLong(m.group(1));
        } catch (Exception e) {
            // if DESCRIBE failed
            rs = executeQuery("select count(*) from " + name);
            rs.next();
            return rs.getLong(1);
        }
    }

    @Override
    public ArrayList<String> getTableCols(String name) throws SQLException {
        ArrayList<String> res = new ArrayList<>();
        ResultSet columns = executeQuery("describe " + name);

        while (columns.next()) {
            String columnName = columns.getString(1);
            if (columnName.isEmpty())
                break;
            res.add(columnName);
        }
        return res;
    }

    @Override
    protected void saveSampleInfo(Sample sample) throws SQLException {
        String q;
        if (sample instanceof StratifiedSample)
            q = "insert into " + METADATA_DATABASE + ".sample select '" + sample.getName() + "', '" + sample.getTableName() + "', current_timestamp(), " + sample.getCompRatio() + ", " + sample.getRowCount() + ", " + sample.getPoissonColumns() + ", cast(1 as boolean), '" + ((StratifiedSample) sample).getStrataColumnsString(getIdentifierWrappingChar()) + "' from " + METADATA_DATABASE + ".dummy";
        else
            q = "insert into " + METADATA_DATABASE + ".sample select '" + sample.getName() + "', '" + sample.getTableName() + "', current_timestamp(), " + sample.getCompRatio() + ", " + sample.getRowCount() + ", " + sample.getPoissonColumns() + ", cast(0 as boolean), '' from " + METADATA_DATABASE + ".dummy";
        executeStatement(q);
        loadSamples();
    }

    @Override
    protected String getSamplesInfoQuery(String conditions) {
        return "select cast(name as varchar(30)) as name, cast(table_name as varchar(20)) as `original table`, cast(round(comp_ratio*100,3) as varchar(8)) as `size (%)`, cast(row_count as varchar(10)) as `rows`, cast(poisson_cols as varchar(15)) as `poisson columns`, strata_cols as `stratified by` from " + METADATA_DATABASE + ".sample"
                + (conditions != null ? " where " + conditions : "")
                + " order by `original table`, name";
    }

    @Override
    public boolean supportsUdfOverloading(){
        return false;
    }
}