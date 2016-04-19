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
                    "drop function if exists " + METADATA_DATABASE + ".poisson_avg; create function " + METADATA_DATABASE + ".poisson_avg as 'edu.umich.tajik.verdict.hive.uda.Avg'";
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

    @Override
    protected void createStratifiedSample(StratifiedSample sample, long tableSize) throws SQLException {
        String tmp1 = METADATA_DATABASE + ".temp1", tmp2 = METADATA_DATABASE + ".temp2", tmp3 = METADATA_DATABASE + ".temp3";
        executeStatement("drop table if exists " + tmp1);
        String strataCols = sample.getStrataColumnsString();
        System.out.println("Collecting strata stats...");
        executeStatement("create table  " + tmp1 + " as select " + strataCols + ", count(*) as cnt from " + sample.getTableName() + " group by " + strataCols );
        computeTableStats(tmp1);
        long groups = getTableSize(tmp1);
        //TODO: don't continue if groupLimit is too small
        long groupLimit = (long) ((tableSize * sample.getCompRatio()) / groups);
        executeStatement("drop table if exists " + tmp2);
        StringBuilder buf = new StringBuilder();
        for (String s : getTableCols(sample.getTableName()))
            buf.append(",").append(s);
        buf.delete(0, 1);
        String cols = buf.toString();
        System.out.println("Creating sample... (This can take minutes)");
        executeStatement("create table " + tmp2 + " as select " + cols + " from (select " + cols + ", rank() over (partition by " + strataCols + " order by rand()) as rnk from " + sample.getTableName() + ") s where rnk <= " + groupLimit + "");
        executeStatement("drop table if exists " + tmp3);
        executeStatement("create table  " + tmp3 + " as select " + strataCols + ", count(*) as cnt from " + tmp2 + " group by " + strataCols );
        String joinConds = sample.getJoinCond("s", "t");
        System.out.println("Calculating group weights...");
        //ratio = (# of tuples in table)/(# of tupples in sample) for each stratum => useful for COUNT and SUM
        //weight = (# of stratum tuples in table)/(table size) => useful for AVG
        executeStatement("create table " + getWeightsTable(sample) + " as select s." + strataCols.replaceAll(",", ",s.") + ", t.cnt/s.cnt as ratio, t.cnt/" + tableSize + " as weight from " + tmp1 + " as t join " + tmp3 + " as s on " + joinConds);
        executeStatement("drop table if exists " + tmp1);
        executeStatement("drop table if exists " + tmp3);
        addPoissonCols(sample, tmp2);
        executeStatement("drop table if exists " + tmp2);

    }

    @Override
    protected void createUniformSample(Sample sample) throws SQLException {
        long buckets = Math.round(1 / sample.getCompRatio());
        String tmp1 = METADATA_DATABASE + ".temp_sample";
        executeStatement("drop table if exists " + tmp1);
        String create = "create table " + tmp1 + " as select * from " + sample.getTableName() + " tablesample(bucket 1 out of " + buckets + " on rand())";
        executeStatement(create);
        addPoissonCols(sample, tmp1);
        executeStatement("drop table if exists " + tmp1);
    }

    private void addPoissonCols(Sample sample, String fromTable) throws SQLException {
        System.out.println("Adding " + sample.getPoissonColumns() + " Poisson random number columns to the sample...");
        StringBuilder buf = new StringBuilder("create table " + getSampleFullName(sample) + " stored as parquet as select *");
        for (int i = 1; i <= sample.getPoissonColumns(); i++)
            buf.append("," + METADATA_DATABASE + ".poisson(").append(i).append(") as v__p").append(i);
        buf.append(" from ").append(fromTable);
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
    public char getAliasCharacter() {
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
        ResultSet columns = executeQuery("describe "+name);

        while (columns.next()) {
            String columnName = columns.getString(1);
            res.add(columnName);
        }
        return res;
    }

    @Override
    protected void saveSampleInfo(Sample sample) throws SQLException {
        String q;
        if (sample instanceof StratifiedSample)
            q = "insert into " + METADATA_DATABASE + ".sample select '" + sample.getName() + "', '" + sample.getTableName() + "', current_timestamp(), " + sample.getCompRatio() + ", " + sample.getRowCount() + ", " + sample.getPoissonColumns() + ", cast(1 as boolean), '" + ((StratifiedSample) sample).getStrataColumnsString() + "' from "+METADATA_DATABASE+".dummy";
        else
            q = "insert into " + METADATA_DATABASE + ".sample select '" + sample.getName() + "', '" + sample.getTableName() + "', current_timestamp(), " + sample.getCompRatio() + ", " + sample.getRowCount() + ", " + sample.getPoissonColumns() + ", cast(0 as boolean), '' from "+METADATA_DATABASE+".dummy";
        executeStatement(q);
        loadSamples();
    }

    @Override
    protected String getSamplesInfoQuery(String conditions){
        return "select cast(name as varchar(30)) as name, cast(table_name as varchar(20)) as `original table`, cast(round(comp_ratio*100,3) as varchar(8)) as `size (%)`, cast(row_count as varchar(10)) as `rows`, cast(poisson_cols as varchar(15)) as `poisson columns`, strata_cols as `stratified by` from " + METADATA_DATABASE + ".sample"
                +(conditions!=null?" where "+conditions:"")
                +" order by `original table`, name";
    }
}