package edu.umich.verdict.connectors;

import edu.umich.verdict.models.Sample;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 * This class is in charge of keeping track of and updating the metadata. The metadata is stored in the underlying DBMS
 * and in database named 'verdict'. Therefore, a MetaDataManager uses the same instance of DBConnector that is being
 * used for query processing to interact with the underlying DBMS.
 */
public class MetaDataManager {
    public static final String METADATA_DATABASE = "verdict";
    protected ArrayList<Sample> samples = new ArrayList<>();
    protected DbConnector connector;
    protected DatabaseMetaData dbmsMetaData;

    public MetaDataManager(DbConnector connector) throws SQLException {
        this.connector = connector;
        this.dbmsMetaData = connector.getConnection().getMetaData();
        loadSamples();
    }

    protected boolean executeStatement(String q) throws SQLException {
        return connector.executeStatement(q);
    }

    protected ResultSet executeQuery(String q) throws SQLException {
        return connector.executeQuery(q);
    }

    //TODO: General implementation
    public void createSample(Sample sample) throws Exception {
        loadSamples();
        for (Sample s : samples)
            if (s.name.equals(sample.name))
                throw new SQLException("A sample with this name is already present.");
        long tableSize = getTableSize(sample.tableName);
        String tmpName;
        if (sample.stratified)
            tmpName = createStratifiedSample(sample, tableSize);
        else
            tmpName = createUniformSample(sample);
        executeStatement("invalidate metadata");
        addPoissonCols(sample, tmpName);
        executeStatement("drop table if exists " + tmpName);
        computeSampleStats(sample);
        sample.rowCount = getTableSize(sample.name);
        sample.compRatio = (double) sample.rowCount / tableSize;
        saveSampleInfo(sample);
    }

    //TODO: General implementation
    protected String createStratifiedSample(Sample sample, long tableSize) throws SQLException {
        String tmp1 = METADATA_DATABASE + ".temp1", tmp2 = METADATA_DATABASE + ".temp2", tmp3 = METADATA_DATABASE + ".temp3";
        executeStatement("drop table if exists " + tmp1);
        String strataCols = sample.getStrataColsStr();
        System.out.println("Collecting groups stats...");
        executeStatement("create table  " + tmp1 + " as (select " + strataCols + ", count(*) as cnt from " + sample.tableName + " group by " + strataCols + ")");
        computeTableStats(tmp1);
        long groups = getTableSize(tmp1);
        long groupLimit = (long) ((tableSize * sample.compRatio) / groups);
        executeStatement("drop table if exists " + tmp2);
        StringBuilder buf = new StringBuilder();
        for (String s : getTableCols(sample.tableName))
            buf.append(",").append(s);
        buf.delete(0, 1);
        String cols = buf.toString();
        System.out.println("Creating sample with Hive... (This can take minutes)");
        executeStatement("create table " + tmp2 + " as select " + cols + " from (select " + cols + ", rank() over (partition by " + strataCols + " order by rand()) as rnk from " + sample.tableName + ") s where rnk <= " + groupLimit + "");
        executeStatement("invalidate metadata");
        executeStatement("drop table if exists " + tmp3);
        executeStatement("create table  " + tmp3 + " as (select " + strataCols + ", count(*) as cnt from " + tmp2 + " group by " + strataCols + ")");
        String joinConds = sample.getJoinCond("s", "t");
        System.out.println("Calculating group weights...");
        executeStatement("create table " + sample.getWeightsTable() + " as (select s." + strataCols.replaceAll(",", ",s.") + ", t.cnt/s.cnt as ratio, t.cnt/" + tableSize + " as weight from " + tmp1 + " as t join " + tmp3 + " as s on " + joinConds + ")");
        executeStatement("drop table if exists " + tmp1);
        executeStatement("drop table if exists " + tmp3);
        return tmp2;
    }

    //TODO: General implementation
    protected String createUniformSample(Sample sample) throws SQLException {
        long buckets = Math.round(1 / sample.compRatio);
        String tmp1 = METADATA_DATABASE + ".temp_sample";
        System.out.println("Creating sample with Hive... (This can take minutes)");
        executeStatement("drop table if exists " + tmp1);
        String create = "create table " + tmp1 + " as select * from " + sample.tableName + " tablesample(bucket 1 out of " + buckets + " on rand())";
        executeStatement(create);
        return tmp1;
    }

    protected void computeSampleStats(Sample sample) throws SQLException {
        System.out.println("Computing sample stats...");
        computeTableStats(sample.name);
    }

    protected void computeTableStats(String name) throws SQLException {
        executeStatement("compute stats " + name);
    }

    protected void addPoissonCols(Sample sample, String fromTable) throws SQLException {
        System.out.println("Adding " + sample.poissonColumns + " Poisson random number columns to the sample...");
        StringBuilder buf = new StringBuilder("create table " + sample.name + " stored as parquet as (select *");
        for (int i = 1; i <= sample.poissonColumns; i++)
            buf.append(",poisson() as __p").append(i);
        buf.append(" from ").append(fromTable).append(")");
        executeStatement(buf.toString());
    }

    private void saveSampleInfo(Sample sample) throws SQLException {
        String q = "insert into " + METADATA_DATABASE + ".sample VALUES ('" + sample.name + "', '" + sample.tableName + "', now(), " + sample.compRatio + ", " + sample.rowCount + ", " + sample.poissonColumns + ", cast(" + (sample.stratified ? 1 : 0) + " as boolean), '" + sample.getStrataColsStr() + "')";
        executeStatement(q);
        loadSamples();
    }

    public List<Sample> getTableSamples(String tableName) {
        return samples.stream().filter(s -> s.tableName.equals(tableName)).collect(Collectors.toList());
    }

    public void loadSamples() throws SQLException {
        String createTable = "create table if not exists " + METADATA_DATABASE + ".sample " +
                "(name string, table_name string, last_update timestamp, comp_ratio double, row_count bigint, poisson_cols int)";
        executeStatement(createTable);

        ResultSet rs = executeQuery("select * from " + METADATA_DATABASE + ".sample");
        ArrayList<Sample> res = new ArrayList<>();
        while (rs.next()) {
            res.add(new Sample(rs.getString("name"), rs.getString("table_name"), rs.getDate("last_update"), rs.getDouble("comp_ratio"), rs.getLong("row_count"), rs.getInt("poisson_cols"), rs.getBoolean("stratified"), rs.getString("strata_cols")));
        }
        samples = res;
    }

    public long getTableSize(String name) throws SQLException {
        ResultSet rs = executeQuery("show table stats " + name);
        rs.next();
        return rs.getLong(1);
    }

    public ResultSet getSamplesInfo() throws SQLException {
        return executeQuery("select name, table_name as `original table name`, round(comp_ratio*100,3) as `size (%)`, row_count as `rows`, cast(poisson_cols as string) as `poission columns`, cast(stratified as string) as `stratified`, strata_cols as `stratified by` from " + METADATA_DATABASE + ".sample order by name");
    }

    public void deleteSample(String name) throws SQLException {
        Sample sample = null;
        for (Sample s : samples)
            if (s.name.equals(name)) {
                sample = s;
                break;
            }
        if (sample == null)
            throw new SQLException("No sample with this name exists.");
        executeStatement("drop table if exists " + name);
        executeStatement("drop table if exists " + sample.getWeightsTable());
        executeStatement("drop table if exists " + METADATA_DATABASE + ".oldSample");
        executeStatement("alter table " + METADATA_DATABASE + ".sample rename to " + METADATA_DATABASE + ".oldSample");
        executeStatement("create table " + METADATA_DATABASE + ".sample as (select * from " + METADATA_DATABASE + ".oldSample where name <> '" + name + "')");
        executeStatement("drop table if exists " + METADATA_DATABASE + ".oldSample");
        samples.remove(sample);
    }

    public ArrayList<String> getTableCols(String name) throws SQLException {
        ArrayList<String> res =new ArrayList<>();
        ResultSet columns = dbmsMetaData.getColumns(null,null,name,null);

        while(columns.next())
        {
            String columnName = columns.getString(4);
            res.add(columnName);
        }
        return res;
    }

    public int getSamplesCount() {
        return samples.size();
    }
}