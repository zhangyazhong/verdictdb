package edu.umich.verdict.connectors;

import edu.umich.verdict.models.Sample;
import edu.umich.verdict.models.StratifiedSample;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 * This class is in charge of keeping track of and updating the metadata. The metadata is stored in the underlying DBMS
 * and in database named 'verdict'. Therefore, a MetaDataManager uses the same instance of DBConnector that is being
 * used for query processing to interact with the underlying DBMS.
 */
public abstract class MetaDataManager {
    public static final String METADATA_DATABASE = "verdict";
    protected ArrayList<Sample> samples = new ArrayList<>();
    protected DbConnector connector;
    protected DatabaseMetaData dbmsMetaData;

    public MetaDataManager(DbConnector connector) throws SQLException {
        this.connector = connector;
        this.dbmsMetaData = connector.getConnection().getMetaData();
    }

    public void initialize() throws SQLException {
        setupMetaDataDatabase();
        loadSamples();
    }

    protected void setupMetaDataDatabase() throws SQLException {
        executeStatement("create database if not exists " + METADATA_DATABASE);
        executeStatement("create table if not exists " + METADATA_DATABASE + ".sample  (name string, table_name string, last_update timestamp, comp_ratio double, row_count bigint, poisson_cols int, stratified boolean, strata_cols string)");
    }

    protected boolean executeStatement(String q) throws SQLException {
        return connector.executeStatement(q);
    }

    protected ResultSet executeQuery(String q) throws SQLException {
        return connector.executeQuery(q);
    }

    //TODO: General implementation
    //TODO: cleanup if failed
    public void createSample(Sample sample) throws Exception {
        loadSamples();
        for (Sample s : samples)
            if (s.getName().equals(sample.getName()))
                throw new SQLException("A sample with this name is already present.");
        long tableSize = getTableSize(sample.getTableName());
        if (sample instanceof StratifiedSample)
            createStratifiedSample((StratifiedSample) sample, tableSize);
        else
            createUniformSample(sample);
        computeSampleStats(sample);
        sample.setRowCount(getTableSize(getSampleFullName(sample)));
        sample.setCompRatio((double) sample.getRowCount() / tableSize);
        saveSampleInfo(sample);

    }

    //TODO: General implementation
    protected abstract void createStratifiedSample(StratifiedSample sample, long tableSize) throws SQLException;

    //TODO: General implementation
    protected abstract void createUniformSample(Sample sample) throws SQLException;

    protected void computeSampleStats(Sample sample) throws SQLException {
        System.out.println("Computing sample stats...");
        computeTableStats(getSampleFullName(sample));
    }

    protected abstract void computeTableStats(String name) throws SQLException;

    protected void saveSampleInfo(Sample sample) throws SQLException {
        String q;
        if (sample instanceof StratifiedSample)
            q = "insert into " + METADATA_DATABASE + ".sample VALUES ('" + sample.getName() + "', '" + sample.getTableName() + "', now(), " + sample.getCompRatio() + ", " + sample.getRowCount() + ", " + sample.getPoissonColumns() + ", cast(1 as boolean), '" + ((StratifiedSample) sample).getStrataColumnsString() + "')";
        else
            q = "insert into " + METADATA_DATABASE + ".sample VALUES ('" + sample.getName() + "', '" + sample.getTableName() + "', now(), " + sample.getCompRatio() + ", " + sample.getRowCount() + ", " + sample.getPoissonColumns() + ", cast(0 as boolean), '')";
        executeStatement(q);
        loadSamples();
    }

    public List<Sample> getTableSamples(String tableName) {
        return samples.stream().filter(s -> s.getTableName().equals(tableName)).collect(Collectors.toList());
    }

    public String getWeightsTable(StratifiedSample sample) {
        return METADATA_DATABASE + ".s_" + sample.getName() + "_w";
    }

    public void loadSamples() throws SQLException {
        ResultSet rs = executeQuery("select * from " + METADATA_DATABASE + ".sample");
        ArrayList<Sample> res = new ArrayList<>();
        while (rs.next()) {
            if (rs.getBoolean("stratified"))
                res.add(new StratifiedSample(rs.getString("name"), rs.getString("table_name"), rs.getDate("last_update"), rs.getDouble("comp_ratio"), rs.getLong("row_count"), rs.getInt("poisson_cols"), rs.getString("strata_cols")));
            else
                res.add(new Sample(rs.getString("name"), rs.getString("table_name"), rs.getDate("last_update"), rs.getDouble("comp_ratio"), rs.getLong("row_count"), rs.getInt("poisson_cols")));
        }
        samples = res;
    }

    public abstract long getTableSize(String name) throws SQLException;

    public ResultSet getSamplesInfo(String type, String table) throws SQLException {
        StringBuilder buf = new StringBuilder();
        buf.append("select name, table_name as `original table name`, round(comp_ratio*100,3) as `size (%)`, row_count as `rows`, cast(poisson_cols as string) as `poission columns`, cast(stratified as string) as `stratified`, strata_cols as `stratified by` from " + METADATA_DATABASE + ".sample where true ");
        if (type.equals("uniform"))
            buf.append(" and stratified<>true");
        if (type.equals("stratified"))
            buf.append(" and stratified=true");
        if (table != null)
            buf.append(" and table_name='").append(table).append("' ");
        buf.append(" order by name");
        return executeQuery(buf.toString());
    }

    //TODO: general implementation
    public abstract void deleteSample(String name) throws SQLException;

    public String getSampleFullName(Sample sample) {
        return METADATA_DATABASE + ".s_" + sample.getName();
    }

    public ArrayList<String> getTableCols(String name) throws SQLException {
        ArrayList<String> res = new ArrayList<>();
        ResultSet columns = dbmsMetaData.getColumns(null, null, name, null);

        while (columns.next()) {
            String columnName = columns.getString(4);
            res.add(columnName);
        }
        return res;
    }

    public int getSamplesCount() {
        return samples.size();
    }

    public char getAliasCharacter() {
        return '\'';
    }

    public String getPossionColumnPrefix() {
        return "v__p";
    }
}