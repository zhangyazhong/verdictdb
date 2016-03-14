package edu.umich.verdict.models;

import edu.umich.verdict.connectors.MetaDataManager;

import java.util.Date;

public class Sample {
    public String name, tableName;
    public double compRatio;
    public long rowCount;
    public Date lastUpdate;
    public int poissonColumns;
    public boolean stratified;
    public String[] strataCols;

    public Sample(String name, String tableName, double compRatio, long rowCount, int poissonColumns, String[] strataCols) {
        this(name, tableName, new Date(), compRatio, rowCount, poissonColumns, strataCols != null && strataCols.length > 0, null);
        this.strataCols = strataCols;
    }

    public Sample(String name, String tableName, Date lastUpdate, double compRatio, long rowCount, int poissonColumns, boolean stratified, String strataCols) {
        this.name = name;
        this.tableName = tableName;
        this.lastUpdate = lastUpdate;
        this.rowCount = rowCount;
        this.compRatio = compRatio;
        this.poissonColumns = poissonColumns;
        this.strataCols = strataCols == null || strataCols.trim().isEmpty() ? new String[0] : strataCols.split(",");
        this.stratified = stratified;
    }

    public Sample(String name, String tableName, double compRatio, long rowCount, int poissonColumns) {
        this(name, tableName, new Date(), compRatio, rowCount, poissonColumns, false, null);
    }

    public String getStrataColsStr() {
        if (strataCols == null || strataCols.length < 1)
            return "";
        StringBuilder buf = new StringBuilder(strataCols[0]);
        for (int i = 1; i < strataCols.length; i++)
            buf.append(",").append(strataCols[i]);
        return buf.toString();
    }

    public String getWeightsTable() {
        return MetaDataManager.METADATA_DATABASE + "." + name + "_w";
    }

    public String getJoinCond(String t1, String t2) {
        StringBuilder buf;
        buf = new StringBuilder();
        for (String s : strataCols)
            buf.append(" and ").append(t1).append(".").append(s).append("=").append(t2).append(".").append(s);
        return buf.toString().substring(4);
    }
}