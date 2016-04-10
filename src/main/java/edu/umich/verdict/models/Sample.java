package edu.umich.verdict.models;

import java.util.Date;

public class Sample {
    private String name;
    private String tableName;
    private double compRatio;
    private long rowCount;
    private Date lastUpdate;
    private int poissonColumns;

    public Sample(String name, String tableName, Date lastUpdate, double compRatio, long rowCount, int poissonColumns) {
        this.setName(name);
        this.setTableName(tableName);
        this.setLastUpdate(lastUpdate);
        this.setRowCount(rowCount);
        this.setCompRatio(compRatio);
        this.setPoissonColumns(poissonColumns);
    }

    public Sample(String name, String tableName, double compRatio, long rowCount, int poissonColumns) {
        this(name, tableName, new Date(), compRatio, rowCount, poissonColumns);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public double getCompRatio() {
        return compRatio;
    }

    public void setCompRatio(double compRatio) {
        this.compRatio = compRatio;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public Date getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(Date lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public int getPoissonColumns() {
        return poissonColumns;
    }

    public void setPoissonColumns(int poissonColumns) {
        this.poissonColumns = poissonColumns;
    }

    public long getTableSize() {
        return (long) (this.getRowCount() * (1 / this.getCompRatio()));
    }
}