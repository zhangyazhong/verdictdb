package edu.umich.verdict.models;

import java.util.Date;

public class StratifiedSample extends Sample {
    private String[] strataColumns;

    public StratifiedSample(String name, String tableName, double compRatio, long rowCount, int poissonColumns, String[] strataColumns) {
        super(name, tableName, new Date(), compRatio, rowCount, poissonColumns);
    }
    public StratifiedSample(String name, String tableName, Date lastUpdate, double compRatio, long rowCount, int poissonColumns, String[] strataColumns) {
        super(name, tableName, lastUpdate, compRatio, rowCount, poissonColumns);
        this.strataColumns = strataColumns;
    }
    public StratifiedSample(String name, String tableName, Date lastUpdate, double compRatio, long rowCount, int poissonColumns, String strataColumns) {
        this(name, tableName, lastUpdate, compRatio, rowCount, poissonColumns, strataColumns.split(","));
    }

    public String[] getStrataColumns(){
        return strataColumns;
    }

    public String getStrataColumnsString() {
        if (strataColumns == null || strataColumns.length < 1)
            return "";
        StringBuilder buf = new StringBuilder(strataColumns[0]);
        for (int i = 1; i < strataColumns.length; i++)
            buf.append(",").append(strataColumns[i]);
        return buf.toString();
    }

    public String getJoinCond(String t1, String t2) {
        StringBuilder buf;
        buf = new StringBuilder();
        for (String s : strataColumns)
            buf.append(" and ").append(t1).append(".").append(s).append("=").append(t2).append(".").append(s);
        return buf.toString().substring(4);
    }
}
