package edu.umich.verdict.models;

import java.util.Date;

public class StratifiedSample extends Sample {
    private String[] strataColumns;

    public StratifiedSample(String name, String tableName, double compRatio, long rowCount, int poissonColumns, String[] strataColumns) {
        this(name, tableName, new Date(), compRatio, rowCount, poissonColumns, strataColumns);
    }

    public StratifiedSample(String name, String tableName, Date lastUpdate, double compRatio, long rowCount, int poissonColumns, String[] strataColumns) {
        super(name, tableName, lastUpdate, compRatio, rowCount, poissonColumns);
        this.strataColumns = strataColumns;
    }

    public StratifiedSample(String name, String tableName, Date lastUpdate, double compRatio, long rowCount, int poissonColumns, String strataColumns) {
        this(name, tableName, lastUpdate, compRatio, rowCount, poissonColumns, strataColumns.split(","));
    }

    public String[] getStrataColumns() {
        return strataColumns;
    }

    public String getStrataColumnsString(char wrappingChar) {
        if (strataColumns == null || strataColumns.length < 1)
            return "";
        StringBuilder buf = new StringBuilder(wrappingChar + strataColumns[0] + wrappingChar);
        for (int i = 1; i < strataColumns.length; i++)
            buf.append(",").append(wrappingChar).append(strataColumns[i]).append(wrappingChar);
        return buf.toString();
    }

    public String getJoinCond(String t1, String t2, char wrappingChar) {
        StringBuilder buf;
        buf = new StringBuilder();
        for (String s : strataColumns)
            buf.append(" and ").append(t1).append(".").append(wrappingChar).append(s).append(wrappingChar).append("=").append(t2).append(".").append(wrappingChar).append(s).append(wrappingChar);
        return buf.toString().substring(4);
    }
}
