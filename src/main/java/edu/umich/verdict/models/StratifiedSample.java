package edu.umich.verdict.models;

import java.util.Date;

public class StratifiedSample extends Sample {
    private String[] strataCols;

    public StratifiedSample(String name, String tableName, double compRatio, long rowCount, int poissonColumns, String[] strataCols) {
        super(name, tableName, new Date(), compRatio, rowCount, poissonColumns);
    }
    public StratifiedSample(String name, String tableName, Date lastUpdate, double compRatio, long rowCount, int poissonColumns, String[] strataCols) {
        super(name, tableName, lastUpdate, compRatio, rowCount, poissonColumns);
        this.strataCols = strataCols;
    }
    public StratifiedSample(String name, String tableName, Date lastUpdate, double compRatio, long rowCount, int poissonColumns, String strataCols) {
        this(name, tableName, lastUpdate, compRatio, rowCount, poissonColumns, strataCols.split(","));
    }

    public String getStrataColsStr() {
        if (strataCols == null || strataCols.length < 1)
            return "";
        StringBuilder buf = new StringBuilder(strataCols[0]);
        for (int i = 1; i < strataCols.length; i++)
            buf.append(",").append(strataCols[i]);
        return buf.toString();
    }

    public String getJoinCond(String t1, String t2) {
        StringBuilder buf;
        buf = new StringBuilder();
        for (String s : strataCols)
            buf.append(" and ").append(t1).append(".").append(s).append("=").append(t2).append(".").append(s);
        return buf.toString().substring(4);
    }
}
