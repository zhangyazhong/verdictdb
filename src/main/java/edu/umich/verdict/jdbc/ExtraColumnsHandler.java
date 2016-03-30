package edu.umich.verdict.jdbc;

import edu.umich.verdict.transformation.TransformedQuery;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;

class ExtraColumnsHandler {
    private final ResultSet originalResultSet;
    private final TransformedQuery q;
    private final int originalCount;
    private final int aggregatesCount;
    //TODO: add variance and error percentage
    private final boolean showIntervals;
    private final boolean showErrors;
    private final ConfidenceInterval[] intervals;
    private final double[] errors;
    private boolean areValuesValid = false;
    private final int extraColumnsPerAggregate;

    public ExtraColumnsHandler(ResultSet rs, TransformedQuery q) {
        this.originalResultSet = rs;
        this.q = q;
        this.originalCount = q.getOriginalColumnsCount();

        showIntervals = true;
        showErrors = false;
        aggregatesCount = q.getAggregates().size();

        intervals = new ConfidenceInterval[q.getAggregates().size()];
        errors = new double[q.getAggregates().size()];

        extraColumnsPerAggregate = (showIntervals ? 1 : 0) + (showErrors ? 1 : 0);
    }

    public void updateValues() throws SQLException {
        int baseIndex = q.getOriginalColumnsCount() + 1;
        int trials = q.getBootstrapTrials();
        int margin = (int) (trials * (1 - q.getConfidence()) / 2);
        for (int j = 0; j < aggregatesCount; j++) {
            TransformedQuery.AggregateInfo aggr = q.getAggregates().get(j);
            double[] bootstrapResults = new double[trials];
            for (int i = 0; i < trials; i++)
                bootstrapResults[i] = originalResultSet.getDouble(i + baseIndex);
            baseIndex += trials;
            Arrays.sort(bootstrapResults);
            double estimatedAnswer = originalResultSet.getDouble(aggr.getColumn());
            ConfidenceInterval confidenceInterval = new ConfidenceInterval(estimatedAnswer, bootstrapResults, trials, margin);
            if (showIntervals)
                intervals[j] = confidenceInterval;
            if (showErrors)
                errors[j] = Math.max(Math.abs(confidenceInterval.start - estimatedAnswer), Math.abs(confidenceInterval.end - estimatedAnswer));
        }
        areValuesValid = true;
    }

    private String getInterval(int i) {
        return intervals[i / extraColumnsPerAggregate].toString();
    }

    private double getError(int i) {
        return errors[i / extraColumnsPerAggregate];
    }

    public int getTotalCount() {
        return getOriginalCount() + getCount();
    }

    public int getCount() {
        return aggregatesCount * extraColumnsPerAggregate;
    }

    public int getOriginalCount() {
        return originalCount;
    }

    public int getDisplaySize(int column) {
        return 40;
    }

    public String getLabel(int column) {
        return "c_" + column;
    }

    public int getPrecision(int column) {
        //TODO: implement
        return 0;
    }

    public int getScale(int column) {
        //TODO: implement
        return 0;
    }

    public int getType(int column) {
        //TODO: complete
        return Types.VARCHAR;
    }

    public String getTypeName(int column) {
        //TODO: complete
        return "VARCHAR";
    }

    public String getClassName(int column) {
        //TODO: complete
        return String.class.getName();
    }

    public Object get(int column) throws SQLException {
        //TODO: complete
        if (!areValuesValid)
            updateValues();
        return getInterval(column - originalCount - 1);
    }

    public void invalidateValues() {
        areValuesValid = false;
    }
}


class ConfidenceInterval {
    double start, end;

    ConfidenceInterval(double estimatedAnswer, double[] sortedNumbers, int trials, int margin) {
        start = 2 * estimatedAnswer - sortedNumbers[trials - margin - 1];
        end = 2 * estimatedAnswer - sortedNumbers[margin];
    }

    public String toString() {
        return "[" + start + ", " + end + "]";
    }
}
