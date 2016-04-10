package edu.umich.verdict.jdbc;

import edu.umich.verdict.transformation.TransformedQuery;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Objects;

class ExtraColumnsHandler {
    private final ResultSet originalResultSet;
    private final TransformedQuery q;
    private final int originalCount;
    private final int aggregatesCount;
    private final boolean showIntervals, showErrors, showErrorPercentages, showVariances;
    private final ConfidenceInterval[] intervals;
    private final double[] errors, errorPercentages, variances;
    private boolean areValuesValid = false;
    private final int extraColumnsPerAggregate;
    private final ExtraColumnType[] extraColumnsTypes;

    public ExtraColumnsHandler(ResultSet rs, TransformedQuery q) {
        this.originalResultSet = rs;
        this.q = q;
        this.originalCount = q.getOriginalColumnsCount();

        showIntervals = true;
        showVariances = true;
        showErrors = false;
        showErrorPercentages = false;
        extraColumnsPerAggregate = (showIntervals ? 1 : 0) + (showErrors ? 1 : 0) + (showVariances ? 1 : 0);
        extraColumnsTypes = new ExtraColumnType[extraColumnsPerAggregate];

        int i = 0;
        if (showIntervals) {
            extraColumnsTypes[i] = ExtraColumnType.ConfidenceInterval;
            i++;
        }
        if (showErrors) {
            extraColumnsTypes[i] = ExtraColumnType.Error;
            i++;
        }
        if (showErrorPercentages) {
            extraColumnsTypes[i] = ExtraColumnType.ErrorPercentage;
            i++;
        }
        if (showVariances)
            extraColumnsTypes[i] = ExtraColumnType.Variance;

        aggregatesCount = q.getAggregates().size();
        intervals = showIntervals ? new ConfidenceInterval[aggregatesCount] : null;
        errors = showErrors ? new double[aggregatesCount] : null;
        errorPercentages = showErrorPercentages ? new double[aggregatesCount] : null;
        variances = showVariances ? new double[aggregatesCount] : null;
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
            if (showIntervals || showErrors) {
                Arrays.sort(bootstrapResults);
                double estimatedAnswer = originalResultSet.getDouble(aggr.getColumn());
                ConfidenceInterval confidenceInterval = new ConfidenceInterval(estimatedAnswer, bootstrapResults, trials, margin);
                if (showIntervals)
                    intervals[j] = confidenceInterval;
                if (showErrors)
                    errors[j] = Math.max(Math.abs(confidenceInterval.start - estimatedAnswer), Math.abs(confidenceInterval.end - estimatedAnswer));
                if (showErrorPercentages)
                    errorPercentages[j] = 100 * Math.max(Math.abs(confidenceInterval.start - estimatedAnswer), Math.abs(confidenceInterval.end - estimatedAnswer)) / estimatedAnswer;
            }
            if (showVariances) {
                variances[j] = getVariance(bootstrapResults);
            }
        }
        areValuesValid = true;
    }

    double getMean(double[] data) {
        double sum = 0.0;
        for (double a : data)
            sum += a;
        return sum / data.length;
    }

    double getVariance(double[] data) {
        double mean = getMean(data);
        double temp = 0;
        for (double a : data)
            temp += (mean - a) * (mean - a);
        return temp / data.length;
    }

    private Object getValue(int i) {
        switch (extraColumnsTypes[i % extraColumnsPerAggregate]) {
            case ConfidenceInterval:
                return intervals[i / extraColumnsPerAggregate].toString();
            case Error:
                return errors[i / extraColumnsPerAggregate] + "";
            case ErrorPercentage:
                return errorPercentages[i / extraColumnsPerAggregate] + "%";
            case Variance:
                return variances[i / extraColumnsPerAggregate] + "";
            default:
                return null;
        }
    }

    private String getInterval(int i) {
        return intervals[i].toString();
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
        column = column - originalCount - 1;
        int i=column/extraColumnsPerAggregate;
        int aggregateColumn = q.getAggregates().get(i).getColumn();
        switch (extraColumnsTypes[column % extraColumnsPerAggregate]) {
            case ConfidenceInterval:
                return "ci_"+aggregateColumn;
            case Error:
                return "e_"+aggregateColumn;
            case ErrorPercentage:
                return "e%_"+aggregateColumn;
            case Variance:
                return "v_"+aggregateColumn;
            default:
                return null;
        }
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
        return getValue(column - originalCount - 1);
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


enum ExtraColumnType {
    ConfidenceInterval,
    Error,
    ErrorPercentage,
    Variance
}
