package edu.umich.verdict.jdbc;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.InvalidConfigurationException;
import edu.umich.verdict.transformation.TransformedQuery;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

class ExtraColumnsHandler {
    private final ResultSet originalResultSet;
    private final TransformedQuery q;
    private final int originalCount;
    private final int aggregatesCount;
    private boolean showIntervals = false, showErrors = false, showErrorPercentages = false, showVariances = false;
    private final ConfidenceInterval[] intervals;
    private final double[] errors, errorPercentages, variances;
    private boolean areValuesValid = false;
    private int extraColumnsPerAggregate;
    private final ExtraColumnType[] extraColumnsTypes;

    public ExtraColumnsHandler(ResultSet rs, TransformedQuery q, Configuration conf) throws InvalidConfigurationException {
        this.originalResultSet = rs;
        this.q = q;
        this.originalCount = q.getOriginalColumnsCount();

        String[] extraColumns = conf.get("bootstrap.extra_columns").split("_");
        extraColumnsPerAggregate = 0;
        ArrayList<ExtraColumnType> extraColumnsTypesList = new ArrayList<ExtraColumnType>();
        for (String extraColumn : extraColumns) {
            extraColumnsPerAggregate++;
            switch (extraColumn) {
                case "ci":
                    extraColumnsTypesList.add(ExtraColumnType.ConfidenceIntervalLower);
                    extraColumnsTypesList.add(ExtraColumnType.ConfidenceIntervalUpper);
                    extraColumnsPerAggregate++;
                    showIntervals = true;
                    break;
                case "e":
                    extraColumnsTypesList.add(ExtraColumnType.Error);
                    showErrors = true;
                    break;
                case "ep":
                    extraColumnsTypesList.add(ExtraColumnType.ErrorPercentage);
                    showErrorPercentages = true;
                    break;
                case "v":
                    extraColumnsTypesList.add(ExtraColumnType.Variance);
                    showVariances = true;
                    break;
                default:
                    throw new InvalidConfigurationException("Invalid value for 'bootstrap.extra_columns': " + extraColumn);
            }
        }

        this.extraColumnsTypes = extraColumnsTypesList.toArray(new ExtraColumnType[extraColumnsTypesList.size()]);

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
                    errorPercentages[j] = 100 * Math.max(Math.abs(confidenceInterval.start - estimatedAnswer), Math.abs(confidenceInterval.end - estimatedAnswer)) / Math.abs(estimatedAnswer);
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
            case ConfidenceIntervalLower:
                return intervals[i / extraColumnsPerAggregate].start+"";
            case ConfidenceIntervalUpper:
                return intervals[i / extraColumnsPerAggregate].end+"";
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
        return 44;
    }

    public String getLabel(int column) {
        column = column - originalCount - 1;
        int i = column / extraColumnsPerAggregate;
        int aggregateColumn = q.getAggregates().get(i).getColumn();
        switch (extraColumnsTypes[column % extraColumnsPerAggregate]) {
            case ConfidenceIntervalLower:
                return "ci_lower_" + aggregateColumn;
            case ConfidenceIntervalUpper:
                return "ci_upper_" + aggregateColumn;
            case Error:
                return "error_" + aggregateColumn;
            case ErrorPercentage:
                return "e_percent_" + aggregateColumn;
            case Variance:
                return "var_" + aggregateColumn;
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
    ConfidenceIntervalLower,
    ConfidenceIntervalUpper,
    Error,
    ErrorPercentage,
    Variance
}
