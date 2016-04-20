package edu.umich.verdict.jdbc;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.InvalidConfigurationException;
import edu.umich.verdict.transformation.TransformedQuery;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

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
    private HashMap<String, Integer> columnLabels = new HashMap<>();

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

        for (int i = originalCount + 1; i <= getTotalCount(); i++)
            columnLabels.put(getLabel(i), i);
    }

    public void updateValues() throws SQLException {
        int baseIndex = q.getOriginalColumnsCount() + 1;
        int trials = q.getBootstrapTrials();
        for (int j = 0; j < aggregatesCount; j++) {
            TransformedQuery.AggregateInfo aggr = q.getAggregates().get(j);
            Double[] bootstrapResults = new Double[trials];
            for (int i = 0; i < trials; i++)
                bootstrapResults[i] = originalResultSet.getDouble(i + baseIndex);
            baseIndex += trials;
            if (showIntervals || showErrors) {
                double estimatedAnswer = originalResultSet.getDouble(aggr.getColumn());
                Arrays.sort(bootstrapResults, Comparator.comparing((Double x) -> Math.abs(x - estimatedAnswer)));
                double bound = bootstrapResults[(int) Math.ceil(trials * q.getConfidence() - 1)];
                ConfidenceInterval confidenceInterval = new ConfidenceInterval(estimatedAnswer, bound);
                if (showIntervals)
                    intervals[j] = confidenceInterval;
                if (showErrors)
                    errors[j] = Math.abs(bound - estimatedAnswer);
                if (showErrorPercentages)
                    errorPercentages[j] = 100 * Math.abs(bound - estimatedAnswer) / estimatedAnswer;
            }
            if (showVariances) {
                variances[j] = getVariance(bootstrapResults);
            }
        }
        areValuesValid = true;
    }

    double getMean(Double[] data) {
        double sum = 0.0;
        for (double a : data)
            sum += a;
        return sum / data.length;
    }

    double getVariance(Double[] data) {
        double mean = getMean(data);
        double temp = 0;
        for (double a : data)
            temp += (mean - a) * (mean - a);
        return temp / data.length;
    }

    private double getValue(int i) {
        switch (extraColumnsTypes[i % extraColumnsPerAggregate]) {
            case ConfidenceIntervalLower:
                return intervals[i / extraColumnsPerAggregate].start;
            case ConfidenceIntervalUpper:
                return intervals[i / extraColumnsPerAggregate].end;
            case Error:
                return errors[i / extraColumnsPerAggregate];
            case ErrorPercentage:
                return errorPercentages[i / extraColumnsPerAggregate];
            case Variance:
                return variances[i / extraColumnsPerAggregate];
            default:
                return 0;
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
        return 22;
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
        return "DOUBLE";
    }

    public String getClassName(int column) {
        //TODO: complete
        return String.class.getName();
    }

    public double get(int column) throws SQLException {
        //TODO: complete
        if (!areValuesValid)
            updateValues();
        return getValue(column - originalCount - 1);
    }

    public void invalidateValues() {
        areValuesValid = false;
    }

    public int findColumn(String columnLabel) {
        return columnLabels.getOrDefault(columnLabel, -1);
    }
}


class ConfidenceInterval {
    double start, end;

    ConfidenceInterval(double estimatedAnswer, double bound) {
        start = estimatedAnswer - Math.abs(estimatedAnswer - bound);
        end = estimatedAnswer + Math.abs(estimatedAnswer - bound);
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
