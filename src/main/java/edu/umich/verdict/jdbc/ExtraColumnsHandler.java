package edu.umich.verdict.jdbc;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.InvalidConfigurationException;
import edu.umich.verdict.transformation.TransformedQuery;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

class ExtraColumnsHandler {
    private final ResultSet originalResultSet;
    private final ResultSetMetaData originalMetaData;
    private final TransformedQuery q;
    private final int originalCount;
    private final int aggregatesCount;
    private boolean showIntervals = false, showErrors = false, showErrorPercentages = false, showVariances = false;
    private final ConfidenceInterval[] intervals;
    private final double[] errors, errorPercentages, variances;
    private final boolean[] isNulls;
    private boolean areValuesValid = false;
    private int extraColumnsPerAggregate;
    private final ExtraColumnType[] extraColumnsTypes;
    private HashMap<String, Integer> columnLabels = new HashMap<>();

    public ExtraColumnsHandler(ResultSet rs, TransformedQuery q, Configuration conf) throws InvalidConfigurationException, SQLException {
        this.originalResultSet = rs;
        this.q = q;
        this.originalCount = q.getOriginalColumnsCount();
        this.originalMetaData = rs.getMetaData();

        extraColumnsPerAggregate = 0;

        String[] extraColumns = conf.get("error_columns").split(",");
        ArrayList<ExtraColumnType> extraColumnsTypesList = new ArrayList<>();
        for (String extraColumn : extraColumns) {
            if (extraColumn.trim().isEmpty())
                continue;
            extraColumnsPerAggregate++;
            switch (extraColumn.trim()) {
                case "conf_inv":
                    extraColumnsTypesList.add(ExtraColumnType.ConfidenceIntervalLower);
                    extraColumnsTypesList.add(ExtraColumnType.ConfidenceIntervalUpper);
                    extraColumnsPerAggregate++;
                    showIntervals = true;
                    break;
                case "err":
                    extraColumnsTypesList.add(ExtraColumnType.Error);
                    showErrors = true;
                    break;
                case "err_percent":
                    extraColumnsTypesList.add(ExtraColumnType.ErrorPercentage);
                    showErrorPercentages = true;
                    break;
                case "variance":
                    extraColumnsTypesList.add(ExtraColumnType.Variance);
                    showVariances = true;
                    break;
                default:
                    throw new InvalidConfigurationException("Invalid value for 'error_columns': " + extraColumn);
            }
        }

        this.extraColumnsTypes = extraColumnsTypesList.toArray(new ExtraColumnType[extraColumnsTypesList.size()]);

        aggregatesCount = q.getAggregates().size();
        intervals = showIntervals ? new ConfidenceInterval[aggregatesCount] : null;
        errors = showErrors ? new double[aggregatesCount] : null;
        errorPercentages = showErrorPercentages ? new double[aggregatesCount] : null;
        variances = showVariances ? new double[aggregatesCount] : null;
        isNulls = new boolean[aggregatesCount];

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
            isNulls[j] = false;
            if (originalResultSet.getObject(aggr.getColumn()) == null)
                isNulls[j] = true;
            else {
                if (showIntervals || showErrors || showErrorPercentages) {
                    double estimatedAnswer = originalResultSet.getDouble(aggr.getColumn());
                    Arrays.sort(bootstrapResults, Comparator.comparing((Double x) -> Math.abs(x - estimatedAnswer)));
                    double bound = bootstrapResults[(int) Math.ceil(trials * q.getConfidence() - 1)];
                    ConfidenceInterval confidenceInterval = new ConfidenceInterval(estimatedAnswer, bound);
                    if (showIntervals)
                        intervals[j] = confidenceInterval;
                    if (showErrors)
                        errors[j] = Math.abs(bound - estimatedAnswer);
                    if (showErrorPercentages)
                        errorPercentages[j] = Math.round(10000 * Math.abs(bound - estimatedAnswer) / estimatedAnswer) / 100;
                }
                if (showVariances) {
                    variances[j] = getVariance(bootstrapResults);
                }
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

    private Double getValue(int i) {
        if (isNulls[i / extraColumnsPerAggregate])
            return null;
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
        return 22;
    }

    public String getLabel(int column) throws SQLException {
        column = column - originalCount - 1;
        int i = column / extraColumnsPerAggregate;
        String aggregateLabel = originalMetaData.getColumnLabel(q.getAggregates().get(i).getColumn());
        switch (extraColumnsTypes[column % extraColumnsPerAggregate]) {
            case ConfidenceIntervalLower:
                return "conf_inv_lower__" + aggregateLabel;
            case ConfidenceIntervalUpper:
                return "conf_inv_upper__" + aggregateLabel;
            case Error:
                return "err__" + aggregateLabel;
            case ErrorPercentage:
                return "err_percent__" + aggregateLabel;
            case Variance:
                return "variance__" + aggregateLabel;
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

    public Double get(int column) throws SQLException {
        //TODO: complete
        if (!areValuesValid)
            updateValues();
        return getValue(column - originalCount - 1);
    }

    public double getDouble(int column) throws SQLException {
        Double val = get(column);
        return val == null ? 0.0 : val;
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
