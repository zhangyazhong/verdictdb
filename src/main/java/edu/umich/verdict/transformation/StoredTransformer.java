package edu.umich.verdict.transformation;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.models.Sample;
import edu.umich.verdict.processing.SelectStatement;

public class StoredTransformer extends QueryTransformer {
    public StoredTransformer(Configuration conf, MetaDataManager metaDataManager, SelectStatement q) {
        super(conf, metaDataManager, q);
    }

    @Override
    protected String getUniformBootstrapTrials(SelectListItem item) {
        if (transformed.getSample().getPoissonColumns() < bootstrapTrials) {
            int requestedTrials = bootstrapTrials;
            bootstrapTrials = transformed.getSample().getPoissonColumns();
            System.err.println("WARNING: Selected sample has just " + bootstrapTrials + " Poisson number columns, however bootstrap.trials is set to " + requestedTrials + " which is more than available Poisson number columns. Performing " + bootstrapTrials + " bootstrap trials...");
        }
        return super.getUniformBootstrapTrials(item);
    }

    @Override
    protected String getUniformTrialExpression(SelectListItem item, int trial) {
        String pref = metaDataManager.getPossionColumnPrefix();
        switch (item.getAggregateType()) {
            case AVG:
                return "sum((" + item.getInnerExpression() + ") * " + pref + trial + ")/sum(" + pref + trial + ")";
            case SUM:
                return "sum((" + item.getInnerExpression() + ") * " + pref + trial + ")";
            case COUNT:
                return "sum(case when (" + item.getInnerExpression() + ") is null then 0 else " + pref + trial + " end)";
            default:
                return null;
        }
    }

    @Override
    protected String getStratifiedTrialExpression(SelectListItem item, int trial) {
        String weightColumn = sampleAlias + "." + metaDataManager.getWeightColumn();
        String pref = metaDataManager.getPossionColumnPrefix();
        switch (item.getAggregateType()) {
            case AVG:
                return "sum((" + item.getInnerExpression() + ") * " + pref + trial + " * " + weightColumn + ")/sum(" + pref + trial + " * " + weightColumn + ")";
            case SUM:
                return "sum((" + item.getInnerExpression() + ") * " + pref + trial + " * " + weightColumn + ")";
            case COUNT:
                return "sum(case when (" + item.getInnerExpression() + ") is null then 0 else " + pref + trial + " * " + weightColumn + " end)";
            default:
                return null;
        }
    }

    @Override
    protected Sample getSample(String tableName) {
        Sample sample = super.getSample(tableName);
        if (sample != null && sample.getPoissonColumns() == 0)
            return null;
        return sample;
    }

    @Override
    protected Sample getPreferred(Sample sample1, Sample sample2) {
        if (sample1.getPoissonColumns() == 0)
            return sample2;
        if (sample2.getPoissonColumns() == 0)
            return sample1;

        double diff1 = Math.max(sample1.getCompRatio() / preferredSample, preferredSample / sample1.getCompRatio());
        double diff2 = Math.max(sample2.getCompRatio() / preferredSample, preferredSample / sample2.getCompRatio());
        if (diff1 > 1.2 && diff2 < diff1)
            return sample2;
        if (diff2 > 1.2 && diff1 <= diff2)
            return sample1;
        if (sample2.getPoissonColumns() < bootstrapTrials)
            return sample2.getPoissonColumns() < sample1.getPoissonColumns() ? sample1 : sample2;
        else
            return sample2.getPoissonColumns() > sample1.getPoissonColumns() && sample1.getPoissonColumns() >= bootstrapTrials ? sample1 : sample2;
    }
}
