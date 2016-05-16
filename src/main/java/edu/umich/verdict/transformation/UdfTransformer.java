package edu.umich.verdict.transformation;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.processing.SelectStatement;

public class UdfTransformer extends QueryTransformer {
    public UdfTransformer(Configuration conf, MetaDataManager metaDataManager, SelectStatement q) {
        super(conf, metaDataManager, q);
    }

    @Override
    protected String getUniformTrialExpression(SelectListItem item, int trial) {
        String poissonUdfName = metaDataManager.getUdfFullName("poisson");
        switch (item.getAggregateType()) {
            case AVG:
                return "sum((" + item.getInnerExpression() + ") * " + poissonUdfName + "(" + trial + "))/sum(" + poissonUdfName + "(" + trial + "))";
            case SUM:
                return "sum((" + item.getInnerExpression() + ") * " + poissonUdfName + "(" + trial + "))";
            case COUNT:
                return "sum(case when (" + item.getInnerExpression() + ") is null then 0 else " + poissonUdfName + "(" + trial + ") end)";
            default:
                return null;
        }
    }

    @Override
    protected String getStratifiedTrialExpression(SelectListItem item, int trial) {
        String weightColumn = sampleAlias + "." + metaDataManager.getWeightColumn();
        String poissonUdfName = metaDataManager.getUdfFullName("poisson");
        switch (item.getAggregateType()) {
            case AVG:
                return "sum((" + item.getInnerExpression() + ") * " + poissonUdfName + "(" + trial + ") * " + weightColumn + ")/sum(" + poissonUdfName + "(" + trial + ") * " + weightColumn + ")";
            case SUM:
                return "sum((" + item.getInnerExpression() + ") * " + poissonUdfName + "(" + trial + ") * " + weightColumn + ")";
            case COUNT:
                return "sum(case when (" + item.getInnerExpression() + ") is null then 0 else " + poissonUdfName + "(" + trial + ") * " + weightColumn + " end)";
            default:
                return null;
        }
    }
}
