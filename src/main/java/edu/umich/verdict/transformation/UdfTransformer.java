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
        switch (item.getAggregateType()) {
            case AVG:
                return "sum((" + item.getInnerExpression() + ") * verdict.poisson(" + trial + "))/sum(verdict.poisson(" + trial + "))";
            case SUM:
                return "sum((" + item.getInnerExpression() + ") * verdict.poisson(" + trial + "))";
            case COUNT:
                return "sum((" + item.getInnerExpression() + ")/(" + item.getInnerExpression() + ") * verdict.poisson(" + trial + "))";
            default:
                return null;
        }
    }

    @Override
    protected String getStratifiedTrialExpression(SelectListItem item, int trial) {
        String weightColumn = sampleAlias + metaDataManager.getWeightColumn();
        switch (item.getAggregateType()) {
            case AVG:
                return "sum((" + item.getInnerExpression() + ") * verdict.poisson(" + trial + ") * " + weightColumn + ")/sum(verdict.poisson(" + trial + ") * " + weightColumn + ")";
            case SUM:
                return "sum((" + item.getInnerExpression() + ") * verdict.poisson(" + trial + ") * " + weightColumn + ")";
            case COUNT:
                return "sum((" + item.getInnerExpression() + ")/(" + item.getInnerExpression() + ") * verdict.poisson(" + trial + ") * " + weightColumn + ")";
            default:
                return null;
        }
    }
}
