package edu.umich.verdict.transformation;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.processing.SelectStatement;

public class UdfTransformer extends QueryTransformer {
    public UdfTransformer(Configuration conf, MetaDataManager metaDataManager, SelectStatement q) {
        super(conf, metaDataManager, q);
    }

    @Override
    protected String getTrialExpression(SelectListItem item, int trial) {
        switch (item.getAggregateType()) {
            case AVG:
                return "sum((" + item.getInnerExpression() + ") * verdict.poisson())/sum(verdict.poisson())";
            case SUM:
                return "sum((" + item.getInnerExpression() + ") * verdict.poisson())";
            case COUNT:
                return "sum(verdict.poisson())";
            default:
                return null;
        }
    }
}
