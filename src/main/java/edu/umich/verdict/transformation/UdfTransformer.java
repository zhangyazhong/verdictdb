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
                return "sum((" + item.getInnerExpression() + ") * verdict.poisson(" + item.getIndex() + "))/sum(verdict.poisson(" + item.getIndex() + "))";
            case SUM:
                return "sum((" + item.getInnerExpression() + ") * verdict.poisson(" + item.getIndex() + "))";
            case COUNT:
                return "sum(verdict.poisson(" + item.getIndex() + "))";
            default:
                return null;
        }
    }
}
