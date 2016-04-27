package edu.umich.verdict.transformation;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.processing.SelectStatement;

public class IdenticalTransformer extends QueryTransformer {
    public IdenticalTransformer(Configuration conf, MetaDataManager metaDataManager, SelectStatement q) {
        super(conf, metaDataManager, q);
    }

    @Override
    public TransformedQuery transform(){
        return transformed;
    }

    @Override
    protected String getUniformTrialExpression(SelectListItem item, int trial) {
        return null;
    }

    @Override
    protected String getStratifiedTrialExpression(SelectListItem item, int trial) {
        return null;
    }
}
