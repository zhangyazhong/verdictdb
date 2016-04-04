package edu.umich.verdict.transformation;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.processing.SelectStatement;

import java.util.Random;

public class UdaTransformer extends QueryTransformer {
    Random rnd = new Random();

    public UdaTransformer(Configuration conf, MetaDataManager metaDataManager, SelectStatement q) {
        super(conf, metaDataManager, q);
    }

    @Override
    protected String getTrialExpression(SelectListItem item, int trial) {
        return getUda(item) + "(" + getRandomSeed() + ", " + item.getInnerExpression() + ")";
    }

    private int getRandomSeed() {
        return rnd.nextInt();
    }

    private String getUda(SelectListItem item) {
        return "verdict.poisson_" + item.getAggregateType().toString().toLowerCase();
    }

}
