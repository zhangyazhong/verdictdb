package edu.umich.verdict.transformation;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.models.Sample;
import edu.umich.verdict.processing.SelectStatement;
import edu.umich.verdict.parser.HplsqlBaseVisitor;
import edu.umich.verdict.parser.HplsqlParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

//TODO: clean and separate 3 methods
public class QueryTransformer {
    static final List<String> supportedAggregates = Arrays.asList("avg sum count".split(" "));
    private final MetaDataManager metaDataManager;
    private final TokenStreamRewriter rewriter;
    private SelectStatement q;
    private TransformedQuery transformed;
    private final int bootstrapRepeats;
    private final double confidence;
    private final boolean useSamples;
    private final double preferredSample;
    //TODO: do we need this?
    private final boolean useConfIntUdf = true;
    private final String method;
    private final String sampleType;
    
    boolean seenSelectList = false, seenSelect = false;
    ArrayList<SelectListItem> selectItems = new ArrayList<SelectListItem>();
    HplsqlParser.Select_listContext innerSelectList;
    HplsqlParser.Group_by_clauseContext groupBy = null;
    HashMap<String, String> selectExpMap = new HashMap<>();

    public QueryTransformer(Configuration conf, MetaDataManager metaDataManager, SelectStatement q) {
        this.q = q;
        this.metaDataManager = metaDataManager;
        rewriter = q.getRewriter();
        useSamples = conf.getBoolean("bootstrap");
        confidence = conf.getPercent("bootstrap.confidence");
        bootstrapRepeats = conf.getInt("bootstrap.trials");
        preferredSample = conf.getPercent("bootstrap.sample-size");
        method = conf.get("bootstrap.method").toLowerCase();
        sampleType = conf.get("bootstrap.sample-type").toLowerCase();
        transformed = new TransformedQuery(q, bootstrapRepeats, confidence, method);
    }

    private boolean replaceTableNames() {
        q.getParseTree().accept(new HplsqlBaseVisitor<Void>() {
            public Void visitTable_name(HplsqlParser.Table_nameContext ctx) {
                if (transformed.getSample() != null)
                    return null;
                HplsqlParser.IdentContext node = ctx.ident();
                String tableName = ctx.ident().getText();
                Sample sample = getSample(tableName);
                if (sample != null) {
                    TerminalNodeImpl lch = (TerminalNodeImpl) node.getChild(0);
                    rewriter.replace(lch.getSymbol(), sample.name);
                    transformed.setSample(sample);
                    if (method.equals("stored") && sample.poissonColumns < bootstrapRepeats)
                        System.err.println("WARNING: The best matched sample has just " + sample.poissonColumns + " Poisson columns, which is " + (bootstrapRepeats - sample.poissonColumns) + " less than needed. Using ON-THE-FLY method for the missing columns.");
                }
                return null;
            }
        });
        return transformed.getSample() != null;
    }

    private boolean replaceSelectList() {
        q.getParseTree().accept(new HplsqlBaseVisitor<Void>() {
            public Void visitSelect_list(HplsqlParser.Select_listContext list) {
                if (seenSelectList)
                    return null;
                seenSelectList = true;

                innerSelectList = list;
                transformed.setOriginalCols(list.select_list_item().size());
                int samplePoissonCols = method.equals("stored") ? transformed.getSample().poissonColumns : 0;

                for (HplsqlParser.Select_list_itemContext item : list.select_list_item()) {
                    SelectListItem itemInfo;
                    try {
                        itemInfo = new SelectListItem(selectItems.size() + 1, item);
                    } catch (Exception e) {
                        transformed.getAggregates().clear();
                        return null;
                    }
                    selectItems.add(itemInfo);
                    selectExpMap.put(itemInfo.expr, itemInfo.getInnerAlias());
                    rewriter.replace(item.start, item.stop, itemInfo.getInnerSql());
                    if (itemInfo.isSupportedAggr) {
                        if (!method.equals("uda") && transformed.getAggregates().isEmpty()) {
                            StringBuilder buf = new StringBuilder();
                            for (int i = 1; i <= samplePoissonCols && i <= bootstrapRepeats; i++)
                                buf.append(", `__p").append(i).append("` ");
                            for (int i = samplePoissonCols + 1; i <= bootstrapRepeats; i++)
                                buf.append(", verdict.poisson() as `__p").append(i).append("` ");
                            rewriter.insertAfter(list.stop, buf.toString());
                        }
                        transformed.addAggregate(itemInfo.aggregateType, itemInfo.expr, itemInfo.index);
                    }
                }
                if (transformed.getSample().stratified)
                    rewriter.insertAfter(list.stop, "," + transformed.getSample().getStrataColsStr() + " ");
                return null;
            }
        });
        return transformed.isChanged();
    }

    private void replaceGroupBy(HplsqlParser.Group_by_clauseContext gb) {
        if (gb == null)
            return;
        if (transformed.getSample().stratified) {
            System.err.println("Sorry, we do not support GROUP BY on stratified samples yet.");
            transformed.getAggregates().clear();
            return;
        }
        for (HplsqlParser.ExprContext exp : gb.expr()) {
            replaceExpWithAlias(exp);
        }
    }

    private void replaceExpWithAlias(HplsqlParser.ExprContext exp) {
        String s = exp.getText();
        String alias = selectExpMap.containsKey(s) ? selectExpMap.get(s) : null;
        if (alias == null) {
            SelectListItem item = new SelectListItem(selectItems.size() + 1, s);
            selectItems.add(item);
            rewriter.insertAfter(innerSelectList.stop, "," + item.getInnerSql() + " ");
            alias = item.getInnerAlias();
            selectExpMap.put(s, alias);
        }
        rewriter.replace(exp.start, exp.stop, alias);
    }

    private boolean addSelectWrapper() {
        q.getParseTree().accept(new HplsqlBaseVisitor<Void>() {
            public Void visitSubselect_stmt(HplsqlParser.Subselect_stmtContext select) {
                if (seenSelect)
                    return null;
                seenSelect = true;

                Sample sample = transformed.getSample();
                StringBuilder buf = new StringBuilder();
                if (sample.stratified) {
                    buf.append(" select ");
                    for (SelectListItem item : selectItems)
                        buf.append(item.getStratifiedOuterSql()).append(",");
                    for (SelectListItem item : selectItems)
                        if (item.isSupportedAggr)
                            buf.append(item.getStratifiedPoissonList());
                    buf.delete(buf.length() - 1, buf.length());
                    buf.append(" from ( select ");
                    for (SelectListItem item : selectItems)
                        buf.append(item.getStratifiedInnerSql()).append(",");
                    for (SelectListItem item : selectItems)
                        if (item.isSupportedAggr)
                            buf.append(item.getStratifiedInnerPoissonList());
                    buf.append("stratified_res.").append(sample.getStrataColsStr().replaceAll(",", ",stratified_res."));
                    buf.append(" from ( ");

                }
                buf.append(" select ");
                for (SelectListItem item : selectItems)
                    buf.append(item.getOuterSql()).append(",");
                for (SelectListItem item : selectItems)
                    if (item.isSupportedAggr)
                        buf.append(item.getPoissonList());
                if (sample.stratified)
                    buf.append(sample.getStrataColsStr()).append(",");
                buf.delete(buf.length() - 1, buf.length());
                buf.append(" from (");
                rewriter.insertBefore(select.start, buf.toString());
                Object afterWhere = null;
                for (ParseTree pt : select.children)
                    if (pt instanceof HplsqlParser.Where_clauseContext)
                        afterWhere = pt;
                if (afterWhere == null) {
                    for (ParseTree pt : select.children)
                        if (pt instanceof HplsqlParser.From_clauseContext)
                            afterWhere = ((HplsqlParser.From_clauseContext) pt).stop;
                } else
                    afterWhere = ((HplsqlParser.Where_clauseContext) afterWhere).stop;
                buf = new StringBuilder();
                if (sample.stratified) {
                    buf.append(") v__select group by ").append(sample.getStrataColsStr())
                            .append(") as stratified_res join ").append(sample.getWeightsTable()).append(" as v__weights ").append(" on ")
                            .append(sample.getJoinCond("stratified_res", "v__weights"));
                }
                buf.append(") v__innerQ ");
                rewriter.insertAfter((Token) afterWhere, buf.toString());
                replaceGroupBy(select.group_by_clause());
                return null;
            }
        });
        return transformed.isChanged();
    }

    public TransformedQuery transform() {
        boolean changed = useSamples && replaceTableNames() && replaceSelectList() && addSelectWrapper();
        return transformed;
    }

    class SelectListItem {

        int index;
        String expr;
        String aggr;
        TransformedQuery.AggregateType aggregateType = TransformedQuery.AggregateType.NONE;
        String alias;
        boolean isSupportedAggr = false;

        SelectListItem(int index, String expr) {
            this.index = index;
            this.expr = expr;
        }

        public SelectListItem(int index, HplsqlParser.Select_list_itemContext item) throws Exception {
            if (item.select_list_asterisk() != null)
                throw new Exception("Not Supported");
            this.index = index;
            this.expr = item.expr().getText();
            if (item.select_list_alias() != null && item.select_list_alias().ident() != null)
                alias = item.select_list_alias().ident().getText();
            else
                alias = expr;
            if (item.expr().expr_agg_window_func() != null) {
                expr = item.expr().expr_agg_window_func().getChild(2).getText();
                aggr = item.expr().expr_agg_window_func().getChild(0).getText();
                if (expr.equals("*"))
                    expr = "1";
                if (supportedAggregates.contains(aggr.toLowerCase())) {
                    isSupportedAggr = true;
                    aggregateType = TransformedQuery.AggregateType.valueOf(aggr.toUpperCase());
                } else
                    aggregateType = TransformedQuery.AggregateType.OTHER;
            }
        }

        public String getInnerAlias() {
            return "`__c" + index + "`";
        }

        public String getInnerSql() {
            return "(" + expr + ") as " + getInnerAlias();
        }

        public String getAlias() {
            if (alias == null) {
                if (aggregateType == TransformedQuery.AggregateType.NONE)
                    return this.expr;
                else

                    return aggr + "(" + this.expr + ")";
            } else
                return this.alias;
        }

        public String getOuterAlias() {
            if (transformed.getSample().stratified)
                return "__a" + index;
            return getAlias();
        }

        public String getStratifiedOuterAlias() {
            if (transformed.getSample().stratified)
                return "__b" + index;
            return getAlias();
        }

        public String getOuterSql() {
            String expr = getInnerAlias();
            if (aggregateType == TransformedQuery.AggregateType.NONE)
                return "(" + expr + ") as `" + this.getOuterAlias() + "`";
            else
                return getScale() + aggr + "(" + expr + ") as `" + this.getOuterAlias() + "`";
        }

        public String getStratifiedOuterSql() {
            String expr = getStratifiedOuterAlias();
            if (aggregateType == TransformedQuery.AggregateType.NONE)
                return "(`" + expr + "`) as `" + this.getAlias() + "`";
            else
                return "sum(`" + expr + "`) as `" + this.getAlias() + "`";
        }

        public String getStratifiedPoissonList() {
            if (!isSupportedAggr)
                return "";
            StringBuilder buf = new StringBuilder();
            for (int i = 1; i <= bootstrapRepeats; i++)
                buf.append("sum(`").append(getStratifiedOuterAlias()).append("_").append(i).append("`),");
            if (useConfIntUdf) {
                buf.insert(0, "verdict.conf_int(" + confidence + ", ");
                buf.delete(buf.length() - 1, buf.length());
                buf.append(") as `").append(getAlias()).append(" Conf. Int.`,");
            }
            return buf.toString();
        }

        private String getScale() {
            switch (aggregateType) {
                case AVG:
                    return "";
                case SUM:
                    return transformed.getSample().stratified ? "" : 1 / transformed.getSample().compRatio + "*";
                case COUNT:
                    return transformed.getSample().stratified ? "" : 1 / transformed.getSample().compRatio + "*";
                default:
                    return "";
            }
        }

        private String getStratifiedScale() {
            switch (aggregateType) {
                case AVG:
                    return "v__weights.weight*";
                case SUM:
                case COUNT:
                    return "v__weights.ratio*";
                default:
                    return "";
            }
        }

        public String getPoissonList() {
            if (!isSupportedAggr)
                return "";
            StringBuilder buf = new StringBuilder();
            for (int i = 1; i <= bootstrapRepeats; i++) {
                if (method.equals("uda"))
                    switch (aggregateType) {
                        case AVG:
                            buf.append(getScale()).append("cast(verdict.my_avg(").append(i).append(",").append(getInnerAlias()).append(") as double)");
                            break;
                        case SUM:
                            buf.append(getScale()).append("verdict.my_sum(").append(i).append(",").append(getInnerAlias()).append(")");
                            break;
                        case COUNT:
                            buf.append(getScale()).append("verdict.my_count(").append(i).append(")");
                            break;
                    }
                else
                    switch (aggregateType) {
                        case AVG:
                            buf.append(getScale()).append("sum(").append(getInnerAlias()).append("*").append("`__p").append(i).append("`)/count(1)");
                            break;
                        case SUM:
                            buf.append(getScale()).append("sum(").append(getInnerAlias()).append("*").append("`__p").append(i).append("`)");
                            break;
                        case COUNT:
                            buf.append(getScale()).append("sum(").append("`__p").append(i).append("`)");
                            break;
                    }
                if (!useConfIntUdf || transformed.getSample().stratified)
                    buf.append(" as `__a").append(index).append("_").append(i).append("`");
                buf.append(",");
            }
            if (useConfIntUdf && !transformed.getSample().stratified) {
                buf.insert(0, "verdict.conf_int(" + confidence + ", ");
                buf.delete(buf.length() - 1, buf.length());
                buf.append(") as `").append(getAlias()).append(" Conf. Int.`,");
            }
            return buf.toString();
        }

        public String getStratifiedInnerSql() {
            return getStratifiedScale() + "`"+getOuterAlias() + "` as `" + getStratifiedOuterAlias() + "`";
        }

        public String getStratifiedInnerPoissonList() {
            if (!isSupportedAggr)
                return "";
            StringBuilder buf = new StringBuilder();
            for (int i = 1; i <= bootstrapRepeats; i++)
                buf.append(getStratifiedScale()).append("`").append(getOuterAlias()).append("_").append(i).append("` as `").append(getStratifiedOuterAlias()).append("_").append(i).append("`,");
            return buf.toString();
        }
    }

    private Sample getSample(String tableName) {
        double min = 1000;
        Sample best = null;
        for (Sample s : metaDataManager.getTableSamples(tableName)) {
            if ((s.stratified && sampleType.equals("uniform")) || (!s.stratified && sampleType.equals("stratified")))
                continue;
            double diff = Math.abs(s.compRatio - preferredSample);
            if ((min > preferredSample * .2 && diff < min) || (diff <= preferredSample * .2 && best != null &&
                    firstIsPreferred(s, best))) {
                min = diff;
                best = s;
            }
        }
        return best;
    }

    private boolean firstIsPreferred(Sample first, Sample second) {
        if (method.equals("stored"))
            return second.poissonColumns < bootstrapRepeats && second.poissonColumns < first.poissonColumns;
        else
            return second.poissonColumns > first.poissonColumns;
    }
}