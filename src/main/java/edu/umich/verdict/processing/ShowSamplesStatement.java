package edu.umich.verdict.processing;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.parser.TsqlParser;
import edu.umich.verdict.transformation.QueryTransformer;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ShowSamplesStatement extends VerdictStatement {
    private final String type;
    private final String table;

    public ShowSamplesStatement(String str, ParseTree tree) {
        super(str, tree);
        TsqlParser.Show_samples_statementContext ctx = (TsqlParser.Show_samples_statementContext) tree;
        type = ctx.type != null ? ctx.type.getText().toLowerCase() : "any";
        table = ctx.table != null ? ctx.table.getText().toLowerCase() : null;
    }

    @Override
    public ResultSet run(Configuration conf, DbConnector connector) throws SQLException {
        return connector.getMetaDataManager().getSamplesInfo(type, table);
    }

    public String getQuery(DbConnector connector){
        return connector.getMetaDataManager().getSamplesInfoQuery(type, table);
    }
}
