package edu.umich.verdict.processing;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.transformation.QueryTransformer;
import edu.umich.verdict.transformation.WSTokenStreamRewriter;
import edu.umich.verdict.parser.HplsqlLexer;
import edu.umich.verdict.parser.HplsqlParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ParsedStatement {
    protected String str;
    protected ParseTree tree;
    protected TokenStreamRewriter rewriter;

    public static ParsedStatement fromString(String q) throws Exception {
        ANTLRInputStream stream = new ANTLRInputStream(q);
        HplsqlLexer lexer = new HplsqlLexer(stream);
        org.antlr.v4.runtime.CommonTokenStream tokens = new org.antlr.v4.runtime.CommonTokenStream(lexer);
        HplsqlParser parser = new HplsqlParser(tokens);
        TokenStreamRewriter rewriter = new WSTokenStreamRewriter(tokens);
        parser.setErrorHandler(new BailErrorStrategy());
        ParseTree tree = parser.single_block_stmt();
        Class<ParsedStatement> cls = findClass(tree);
        return cls.getConstructor(String.class, ParseTree.class, TokenStreamRewriter.class).newInstance(q, tree, rewriter);
    }

    private static Class findClass(ParseTree tree) {
        if (tree.getChildCount() > 0) {
            ParseTree ch = tree.getChild(0);
            if (ch.getChildCount() > 0) {
                Class c = ch.getChild(0).getClass();
                if (c == HplsqlParser.Select_stmtContext.class)
                    return SelectStatement.class;
                else if (c == HplsqlParser.Show_samples_stmtContext.class)
                    return ShowSamplesStatement.class;
                else if (c == HplsqlParser.Create_sample_stmtContext.class)
                    return CreateSampleStatement.class;
                else if (c == HplsqlParser.Delete_sample_stmtContext.class)
                    return DeleteSampleStatement.class;
                else if (c == HplsqlParser.Set_configContext.class)
                    return SetConfigStatement.class;
                return ParsedStatement.class;
            }
        }
        return ParsedStatement.class;
    }

    public ParsedStatement(String str, ParseTree tree, TokenStreamRewriter rewriter){
        this.str = str;
        this.tree = tree;
        this.rewriter = rewriter;
    }

    //TODO: use customized exceptions
    public ResultSet run(Configuration conf, DbConnector connector) throws Exception {
        ResultSet rs;
        try {
            rs = connector.executeQuery(this.toString());
        } catch (SQLException e) {
            System.err.println("Error in executing query:");
            throw e;
        }
        return rs;
    }

    //TODO: use a logger
    protected void info(String msg) {
//        if (!q)
//            return;
        System.out.println(msg);
    }

    public ParseTree getParseTree(){
        return tree;
    }

    public TokenStreamRewriter getRewriter(){
        return rewriter;
    }

    public String toString() {
        return str.replaceAll("(;\\s*)+$", "");
    }
}
