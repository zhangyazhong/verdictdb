package edu.umich.verdict.transformation;

import edu.umich.verdict.parser.TsqlLexer;
import edu.umich.verdict.parser.TsqlParser;
import edu.umich.verdict.processing.*;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

public class Parser {
    public static ParsedStatement parse(String q) throws Exception {
        ANTLRInputStream stream = new ANTLRInputStream(q);
        TsqlLexer lexer = new TsqlLexer(stream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        TsqlParser parser = new TsqlParser(tokens);
        TokenStreamRewriter rewriter = new WSTokenStreamRewriter(tokens);
        parser.setErrorHandler(new BailErrorStrategy());
        parser.removeErrorListeners();
        ParseTree tree;
        try {
            tree = parser.select_statement();
            return new SelectStatement(q, tree, rewriter);
        } catch (ParseCancellationException e) {
            try {
                tree = parser.verdict_statement().getChild(0);
                Class<ParsedStatement> cls = findClass(tree);
                return cls.getConstructor(String.class, ParseTree.class, TokenStreamRewriter.class).newInstance(q, tree, rewriter);
            } catch (ParseCancellationException e1) {
                return new ParsedStatement(q, null, rewriter);
            }
        }
    }

    private static Class findClass(ParseTree tree) {
        Class c = tree.getClass();
        if (c == TsqlParser.Show_samples_statementContext.class)
            return ShowSamplesStatement.class;
        else if (c == TsqlParser.Create_sample_statementContext.class)
            return CreateSampleStatement.class;
        else if (c == TsqlParser.Delete_sample_statementContext.class)
            return DeleteSampleStatement.class;
        else if (c == TsqlParser.Config_statementContext.class)
            return ConfigStatement.class;
        return ParsedStatement.class;
    }
}
