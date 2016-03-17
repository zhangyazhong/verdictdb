package edu.umich.verdict.transformation;

import edu.umich.verdict.parser.HplsqlLexer;
import edu.umich.verdict.parser.HplsqlParser;
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
        }catch (ParseCancellationException e){
            return new ParsedStatement(q, null, rewriter);
        }
//        Class<ParsedStatement> cls = findClass(tree);
//        return cls.getConstructor(String.class, ParseTree.class, TokenStreamRewriter.class).newInstance(q, tree, rewriter);
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
}
