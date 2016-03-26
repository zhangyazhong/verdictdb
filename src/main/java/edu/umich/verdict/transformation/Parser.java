package edu.umich.verdict.transformation;

import edu.umich.verdict.parser.TsqlLexer;
import edu.umich.verdict.parser.TsqlParser;
import edu.umich.verdict.processing.*;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;

public class Parser {
    public static ParsedStatement parse(String q) throws Exception {
        ANTLRInputStream stream = new ANTLRInputStream(q);
        TsqlLexer lexer = new TsqlLexer(stream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        TsqlParser parser = new TsqlParser(tokens);
        TokenStreamRewriter rewriter = new WSTokenStreamRewriter(tokens);
        parser.setErrorHandler(new BailErrorStrategy());
        parser.removeErrorListeners();

        try {
            ParseTree tree = parser.select_statement();
            return new SelectStatement(q, tree, rewriter);
        } catch (ParseCancellationException e) {
            try {
                return tryParseVerdictStatement(q, parser);
            } catch (ParseCancellationException e1) {
                return new ParsedStatement(q, null);
            }
        }
    }

    private static ParsedStatement tryParseVerdictStatement(String q, TsqlParser parser) throws SQLException, ParseCancellationException {
        VerdictStatementErrorListener errorListener = new VerdictStatementErrorListener();
        parser.addErrorListener(errorListener);
        ParseTree tree = parser.verdict_statement().getChild(0);
        if (errorListener.errorMessages.isEmpty()) {
            Class<ParsedStatement> cls = findVerdictStatementClass(tree);
            try {
                return cls.getConstructor(String.class, ParseTree.class).newInstance(q, tree);
            } catch (ReflectiveOperationException e) {
                e.printStackTrace();
            }
        }
        throw new SQLException(errorListener.errorMessages.get(0));
    }

    private static Class findVerdictStatementClass(ParseTree tree) {
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

    private static class VerdictStatementErrorListener extends BaseErrorListener {
        ArrayList<String> errorMessages = new ArrayList<>();

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
            errorMessages.add("Error in line " + line + " at character " + charPositionInLine + ": " + msg);
        }
    }
}
