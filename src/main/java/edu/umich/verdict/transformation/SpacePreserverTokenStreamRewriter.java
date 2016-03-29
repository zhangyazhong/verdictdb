package edu.umich.verdict.transformation;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.misc.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SpacePreserverTokenStreamRewriter extends TokenStreamRewriter {
    public SpacePreserverTokenStreamRewriter(CommonTokenStream tokens) {
        super(tokens);
    }

    public String getText(String programName, Interval interval) {
        List<RewriteOperation> rewrites = this.programs.get(programName);
        int start = interval.a;
        int stop = interval.b;
        if (stop > this.tokens.size() - 1) {
            stop = this.tokens.size() - 1;
        }

        if (start < 0) {
            start = 0;
        }

        if (rewrites != null && !rewrites.isEmpty()) {
            StringBuilder buf = new StringBuilder();
            Map indexToOp = this.reduceToSingleOperationPerIndex(rewrites);
            int i = start;

            while (i <= stop && i < this.tokens.size()) {
                TokenStreamRewriter.RewriteOperation op = (TokenStreamRewriter.RewriteOperation) indexToOp.get(i);
                indexToOp.remove(i);
                Token op1 = this.tokens.get(i);
                if (op == null) {
                    if (op1.getType() != -1) {
                        String str = op1.getText();
                        buf.append(str);
                        buf.append(" ");
                    }

                    ++i;
                } else {
                    i = op.execute(buf);
                    buf.append(" ");
                }
            }

            return buf.toString();
        } else {
            StringBuilder buf = new StringBuilder();
            int i = start;

            while (i <= stop && i < this.tokens.size()) {
                Token op1 = this.tokens.get(i);
                if (op1.getType() != -1) {
                    String str = op1.getText();
                    buf.append(str);
                    buf.append(" ");
                }
                ++i;
            }
            return buf.toString();
        }
    }

}
