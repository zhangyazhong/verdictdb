package edu.umich.verdict.processing;

import org.antlr.v4.runtime.tree.ParseTree;

public abstract class VerdictStatement extends ParsedStatement {
    public VerdictStatement(String str, ParseTree tree) {
        super(str, tree);
    }
}
