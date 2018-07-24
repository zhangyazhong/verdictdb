/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.query;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.relation.ApproxRelation;
import edu.umich.verdict.relation.ExactRelation;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

import java.util.Map;

public class SelectQuery extends Query {

    public SelectQuery(VerdictContext vc, String queryString) {
        super(vc, queryString);
    }

    private Relation queryToRelation(VerdictContext vc, String sql) throws VerdictException {
        ExactRelation r = ExactRelation.from(vc, sql);

        VerdictSQLParser p = StringManipulations.parserOf(sql);
        VerdictSQLBaseVisitor<Boolean> visitor = new VerdictSQLBaseVisitor<Boolean>() {
            @Override
            public Boolean visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
                return (ctx.EXACT() != null) ? true : false;
            }
        };
        Boolean exact = visitor.visit(p.select_statement());

        // DEBUG: zyz added
        VerdictLogger.debug(this, "exact: " + exact);

        if (exact) {
            return r;
        } else {
            Map<TableUniqueName, SampleParam> forcedSamples = VerdictConf.getSamplesToUse();

            // DEBUG: zyz added
            if (forcedSamples == null) {
                VerdictLogger.debug(this, "forcedSamples is null");
            } else {
                forcedSamples.forEach((name, sample) -> VerdictLogger.debug(this, "(name, sample): " + name + " | " + sample.toString()));
            }

            ApproxRelation a = (forcedSamples == null) ? r.approx() : r.approxWith(forcedSamples);
            return a;
        }
    }

    @Override
    public void compute() throws VerdictException {
        super.compute();
        Relation r = queryToRelation(vc, queryString);
        setResultsFromRelation(r);
    }

}
