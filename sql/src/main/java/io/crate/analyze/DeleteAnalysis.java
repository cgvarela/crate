/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.TableIdent;
import io.crate.metadata.relation.AnalyzedRelation;
import io.crate.metadata.table.TableInfo;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class DeleteAnalysis extends Analysis {

    private static final Predicate<NestedDeleteAnalysis> HAS_NO_RESULT_PREDICATE = new Predicate<NestedDeleteAnalysis>() {
        @Override
        public boolean apply(@Nullable NestedDeleteAnalysis input) {
            return input != null && input.hasNoResult();
        }
    };

    List<NestedDeleteAnalysis> nestedAnalysisList;

    public DeleteAnalysis(ReferenceInfos referenceInfos,
                          Functions functions,
                          Analyzer.ParameterContext parameterContext, ReferenceResolver referenceResolver) {
        super(parameterContext);
        int numNested = 1;
        if (parameterContext.bulkParameters.length > 0) {
            numNested = parameterContext.bulkParameters.length;
        }
        nestedAnalysisList = new ArrayList<>(numNested);
        for (int i = 0; i < numNested; i++) {
            nestedAnalysisList.add(new NestedDeleteAnalysis(
                    referenceInfos,
                    functions,
                    parameterContext,
                    referenceResolver
            ));
        }
    }

    public List<NestedDeleteAnalysis> nestedAnalysis() {
        return nestedAnalysisList;
    }

    @Override
    public boolean hasNoResult() {
        return Iterables.all(nestedAnalysisList, HAS_NO_RESULT_PREDICATE);
    }

    @Override
    public void normalize() {
        for (NestedDeleteAnalysis nestedDeleteAnalysis : nestedAnalysisList) {
            nestedDeleteAnalysis.normalize();
        }
    }

    @Override
    public boolean isData() {
        return true;
    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitDeleteAnalysis(this, context);
    }

    public static class NestedDeleteAnalysis extends AbstractDataAnalysis {

        private AnalyzedRelation relation;
        private TableInfo tableInfo;

        public NestedDeleteAnalysis(ReferenceInfos referenceInfos,
                                    Functions functions,
                                    Analyzer.ParameterContext parameterContext,
                                    ReferenceResolver referenceResolver) {
            super(referenceInfos, functions, parameterContext, referenceResolver);
        }

        @Override
        public TableInfo getTableInfo(TableIdent tableIdent) {
            tableInfo = referenceInfos.getEditableTableInfoSafe(tableIdent);
            return tableInfo;
        }

        public void relation(AnalyzedRelation relation) {
            this.relation = relation;
        }

        public AnalyzedRelation relation() {
            return relation;
        }

        public TableInfo tableInfo() {
            return tableInfo;
        }

        @Override
        public void normalize() {
            relation.normalize(normalizer);
        }

        @Override
        public boolean hasNoResult() {
            // TODO;return relation.whereClause().noMatch();
            return false;
        }

        @Override
        public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
            return null;
        }
    }
}
