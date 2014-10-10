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

import io.crate.analyze.where.WhereClause;
import io.crate.core.collections.StringObjectMaps;
import io.crate.exceptions.ColumnValidationException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.relation.AnalyzedRelation;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.RelationSymbol;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.Update;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Inject;

import java.util.Map;

public class UpdateStatementAnalyzer extends AbstractStatementAnalyzer<Symbol, UpdateAnalysis> {

    final DataStatementAnalyzer<UpdateAnalysis.NestedAnalysis> innerAnalyzer =
            new DataStatementAnalyzer<UpdateAnalysis.NestedAnalysis>() {

                @Override
                public Symbol visitUpdate(Update node, UpdateAnalysis.NestedAnalysis context) {
                    Symbol relationSymbol = process(node.relation(), context);
                    assert relationSymbol instanceof RelationSymbol;
                    AnalyzedRelation relation = ((RelationSymbol) relationSymbol).relation();
                    context.relation(relation);
                    for (Assignment assignment : node.assignements()) {
                        process(assignment, context);
                    }
                    if (node.whereClause().isPresent()) {
                        Symbol query = process(node.whereClause().get(), context);
                        context.whereClause(new WhereClause(context.normalizer.normalize(query)));
                    } else {
                        context.whereClause(WhereClause.MATCH_ALL);
                    }
                    return null;
                }

                @Override
                public Analysis newAnalysis(Analyzer.ParameterContext parameterContext) {
                    return new UpdateAnalysis.NestedAnalysis(
                        referenceInfos, functions, parameterContext, globalReferenceResolver);
                }

                @Override
                public Symbol visitAssignment(Assignment node, UpdateAnalysis.NestedAnalysis context) {
                    // unknown columns in strict objects handled in here
                    Reference reference = (Reference)process(node.columnName(), context);
                    final ColumnIdent ident = reference.info().ident().columnIdent();
                    if (ident.name().startsWith("_")) {
                        throw new IllegalArgumentException("Updating system columns is not allowed");
                    }

                    TableInfo tableInfo = context.tableInfo();
                    ColumnIdent clusteredBy = tableInfo.clusteredBy();
                    if (clusteredBy != null && clusteredBy.equals(ident)) {
                        throw new IllegalArgumentException("Updating a clustered-by column is currently not supported");
                    }

                    if (AbstractDataAnalysis.hasMatchingParent(tableInfo,
                            reference.info(), UpdateAnalysis.NestedAnalysis.HAS_OBJECT_ARRAY_PARENT)) {
                        // cannot update fields of object arrays
                        throw new IllegalArgumentException("Updating fields of object arrays is not supported");
                    }

                    // it's something that we can normalize to a literal
                    Symbol value = process(node.expression(), context);
                    Literal updateValue;
                    try {
                        updateValue = context.normalizeInputForReference(value, reference, true);
                    } catch(IllegalArgumentException|UnsupportedOperationException e) {
                        throw new ColumnValidationException(ident.fqn(), e);
                    }

                    for (ColumnIdent pkIdent : tableInfo.primaryKey()) {
                        ensureNotUpdated(ident, updateValue, pkIdent, "Updating a primary key is not supported");
                    }
                    for (ColumnIdent partitionIdent : tableInfo.partitionedBy()) {
                        ensureNotUpdated(ident, updateValue, partitionIdent, "Updating a partitioned-by column is not supported");
                    }

                    context.addAssignment(reference, updateValue);
                    return null;
                }

                @Override
                protected Symbol visitQualifiedNameReference(QualifiedNameReference node, UpdateAnalysis.NestedAnalysis context) {
                    return context.allocationContext().resolveReferenceForWrite(node.getName());
                }

                private void ensureNotUpdated(ColumnIdent columnUpdated,
                                              Literal newValue,
                                              ColumnIdent protectedColumnIdent,
                                              String errorMessage) {
                    if (columnUpdated.equals(protectedColumnIdent)) {
                        throw new IllegalArgumentException(errorMessage);
                    }

                    if (columnUpdated.isChildOf(protectedColumnIdent) &&
                            !(newValue.valueType().equals(DataTypes.OBJECT)
                                    && StringObjectMaps.fromMapByPath((Map) newValue.value(), protectedColumnIdent.path()) == null)) {
                        throw new IllegalArgumentException(errorMessage);
                    }
                }
    };
    private final ReferenceInfos referenceInfos;
    private final Functions functions;
    private final ReferenceResolver globalReferenceResolver;

    @Inject
    public UpdateStatementAnalyzer(ReferenceInfos referenceInfos,
                                   Functions functions,
                                   ReferenceResolver globalReferenceResolver) {
        this.referenceInfos = referenceInfos;
        this.functions = functions;
        this.globalReferenceResolver = globalReferenceResolver;
    }

    @Override
    public Symbol visitUpdate(Update node, UpdateAnalysis context) {
        java.util.List<UpdateAnalysis.NestedAnalysis> nestedAnalysisList = context.nestedAnalysisList;
        for (int i = 0, nestedAnalysisListSize = nestedAnalysisList.size(); i < nestedAnalysisListSize; i++) {
            UpdateAnalysis.NestedAnalysis nestedAnalysis = nestedAnalysisList.get(i);
            context.parameterContext().setBulkIdx(i);
            innerAnalyzer.process(node, nestedAnalysis);
        }
        return null;
    }

    @Override
    protected Symbol visitQualifiedNameReference(QualifiedNameReference node, UpdateAnalysis context) {
        return context.allocationContext().resolveReferenceForWrite(node.getName());
    }


    @Override
    public Analysis newAnalysis(Analyzer.ParameterContext parameterContext) {
        return new UpdateAnalysis(referenceInfos, functions, parameterContext, globalReferenceResolver);
    }
}
