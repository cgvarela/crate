/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.IntSet;
import com.google.common.base.Preconditions;
import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.Reference;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * holding analysis results for any insert statement variant
 */
public abstract class AbstractInsertAnalysis extends AbstractDataAnalysis {

    private List<Reference> columns;
    private IntSet primaryKeyColumnIndices = new IntOpenHashSet();
    private IntSet partitionedByColumnsIndices = new IntOpenHashSet();
    private int routingColumnIndex = -1;
    private TableInfo tableInfo;
    private final List<String> ids = new ArrayList<>();
    private final List<String> routingValues = new ArrayList<>();

    public AbstractInsertAnalysis(ReferenceInfos referenceInfos,
                                  Functions functions,
                                  Analyzer.ParameterContext parameterContext,
                                  ReferenceResolver referenceResolver) {
        super(referenceInfos, functions, parameterContext, referenceResolver);
    }

    @Override
    public TableInfo getTableInfo(TableIdent tableIdent) {
        Preconditions.checkState(tableInfo == null, "inserting into multiple tables is not supported");
        tableInfo = referenceInfos.getEditableTableInfoSafe(tableIdent);
        if (tableInfo.isAlias() && !tableInfo.isPartitioned()) {
            throw new IllegalArgumentException("Table alias not allowed in INSERT statement.");
        }
        return tableInfo;
    }

    public TableInfo table() {
        return tableInfo;
    }

    public List<String> ids() {
        return ids;
    }

    public List<String> routingValues() {
        return routingValues;
    }

    public List<Reference> columns() {
        return columns;
    }

    public IntSet partitionedByIndices() {
        return partitionedByColumnsIndices;
    }

    public void addPartitionedByIndex(int i) {
        this.partitionedByColumnsIndices.add(i);
    }

    public void columns(List<Reference> columns) {
        this.columns = columns;
    }

    public IntSet primaryKeyColumnIndices() {
        return primaryKeyColumnIndices;
    }

    public void addPrimaryKeyColumnIdx(int primaryKeyColumnIdx) {
        this.primaryKeyColumnIndices.add(primaryKeyColumnIdx);
    }

    protected List<String> partitionedByColumnNames() {
        assert tableInfo != null;
        List<String> names = new ArrayList<>(tableInfo.partitionedByColumns().size());
        for (ReferenceInfo info : tableInfo.partitionedByColumns()) {
            names.add(info.ident().columnIdent().fqn());
        }
        return names;
    }

    public void routingColumnIndex(int routingColumnIndex) {
        this.routingColumnIndex = routingColumnIndex;
    }

    public int routingColumnIndex() {
        return routingColumnIndex;
    }

    /**
     *
     * @return routing column if it is used in insert statement
     */
    public @Nullable ColumnIdent routingColumn() {
        if (routingColumnIndex < 0) { return null; }
        else {
            return table().clusteredBy();
        }
    }
}
