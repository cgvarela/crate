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

import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.relation.AnalyzedRelation;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.planner.symbol.DynamicReference;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class AllocationContext {

    private final ReferenceInfos referenceInfos;
    private List<AnalyzedRelation> currentRelations = null;
    private AnalyzedRelation currentRelation = null;
    public final Map<ReferenceInfo, Reference> allocatedReferences = new HashMap<>();
    public final Map<Function, Function> allocatedFunctions = new HashMap<>();

    boolean hasSysExpressions = false;

    public AllocationContext(ReferenceInfos referenceInfos) {
        this.referenceInfos = referenceInfos;
    }

    public Reference resolveReference(QualifiedName name) {
        return resolveReference(name, null, false);
    }

    public Reference resolveReferenceForWrite(QualifiedName name) {
        return resolveReference(name, null, true);
    }

    public Reference resolveReference(QualifiedName name, @Nullable List<String> path, boolean forWrite) {
        assert currentRelations != null || currentRelation != null :
                "currentRelation must be set in order to resolve a qualifiedName";

        List<String> parts = name.getParts();
        String schema = null;
        String tableOrAlias = null;
        ColumnIdent columnIdent;

        switch (parts.size()) {
            case 1:
                columnIdent = new ColumnIdent(parts.get(0), path);
                break;
            case 2:
                tableOrAlias = parts.get(0);
                columnIdent = new ColumnIdent(parts.get(1), path);
                break;
            case 3:
                schema = parts.get(0).toLowerCase(Locale.ENGLISH);

                // enables statements like : select sys.shards.id, * from blob.myblobs
                if (schema.equals(SysSchemaInfo.NAME)) {
                    hasSysExpressions = true;
                    TableIdent sysTableIdent = new TableIdent(SysSchemaInfo.NAME, parts.get(1));
                    columnIdent = new ColumnIdent(parts.get(2), path);
                    ReferenceInfo referenceInfo = referenceInfos.getTableInfoSafe(sysTableIdent).getReferenceInfo(columnIdent);
                    Reference reference = new Reference(referenceInfo);
                    allocatedReferences.put(referenceInfo, reference);
                    return reference;
                } else {
                    tableOrAlias = parts.get(1);
                    columnIdent = new ColumnIdent(parts.get(2), path);
                }
                break;
            default:
                throw new IllegalArgumentException("Column reference \"%s\" has too many parts. " +
                        "A column reference can have at most 3 parts and must have one of the following formats:  " +
                        "\"<column>\", \"<table>.<column>\" or \"<schema>.<table>.<column>\"");
        }

        Reference reference = null;
        if (currentRelations != null) {
            assert currentRelation == null : "can't have both currentRelation and currentRelations set";
            for (AnalyzedRelation currentRelation : currentRelations) {
                Reference currentRef = currentRelation.getReference(schema, tableOrAlias, columnIdent, forWrite);
                if (reference == null) {
                    reference = currentRef;
                } else if (currentRef != null
                        && !((currentRef instanceof DynamicReference) || (reference instanceof DynamicReference))) {
                    throw new AmbiguousColumnException(columnIdent.sqlFqn());
                }
            }
        } else {
            assert currentRelation != null : "must have either currentRelations or currentRelation set";
            reference = currentRelation.getReference(schema, tableOrAlias, columnIdent, forWrite);
        }
        if (reference == null) {
            throw new ColumnUnknownException(columnIdent.sqlFqn());
        }
        allocatedReferences.put(reference.info(), reference);
        return reference;
    }

    public void setRelation(AnalyzedRelation analyzedRelation) {
        currentRelation = analyzedRelation;
        currentRelations = null;
    }

    public void setRelation(List<AnalyzedRelation> analyzedRelations) {
        currentRelation = null;
        currentRelations = analyzedRelations;
    }
}
