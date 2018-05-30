/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.pgcatalog;

import io.crate.metadata.RelationName;
import io.crate.expression.reference.StaticTableDefinition;
import io.crate.protocols.postgres.types.PGTypes;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class PgCatalogTableDefinitions {

    private final Map<RelationName, StaticTableDefinition<?>> tableDefinitions;

    public PgCatalogTableDefinitions() {
        tableDefinitions = new HashMap<>(1);

        tableDefinitions.put(PgTypeTable.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(PGTypes.pgTypes()),
            PgTypeTable.expressions()
        ));
    }

    public StaticTableDefinition<?> get(RelationName relationName) {
        return tableDefinitions.get(relationName);
    }
}
