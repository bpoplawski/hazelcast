/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.logical.rel;

import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class MapScanLogicalRel extends TableScan implements LogicalRel {
    /** Projection. */
    private final List<Integer> projects;

    /** Filter. */
    private final RexNode filter;

    public MapScanLogicalRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        List<Integer> projects,
        RexNode filter
    ) {
        super(cluster, traitSet, table);

        assert projects != null;

        this.projects = projects;
        this.filter = filter;
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MapScanLogicalRel(getCluster(), traitSet, table, projects, filter);
    }

    @Override
    public final RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("projects", projects, !projects.isEmpty())
            .itemIf("filter", filter, filter != null);
    }

    @Override
    public final RelDataType deriveRowType() {
        RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        List<RelDataTypeField> fieldList = table.getRowType().getFieldList();

        for (int project : projects) {
            builder.add(fieldList.get(project));
        }

        return builder.build();
    }

    public List<Integer> getProjects() {
        return projects;
    }

    public RexNode getFilter() {
        return filter;
    }

    public static boolean isProjectableFilterable(TableScan scan) {
        return scan.getTable().unwrap(HazelcastTable.class) != null;
    }
}