/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.sourcefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelationBuilder;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.List;

/**
 * A source function whose answer is known on the coordinator: it writes its columns and rows into a
 * {@link LocalRelationBuilder} and this hands back the relation they denote.
 * <p>
 * The rows are materialized while the query is planned and held until it finishes, on the coordinator, with no
 * circuit breaker charged — {@link PlannerUtils#NON_BREAKING_BLOCK_FACTORY}, as {@code ROW} uses. A function whose
 * answer is not small enough for that should read like one, which means handing back an unresolved leaf and letting
 * the ordinary pipeline finish it, rather than extending this.
 * <p>
 * Every place a call appears is asked on its own, and this builds a fresh {@link LocalRelationBuilder} per ask, so
 * each place gets a page of its own. That is what the facility requires of a function — one page cannot stand under
 * two parents — and subclasses get it without having to know: writing into the builder handed to {@link #write} is
 * the only thing they do, and there is nothing here for them to cache.
 */
public abstract class CoordinatorRowsFunction implements SourceFunction {

    /**
     * Writes this invocation's columns and rows, then completes {@code listener}. Everything written is released if
     * the listener completes with a failure instead.
     */
    protected abstract void write(
        SourceFunctionInvocation call,
        ProjectMetadata projectMetadata,
        LocalRelationBuilder rows,
        ActionListener<Void> listener
    );

    /**
     * The factory the rows are built with. Overridden only by a test, which needs a breaker that notices a block
     * a failure path forgot to release; production has nothing to charge against, the page being planning state.
     */
    BlockFactory blockFactory() {
        return PlannerUtils.NON_BREAKING_BLOCK_FACTORY;
    }

    /** For a function that answers from memory, with no listener of its own to complete. */
    protected static void computed(
        ActionListener<Void> listener,
        CheckedConsumer<LocalRelationBuilder, Exception> write,
        LocalRelationBuilder rows
    ) {
        ActionListener.completeWith(listener, () -> {
            write.accept(rows);
            return null;
        });
    }

    @Override
    public final void toRelation(
        SourceFunctionInvocation call,
        List<NamedExpression> metadataFields,
        ProjectMetadata projectMetadata,
        ActionListener<LeafPlan> listener
    ) {
        LocalRelationBuilder rows = new LocalRelationBuilder(name(), blockFactory());
        // ActionListener.wrap routes a throw from relation() to the failure arm, and run() does the same for one from write().
        ActionListener<Void> written = ActionListener.wrap(ignored -> listener.onResponse(rows.relation(call.source())), failure -> {
            rows.close();
            listener.onFailure(failure);
        });
        ActionListener.run(written, l -> write(call, projectMetadata, rows, l));
    }
}
