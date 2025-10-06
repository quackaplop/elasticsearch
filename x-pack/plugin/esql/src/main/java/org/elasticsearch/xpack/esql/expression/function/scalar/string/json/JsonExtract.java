/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string.json;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;

public class JsonExtract extends BinaryScalarFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "JsonExtract",
        JsonExtract::new
    );

    @FunctionInfo(
        returnType = "keyword",
        description = "Retrieve a specified element out of a Json string using a path expression.",
        examples = @Example(file = "string", tag = "json_extract")
    )
    public JsonExtract(
        Source source,
        @Param(
            name = "json_string",
            type = { "keyword", "text" },
            description = "Json string. If `null`, the function returns `null`."
        ) Expression jsonString,
        @Param(
            name = "json_path",
            type = { "keyword", "text" },
            description = "Json path expression. If `null`, the function returns `null`."
        ) Expression jsonPath
    ) {
        super(source, jsonString, jsonPath);
    }

    private JsonExtract(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Evaluator(extraName = "FixedPath")
    static void process(BytesRefBlock.Builder builder, BytesRef jsonString, @Fixed String jsonPath) {
        builder.appendBytesRef(jsonString);
    }

    @Evaluator(extraName = "VariablePath")
    static void process(BytesRefBlock.Builder builder, BytesRef jsonString, BytesRef jsonPath) {
        process(builder, jsonString, jsonPath);
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newLeft, Expression newRight) {
        return new JsonExtract(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, JsonExtract::new, left(), right());
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var jsonStringEvaluator = toEvaluator.apply(jsonString());
        // If json path is a constant, use a specialized evaluator to avoid re-evaluating the path for each row
        if (jsonPath().foldable()) {
            BytesRef jsonPathFixed = (BytesRef) jsonPath().fold(toEvaluator.foldCtx());
            return new JsonExtractFixedPathEvaluator.Factory(source(), jsonStringEvaluator, jsonPathFixed.utf8ToString());
        } else {
            return new JsonExtractVariablePathEvaluator.Factory(source(), jsonStringEvaluator, toEvaluator.apply(jsonPath()));
        }
    }

    Expression jsonString() {
        return left();
    }

    Expression jsonPath() {
        return right();
    }
}
