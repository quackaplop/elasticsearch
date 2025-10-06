// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.string.json;

import java.lang.IllegalArgumentException;
import java.lang.Override;
import java.lang.String;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link EvalOperator.ExpressionEvaluator} implementation for {@link JsonExtract}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class JsonExtractVariablePathEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(JsonExtractVariablePathEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator jsonString;

  private final EvalOperator.ExpressionEvaluator jsonPath;

  private final DriverContext driverContext;

  private Warnings warnings;

  public JsonExtractVariablePathEvaluator(Source source,
      EvalOperator.ExpressionEvaluator jsonString, EvalOperator.ExpressionEvaluator jsonPath,
      DriverContext driverContext) {
    this.source = source;
    this.jsonString = jsonString;
    this.jsonPath = jsonPath;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock jsonStringBlock = (BytesRefBlock) jsonString.eval(page)) {
      try (BytesRefBlock jsonPathBlock = (BytesRefBlock) jsonPath.eval(page)) {
        BytesRefVector jsonStringVector = jsonStringBlock.asVector();
        if (jsonStringVector == null) {
          return eval(page.getPositionCount(), jsonStringBlock, jsonPathBlock);
        }
        BytesRefVector jsonPathVector = jsonPathBlock.asVector();
        if (jsonPathVector == null) {
          return eval(page.getPositionCount(), jsonStringBlock, jsonPathBlock);
        }
        return eval(page.getPositionCount(), jsonStringVector, jsonPathVector);
      }
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += jsonString.baseRamBytesUsed();
    baseRamBytesUsed += jsonPath.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock jsonStringBlock,
      BytesRefBlock jsonPathBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef jsonStringScratch = new BytesRef();
      BytesRef jsonPathScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        if (jsonStringBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (jsonStringBlock.getValueCount(p) != 1) {
          if (jsonStringBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        if (jsonPathBlock.isNull(p)) {
          result.appendNull();
          continue position;
        }
        if (jsonPathBlock.getValueCount(p) != 1) {
          if (jsonPathBlock.getValueCount(p) > 1) {
            warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
          }
          result.appendNull();
          continue position;
        }
        JsonExtract.process(result, jsonStringBlock.getBytesRef(jsonStringBlock.getFirstValueIndex(p), jsonStringScratch), jsonPathBlock.getBytesRef(jsonPathBlock.getFirstValueIndex(p), jsonPathScratch));
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector jsonStringVector,
      BytesRefVector jsonPathVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef jsonStringScratch = new BytesRef();
      BytesRef jsonPathScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        JsonExtract.process(result, jsonStringVector.getBytesRef(p, jsonStringScratch), jsonPathVector.getBytesRef(p, jsonPathScratch));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "JsonExtractVariablePathEvaluator[" + "jsonString=" + jsonString + ", jsonPath=" + jsonPath + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(jsonString, jsonPath);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(
              driverContext.warningsMode(),
              source.source().getLineNumber(),
              source.source().getColumnNumber(),
              source.text()
          );
    }
    return warnings;
  }

  static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
    private final Source source;

    private final EvalOperator.ExpressionEvaluator.Factory jsonString;

    private final EvalOperator.ExpressionEvaluator.Factory jsonPath;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory jsonString,
        EvalOperator.ExpressionEvaluator.Factory jsonPath) {
      this.source = source;
      this.jsonString = jsonString;
      this.jsonPath = jsonPath;
    }

    @Override
    public JsonExtractVariablePathEvaluator get(DriverContext context) {
      return new JsonExtractVariablePathEvaluator(source, jsonString.get(context), jsonPath.get(context), context);
    }

    @Override
    public String toString() {
      return "JsonExtractVariablePathEvaluator[" + "jsonString=" + jsonString + ", jsonPath=" + jsonPath + "]";
    }
  }
}
