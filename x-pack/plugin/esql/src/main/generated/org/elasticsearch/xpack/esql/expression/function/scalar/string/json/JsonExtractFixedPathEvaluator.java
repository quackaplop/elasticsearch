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
public final class JsonExtractFixedPathEvaluator implements EvalOperator.ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(JsonExtractFixedPathEvaluator.class);

  private final Source source;

  private final EvalOperator.ExpressionEvaluator jsonString;

  private final String jsonPath;

  private final DriverContext driverContext;

  private Warnings warnings;

  public JsonExtractFixedPathEvaluator(Source source, EvalOperator.ExpressionEvaluator jsonString,
      String jsonPath, DriverContext driverContext) {
    this.source = source;
    this.jsonString = jsonString;
    this.jsonPath = jsonPath;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock jsonStringBlock = (BytesRefBlock) jsonString.eval(page)) {
      BytesRefVector jsonStringVector = jsonStringBlock.asVector();
      if (jsonStringVector == null) {
        return eval(page.getPositionCount(), jsonStringBlock);
      }
      return eval(page.getPositionCount(), jsonStringVector);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += jsonString.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BytesRefBlock eval(int positionCount, BytesRefBlock jsonStringBlock) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef jsonStringScratch = new BytesRef();
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
        JsonExtract.process(result, jsonStringBlock.getBytesRef(jsonStringBlock.getFirstValueIndex(p), jsonStringScratch), this.jsonPath);
      }
      return result.build();
    }
  }

  public BytesRefBlock eval(int positionCount, BytesRefVector jsonStringVector) {
    try(BytesRefBlock.Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
      BytesRef jsonStringScratch = new BytesRef();
      position: for (int p = 0; p < positionCount; p++) {
        JsonExtract.process(result, jsonStringVector.getBytesRef(p, jsonStringScratch), this.jsonPath);
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "JsonExtractFixedPathEvaluator[" + "jsonString=" + jsonString + ", jsonPath=" + jsonPath + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(jsonString);
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

    private final String jsonPath;

    public Factory(Source source, EvalOperator.ExpressionEvaluator.Factory jsonString,
        String jsonPath) {
      this.source = source;
      this.jsonString = jsonString;
      this.jsonPath = jsonPath;
    }

    @Override
    public JsonExtractFixedPathEvaluator get(DriverContext context) {
      return new JsonExtractFixedPathEvaluator(source, jsonString.get(context), jsonPath, context);
    }

    @Override
    public String toString() {
      return "JsonExtractFixedPathEvaluator[" + "jsonString=" + jsonString + ", jsonPath=" + jsonPath + "]";
    }
  }
}
