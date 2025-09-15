/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{BindReferences, RuntimeFilterExpression, RuntimeFilterSubquery, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.RUNTIME_FILTER_SUBQUERY
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode, HashJoin}
import org.apache.spark.sql.execution.{BroadcastExchangeExecProxy, ColumnarBroadcastExchangeExec, ColumnarFilterExec, ColumnarFileSourceScanExec,
  OmniColumnarToRowExec, ColumnarConditionProjectExec, ColumnarProjectExec}

/**
 * This planner rule aims at rewriting runtime filter in order to reuse the
 * results of broadcast. For joins that are not planned as broadcast hash joins we keep
 * the fallback mechanism with subquery duplicate.
 */
case class PlanRuntimeFilterFilters(sparkSession: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.runtimeFilterBloomFilterEnabled) {
      return plan
    }

    plan.transformAllExpressionsWithPruning(_.containsPattern(RUNTIME_FILTER_SUBQUERY)) {
      case RuntimeFilterSubquery(_, buildPlan, buildKey, exprId, _) =>
        val sparkPlan = QueryExecution.createSparkPlan(
          sparkSession, sparkSession.sessionState.planner, buildPlan)
        val filterCreationSidePlan = getFilterCreationSidePlan(sparkPlan)
        val executedFilterCreationSidePlan =
          QueryExecution.prepareExecutedPlan(sparkSession, filterCreationSidePlan)

        val newExecutedFilterCreationSidePlan = executedFilterCreationSidePlan transformUp {
          case omniColumnarToRowExec: OmniColumnarToRowExec =>
            omniColumnarToRowExec.child
        }

        val broadcastExchangeNoFallback = checkBroadcastExchangeNoFallback(newExecutedFilterCreationSidePlan)

        val onlyAttributeReference = checkAttributeReference(Seq(buildKey), 0)

        // Using `sparkPlan` is a little hacky as it is based on the assumption that this rule is
        // the first to be applied (apart from `InsertAdaptiveSparkPlan`).
        val canReuseExchange = conf.exchangeReuseEnabled && broadcastExchangeNoFallback && onlyAttributeReference &&
          plan.exists {
            case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _, _) =>
              val executedLeft = QueryExecution.prepareExecutedPlan(sparkSession, left)
              executedLeft.sameResult(executedFilterCreationSidePlan)
            case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _) =>
              val executedRight = QueryExecution.prepareExecutedPlan(sparkSession, right)
              executedRight.sameResult(executedFilterCreationSidePlan)
            case _ => false
          }

        val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, sparkPlan)

        val bloomFilterSubquery = if (canReuseExchange) {
          val replacePlan = getFilterCreationSidePlan(executedPlan)
          val packedKeys = BindReferences.bindReferences(
            HashJoin.rewriteKeyExpr(Seq(buildKey)), newExecutedFilterCreationSidePlan.output)
          val mode = HashedRelationBroadcastMode(packedKeys)
          // plan a broadcast exchange of the build side of the join
          val exchange = new ColumnarBroadcastExchangeExec(mode, newExecutedFilterCreationSidePlan)
          val name = s"runtimefilter#${exprId.id}"
          val broadcastValues = SubqueryBroadcastExec(name, 0, Seq(buildKey), exchange)
          val broadcastProxy =
            BroadcastExchangeExecProxy(broadcastValues, newExecutedFilterCreationSidePlan.output)

          val newExecutedPlan = executedPlan transformUp {
            case hashAggregateExec: ObjectHashAggregateExec
              if hashAggregateExec.child.eq(replacePlan) =>
              hashAggregateExec.copy(child = broadcastProxy)
          }

          ScalarSubquery(
            SubqueryExec.createForScalarSubquery(
              s"scalar-subquery#${exprId.id}",
              newExecutedPlan), exprId)
        } else {
          ScalarSubquery(
            SubqueryExec.createForScalarSubquery(
              s"scalar-subquery#${exprId.id}",
              executedPlan), exprId)
        }

        RuntimeFilterExpression(bloomFilterSubquery)
    }
  }

  private def getFilterCreationSidePlan(plan: SparkPlan): SparkPlan = {
    assert(plan.isInstanceOf[ObjectHashAggregateExec])
    plan.asInstanceOf[ObjectHashAggregateExec].child match {
      case objectHashAggregate: ObjectHashAggregateExec =>
        objectHashAggregate.child
      case shuffleExchange: ShuffleExchangeExec =>
        shuffleExchange.child.asInstanceOf[ObjectHashAggregateExec].child
      case other => other
    }
  }

  private def checkBroadcastExchangeNoFallback(plan: SparkPlan): Boolean = {
    plan match {
      case columnarFilter: ColumnarFilterExec =>
        checkBroadcastExchangeNoFallback(columnarFilter.child)
      case columnarCondition: ColumnarConditionProjectExec =>
        checkBroadcastExchangeNoFallback(columnarCondition.child)
      case columnarProject: ColumnarProjectExec =>
        checkBroadcastExchangeNoFallback(columnarProject.child)
      case columnarFileScan: ColumnarFileSourceScanExec => true
      case other => false
    }
  }

  private def checkAttributeReference(buildKeys: Seq[Expression], index: Int): Boolean = {
    if (index >= 0 && index < buildKeys.length) {
      buildKeys(index).isInstanceOf[AttributeReference]
    } else {
      false
    }
  }
}