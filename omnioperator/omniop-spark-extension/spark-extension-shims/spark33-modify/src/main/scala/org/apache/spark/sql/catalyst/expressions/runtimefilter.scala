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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreePattern.{RUNTIME_FILTER_EXPRESSION, RUNTIME_FILTER_SUBQUERY, TreePattern}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{BinaryType, DataType}

/**
 * The RuntimeFilterSubquery expression is only used in runtime filter. It is inserted in cases
 * when broadcast exchange can be reused.
 *
 * @param filterApplicationSideExp the filtering key of the application side.
 * @param filterCreationSidePlan the build side of the join.
 * @param filterCreationSideExp the key of the creation side.
 */
case class RuntimeFilterSubquery(
                                  filterApplicationSideExp: Expression,
                                  filterCreationSidePlan: LogicalPlan,
                                  filterCreationSideExp: Expression,
                                  exprId: ExprId = NamedExpression.newExprId,
                                  hint: Option[HintInfo] = None)
  extends SubqueryExpression(
    filterCreationSidePlan, Seq(filterApplicationSideExp), exprId, Seq.empty)
    with Unevaluable
    with UnaryLike[Expression] {

  override def child: Expression = filterApplicationSideExp

  override def dataType: DataType = BinaryType

  override def plan: LogicalPlan = filterCreationSidePlan

  override def nullable: Boolean = false

  override def withNewPlan(plan: LogicalPlan): RuntimeFilterSubquery =
    copy(filterCreationSidePlan = plan)

  override lazy val resolved: Boolean = {
    filterApplicationSideExp.resolved &&
      filterCreationSidePlan.resolved &&
      filterCreationSideExp.resolved
  }

  final override def nodePatternsInternal: Seq[TreePattern] = Seq(RUNTIME_FILTER_SUBQUERY)

  override def toString: String = s"runtimefilter#${exprId.id} $conditionString"

  override lazy val canonicalized: RuntimeFilterSubquery = {
    copy(
      filterApplicationSideExp = filterApplicationSideExp.canonicalized,
      filterCreationSidePlan = filterCreationSidePlan.canonicalized,
      filterCreationSideExp = filterCreationSideExp.canonicalized,
      exprId = ExprId(0))
  }

  override protected def withNewChildInternal(newChild: Expression): RuntimeFilterSubquery =
    copy(filterApplicationSideExp = newChild)
}

/**
 * Marker for a planned runtime filter expression.
 * The expression is created during planning, and it defers to its child for evaluation.
 *
 * @param child underlying aggregate for runtime filter.
 */
case class RuntimeFilterExpression(child: Expression)
  extends UnaryExpression {
  override def dataType: DataType = child.dataType
  override def eval(input: InternalRow): Any = child.eval(input)
  final override val nodePatterns: Seq[TreePattern] = Seq(RUNTIME_FILTER_EXPRESSION)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.genCode(ctx)
  }

  override protected def withNewChildInternal(newChild: Expression): RuntimeFilterExpression =
    copy(child = newChild)
}