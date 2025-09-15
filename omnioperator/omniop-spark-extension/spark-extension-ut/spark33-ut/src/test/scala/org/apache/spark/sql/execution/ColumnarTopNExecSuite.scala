/*
 * Copyright (C) 2022-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, SortOrder}

// refer to TakeOrderedAndProjectSuite
class ColumnarTopNExecSuite extends ColumnarSparkPlanTest {
  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  private var inputDf: DataFrame = _
  private var inputDfWithNull: DataFrame = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    inputDf = Seq[(java.lang.Integer, java.lang.Double, String)](
      (4, 2.0, "abc"),
      (1, 1.0, "aaa"),
      (8, 3.0, "ddd"),
      (10, 8.0, "")
    ).toDF("a", "b", "c")

    inputDfWithNull = Seq[(String, String, java.lang.Integer, java.lang.Double)](
      ("abc", "", 4, 2.0),
      ("", null, 1, 1.0),
      (" add", "World", 8, null),
      (" yeah  ", "yeah", 10, 8.0)
    ).toDF("a", "b", "c", "d")
  }

  test("validate columnar topN exec happened") {
    val res = inputDf.sort("a").limit(2)
    assert(res.queryExecution.executedPlan.find(_.isInstanceOf[ColumnarTakeOrderedAndProjectExec]).isDefined, s"ColumnarTakeOrderedAndProjectExec not happened, executedPlan as follows: \n${res.queryExecution.executedPlan}")
  }

  test("columnar topN is equal to native") {
    val limit = 3
    val sortOrder = Stream('a.asc, 'b.desc)
    val projectList = Seq(inputDf.col("a").as("abc").expr.asInstanceOf[NamedExpression])
    checkThatPlansAgreeTemplate(inputDf, limit, sortOrder, projectList)
  }

  test("columnar topN is equal to native with null") {
    val res = inputDfWithNull.orderBy("a", "b").selectExpr("c + 1", "d + 2").limit(2)
    checkAnswer(res, Seq(Row(2, 3.0), Row(9, null)))
  }

  def checkThatPlansAgreeTemplate(df: DataFrame, limit: Int, sortOrder: Seq[SortOrder],
                                  projectList: Seq[NamedExpression]): Unit = {
    checkThatPlansAgree(
      df,
      input => ColumnarTakeOrderedAndProjectExec(limit, sortOrder, projectList, input),
      input => TakeOrderedAndProjectExec(limit, sortOrder, projectList, input),
      sortAnswers = false)
  }
}
