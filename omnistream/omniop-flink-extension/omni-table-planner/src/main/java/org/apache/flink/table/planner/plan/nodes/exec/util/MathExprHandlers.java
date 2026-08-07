/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.apache.flink.table.planner.plan.nodes.exec.util;

import org.apache.flink.table.planner.plan.nodes.exec.util.RexNodeUtil.SpecialExprType;

/**
 * Math/numeric special-expression registrations extracted from {@link RexNodeUtil}.
 *
 * <p>All math functions here forward their operands one-to-one to a native function, so they share
 * the generic {@link RexNodeUtil#handleSimpleFunction} handler and only contribute operator-name
 * and native-function-name mappings. Adding a math function only touches this file (plus the shared
 * {@link SpecialExprType} enum).
 */
final class MathExprHandlers {

    private MathExprHandlers() {
    }

    /** Register operator names, native function names and handlers for all math expressions. */
    static void register() {
        RexNodeUtil.specialOperatorMap.put("ROUND", SpecialExprType.ROUND);
        RexNodeUtil.specialOperatorMap.put("GREATEST", SpecialExprType.GREATEST);
        RexNodeUtil.specialOperatorMap.put("LEAST", SpecialExprType.LEAST);
        RexNodeUtil.specialOperatorMap.put("SINH", SpecialExprType.SINH);
        RexNodeUtil.specialOperatorMap.put("COS", SpecialExprType.COS);
        RexNodeUtil.specialOperatorMap.put("COT", SpecialExprType.COT);
        RexNodeUtil.specialOperatorMap.put("ASIN", SpecialExprType.ASIN);
        RexNodeUtil.specialOperatorMap.put("ACOS", SpecialExprType.ACOS);
        RexNodeUtil.specialOperatorMap.put("ATAN", SpecialExprType.ATAN);
        RexNodeUtil.specialOperatorMap.put("ATAN2", SpecialExprType.ATAN2);
        RexNodeUtil.specialOperatorMap.put("COSH", SpecialExprType.COSH);
        RexNodeUtil.specialOperatorMap.put("DEGREES", SpecialExprType.DEGREES);
        RexNodeUtil.specialOperatorMap.put("SIGN", SpecialExprType.SIGN);
        RexNodeUtil.specialOperatorMap.put("SIN", SpecialExprType.SIN);
        RexNodeUtil.specialOperatorMap.put("TAN", SpecialExprType.TAN);
        RexNodeUtil.specialOperatorMap.put("TANH", SpecialExprType.TANH);
        RexNodeUtil.specialOperatorMap.put("RADIANS", SpecialExprType.RADIANS);
        RexNodeUtil.specialOperatorMap.put("PI", SpecialExprType.PI);
        RexNodeUtil.specialOperatorMap.put("E", SpecialExprType.E);
        RexNodeUtil.specialOperatorMap.put("RAND", SpecialExprType.RAND);
        RexNodeUtil.specialOperatorMap.put("RAND_INTEGER", SpecialExprType.RAND_INTEGER);
        RexNodeUtil.specialOperatorMap.put("TRUNCATE", SpecialExprType.TRUNCATE);

        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.ROUND, "round");
        // "flink_" prefixed natives exist where Flink semantics diverge from Spark's:
        //   GREATEST/LEAST: Flink propagates NULL, Spark skips NULL arguments.
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.GREATEST, "flink_greatest");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.LEAST, "flink_least");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.SINH, "sinh");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.COS, "cos");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.COT, "cot");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.ASIN, "asin");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.ACOS, "acos");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.ATAN, "atan");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.ATAN2, "atan2");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.COSH, "cosh");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.DEGREES, "degrees");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.SIGN, "sign");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.SIN, "sin");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.TAN, "tan");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.TANH, "tanh");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.RADIANS, "radians");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.PI, "pi");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.E, "e");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.RAND, "rand");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.RAND_INTEGER, "rand_integer");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.TRUNCATE, "truncate");

        RexNodeUtil.specialHandlerMap.put(SpecialExprType.ROUND, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.GREATEST, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.LEAST, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.SINH, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.COS, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.COT, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.ASIN, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.ACOS, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.ATAN, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.ATAN2, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.COSH, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.DEGREES, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.SIGN, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.SIN, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.TAN, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.TANH, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.RADIANS, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.PI, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.E, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.RAND, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.RAND_INTEGER, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.TRUNCATE, RexNodeUtil::handleSimpleFunction);
    }
}
