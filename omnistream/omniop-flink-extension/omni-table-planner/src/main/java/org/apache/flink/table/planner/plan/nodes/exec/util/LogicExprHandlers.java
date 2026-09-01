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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.planner.plan.nodes.exec.util.RexNodeUtil.SpecialExprType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Three-valued-logic / predicate special-expression registrations extracted from
 * {@link RexNodeUtil}.
 *
 * <p>{@code IS NULL} / {@code IS NOT NULL} emit dedicated JSON node types; the remaining
 * predicates forward one-to-one via {@link RexNodeUtil#handleSimpleFunction}. Adding a logic
 * predicate only touches this file (plus the shared {@link SpecialExprType} enum).
 *
 * <p>{@code IS NOT TRUE} is also registered here, but {@code buildJsonMap} prefers the unary
 * rewrite to {@code CASE WHEN x THEN FALSE ELSE TRUE END}, so the special handler is rarely hit.
 */
final class LogicExprHandlers {

    private LogicExprHandlers() {
    }

    /** Register operator names, native function names and handlers for all logic predicates. */
    static void register() {
        RexNodeUtil.specialOperatorMap.put("IS NULL", SpecialExprType.IS_NULL);
        RexNodeUtil.specialOperatorMap.put("IS UNKNOWN", SpecialExprType.IS_NULL);
        RexNodeUtil.specialOperatorMap.put("IS NOT NULL", SpecialExprType.IS_NOT_NULL);
        RexNodeUtil.specialOperatorMap.put("IS FALSE", SpecialExprType.IS_FALSE);
        RexNodeUtil.specialOperatorMap.put("IS NOT FALSE", SpecialExprType.IS_NOT_FALSE);
        RexNodeUtil.specialOperatorMap.put("IS NOT UNKNOWN", SpecialExprType.IS_NOT_UNKNOWN);
        RexNodeUtil.specialOperatorMap.put("IS NOT TRUE", SpecialExprType.IS_NOT_TRUE);
        RexNodeUtil.specialOperatorMap.put("NULLIF", SpecialExprType.NULLIF);

        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.IS_FALSE, "is_false");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.IS_NOT_FALSE, "is_not_false");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.IS_NOT_UNKNOWN, "is_not_unknown");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.IS_NOT_TRUE, "is_not_true");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.NULLIF, "nullif");

        RexNodeUtil.specialHandlerMap.put(SpecialExprType.IS_NULL, LogicExprHandlers::handleIsNull);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.IS_NOT_NULL, LogicExprHandlers::handleIsNotNull);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.IS_FALSE, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.IS_NOT_FALSE, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.IS_NOT_UNKNOWN, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.IS_NOT_TRUE, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.NULLIF, RexNodeUtil::handleSimpleFunction);
    }

    static Map<String, Object> handleIsNull(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "IS_NULL");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        List<Map<String, Object>> isnullArgList = new ArrayList<>();
        isnullArgList.add(RexNodeUtil.buildJsonMap(operands.get(0)));
        jsonMap.put("arguments", isnullArgList);
        return jsonMap;
    }

    static Map<String, Object> handleIsNotNull(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "IS_NOT_NULL");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        List<Map<String, Object>> notnullArgList = new ArrayList<>();
        notnullArgList.add(RexNodeUtil.buildJsonMap(operands.get(0)));
        jsonMap.put("arguments", notnullArgList);
        return jsonMap;
    }
}
