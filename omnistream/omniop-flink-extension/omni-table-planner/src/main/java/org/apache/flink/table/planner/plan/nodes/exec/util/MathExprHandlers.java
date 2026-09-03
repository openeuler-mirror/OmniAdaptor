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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.nodes.exec.util.RexNodeUtil.SpecialExprType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Math/numeric special-expression registrations extracted from {@link RexNodeUtil}.
 *
 * <p>All math functions here forward their operands one-to-one to a native function, so they share
 * the generic {@link RexNodeUtil#handleSimpleFunction} handler and only contribute operator-name
 * and native-function-name mappings. Adding a math function only touches this file (plus the shared
 * {@link SpecialExprType} enum).
 */
final class MathExprHandlers {
    private static final Logger LOG = LoggerFactory.getLogger(MathExprHandlers.class);

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
        RexNodeUtil.specialOperatorMap.put("FLOOR", SpecialExprType.FLOOR);
        RexNodeUtil.specialOperatorMap.put("CEIL", SpecialExprType.CEIL);
        RexNodeUtil.specialOperatorMap.put("CEILING", SpecialExprType.CEIL);
        RexNodeUtil.specialOperatorMap.put("LN", SpecialExprType.LN);
        RexNodeUtil.specialOperatorMap.put("ABS", SpecialExprType.ABS);
        RexNodeUtil.specialOperatorMap.put("POWER", SpecialExprType.POWER);

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
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.RAND, "flink_rand");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.RAND_INTEGER, "rand_integer");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.TRUNCATE, "truncate");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.ABS, "abs");

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
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.FLOOR, MathExprHandlers::handleFloor);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.CEIL, MathExprHandlers::handleCeil);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.LN, MathExprHandlers::handleLn);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.ABS, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.POWER, MathExprHandlers::handlePower);
    }

    static Map<String, Object> handleFloor(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        if (operands.size() == 1) {
            jsonMap.put("function_name", "floor");
            List<Map<String, Object>> floorArgList = new ArrayList<>();
            floorArgList.add(RexNodeUtil.buildJsonMap(operands.get(0)));
            jsonMap.put("arguments", floorArgList);
        } else {
            jsonMap.put("function_name", "flink_floor_time");
            List<Map<String, Object>> floorTimeArgList = new ArrayList<>();
            floorTimeArgList.add(RexNodeUtil.buildJsonMap(operands.get(0)));
            Map<String, Object> unitMap = RexNodeUtil.buildJsonMap(operands.get(1));
            unitMap.put("dataType", 15);
            unitMap.put("width", 2147483647);
            floorTimeArgList.add(unitMap);
            jsonMap.put("arguments", floorTimeArgList);
            if (rexCall.getType().getSqlTypeName() == SqlTypeName.TIME) {
                String flagFloor = RexNodeUtil.getSymbolLiteralName(operands.get(1));
                switch (flagFloor) {
                    case "DAY":
                    case "WEEK":
                    case "MONTH":
                    case "QUARTER":
                    case "YEAR":
                        jsonMap.put("exprType", "INVALID");
                        LOG.info("FLOOR function need HOUR, MINUTE or SECOND when use Time type");
                        break;
                    default:
                        break;
                }
            }
        }
        return jsonMap;
    }

    static Map<String, Object> handleCeil(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        if (operands.size() == 1) {
            jsonMap.put("function_name", "ceil");
            List<Map<String, Object>> ceilArgList = new ArrayList<>();
            ceilArgList.add(RexNodeUtil.buildJsonMap(operands.get(0)));
            jsonMap.put("arguments", ceilArgList);
        } else {
            jsonMap.put("function_name", "flink_ceil_time");
            List<Map<String, Object>> ceilTimeArgList = new ArrayList<>();
            ceilTimeArgList.add(RexNodeUtil.buildJsonMap(operands.get(0)));
            Map<String, Object> ceilMap = RexNodeUtil.buildJsonMap(operands.get(1));
            ceilMap.put("dataType", 15);
            ceilMap.put("width", 2147483647);
            ceilTimeArgList.add(ceilMap);
            jsonMap.put("arguments", ceilTimeArgList);
            if (rexCall.getType().getSqlTypeName() == SqlTypeName.TIME) {
                String flagCeil = RexNodeUtil.getSymbolLiteralName(operands.get(1));
                switch (flagCeil) {
                    case "DAY":
                    case "WEEK":
                    case "MONTH":
                    case "QUARTER":
                    case "YEAR":
                        jsonMap.put("exprType", "INVALID");
                        LOG.info("CEIL function need HOUR, MINUTE or SECOND when use Time type");
                        break;
                    default:
                        break;
                }
            }
        }
        return jsonMap;
    }

    static Map<String, Object> handleLn(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "ln");
        List<Map<String, Object>> lnArgList = new ArrayList<>();
        Map<String, Object> lnOperandMap = RexNodeUtil.buildJsonMap(operands.get(0));
        if (operands.get(0).getType().getSqlTypeName() != SqlTypeName.DOUBLE) {
            Map<String, Object> lnCastMap = new LinkedHashMap<>();
            lnCastMap.put("exprType", "FUNCTION");
            lnCastMap.put("function_name", "CAST");
            lnCastMap.put("returnType", 3);
            List<Map<String, Object>> lnCastArgs = new ArrayList<>();
            lnCastArgs.add(lnOperandMap);
            lnCastMap.put("arguments", lnCastArgs);
            lnArgList.add(lnCastMap);
        } else {
            lnArgList.add(lnOperandMap);
        }
        jsonMap.put("arguments", lnArgList);
        return jsonMap;
    }

    static Map<String, Object> handlePower(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "power");
        List<Map<String, Object>> powerArgList = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            Map<String, Object> powerOperandMap = RexNodeUtil.buildJsonMap(operands.get(i));
            if (operands.get(i).getType().getSqlTypeName() != SqlTypeName.DOUBLE) {
                Map<String, Object> powerCastMap = new LinkedHashMap<>();
                powerCastMap.put("exprType", "FUNCTION");
                powerCastMap.put("function_name", "CAST");
                powerCastMap.put("returnType", 3);
                List<Map<String, Object>> powerCastArgs = new ArrayList<>();
                powerCastArgs.add(powerOperandMap);
                powerCastMap.put("arguments", powerCastArgs);
                powerArgList.add(powerCastMap);
            } else {
                powerArgList.add(powerOperandMap);
            }
        }
        jsonMap.put("arguments", powerArgList);
        return jsonMap;
    }
}
