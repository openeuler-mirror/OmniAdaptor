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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.util.RexNodeUtil.SpecialExprType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Date/time-related special-expression handlers extracted from {@link RexNodeUtil}.
 *
 * <p>Each handler builds the OmniOperator JSON for one date/time function and is wired into
 * {@link RexNodeUtil#specialHandlerMap} by {@link #register()}. Keeping every date/time function
 * in this file means adding/adjusting such an expression only touches this file (plus the shared
 * {@link SpecialExprType} enum).
 */
final class DateTimeExprHandlers {
    private static final Logger LOG = LoggerFactory.getLogger(DateTimeExprHandlers.class);

    private DateTimeExprHandlers() {
    }

    /** Register operator names, native function names and handlers for all date/time expressions. */
    static void register() {
        RexNodeUtil.specialOperatorMap.put("PROCTIME_MATERIALIZE", SpecialExprType.PROCTIME);
        RexNodeUtil.specialOperatorMap.put("PROCTIME", SpecialExprType.PROCTIME);
        RexNodeUtil.specialOperatorMap.put("EXTRACT", SpecialExprType.EXTRACT);
        RexNodeUtil.specialOperatorMap.put("DATE_FORMAT", SpecialExprType.DATE_FORMAT);
        RexNodeUtil.specialOperatorMap.put("TO_TIMESTAMP_LTZ", SpecialExprType.TO_TIMESTAMP_LTZ);
        RexNodeUtil.specialOperatorMap.put("TO_DATE", SpecialExprType.TO_DATE);
        RexNodeUtil.specialOperatorMap.put("CURRENT_TIMESTAMP", SpecialExprType.CURRENT_TIMESTAMP);
        RexNodeUtil.specialOperatorMap.put("DATE_ADD", SpecialExprType.DATE_ADD);
        RexNodeUtil.specialOperatorMap.put("UNIX_TIMESTAMP", SpecialExprType.UNIX_TIMESTAMP);
        RexNodeUtil.specialOperatorMap.put("FROM_UNIXTIME", SpecialExprType.FROM_UNIXTIME);
        RexNodeUtil.udfOperatorMap.put("DATE_ADD", SpecialExprType.DATE_ADD);

        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.UNIX_TIMESTAMP, "unix_timestamp");

        RexNodeUtil.specialHandlerMap.put(SpecialExprType.TO_TIMESTAMP_LTZ, DateTimeExprHandlers::handleToTimestampLtz);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.TO_DATE, DateTimeExprHandlers::handleToDate);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.PROCTIME, DateTimeExprHandlers::handleProctime);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.DATE_FORMAT, DateTimeExprHandlers::handleDateFormat);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.CURRENT_TIMESTAMP, DateTimeExprHandlers::handleCurrentTimestamp);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.DATE_ADD, DateTimeExprHandlers::handleDateAdd);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.FROM_UNIXTIME, DateTimeExprHandlers::handleFromUnixtime);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.EXTRACT, DateTimeExprHandlers::handleExtract);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.UNIX_TIMESTAMP, RexNodeUtil::handleSimpleFunction);
    }

    static Map<String, Object> handleToTimestampLtz(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "to_timestamp_ltz");
        List<Map<String, Object>> toTimestampLtzArgList = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            toTimestampLtzArgList.add(RexNodeUtil.buildJsonMap(operands.get(i)));
        }
        jsonMap.put("arguments", toTimestampLtzArgList);
        return jsonMap;
    }

    static Map<String, Object> handleToDate(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // TO_DATE(string1[, string2]) -> DATE, default format 'yyyy-MM-dd'.
        // Maps to the vectorized to_date({VARCHAR,VARCHAR}) -> DATE32 function.
        jsonMap.put("exprType", "FUNCTION");
        // to_date is registered to return OMNI_DATE32(8); setDataType would collapse
        // the DATE return type to LONG(2), so set it explicitly.
        jsonMap.put("returnType", RexNodeUtil.RexTypeToIdMap.get("DATE"));
        jsonMap.put("function_name", "to_date");
        List<Map<String, Object>> toDateArgs = new ArrayList<>();
        Map<String, Object> toDateInputArg = RexNodeUtil.buildJsonMap(operands.get(0));
        RexNodeUtil.normalizeCharLiteralToVarchar(toDateInputArg);
        toDateArgs.add(toDateInputArg);
        Map<String, Object> toDateFormatArg;
        if (operands.size() >= 2) {
            toDateFormatArg = RexNodeUtil.buildJsonMap(operands.get(1));
            RexNodeUtil.normalizeCharLiteralToVarchar(toDateFormatArg);
        } else {
            // Flink default: TO_DATE(string) -> 'yyyy-MM-dd'
            toDateFormatArg = new LinkedHashMap<>();
            toDateFormatArg.put("exprType", "LITERAL");
            toDateFormatArg.put("dataType", RexNodeUtil.RexTypeToIdMap.get("VARCHAR"));
            toDateFormatArg.put("isNull", false);
            toDateFormatArg.put("value", "yyyy-MM-dd");
            toDateFormatArg.put("width", 10);
        }
        toDateArgs.add(toDateFormatArg);
        jsonMap.put("arguments", toDateArgs);
        LOG.info("The TO_DATE expression is {} ", rexCall.toString());
        return jsonMap;
    }

    static Map<String, Object> handleProctime(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        SqlOperator operator = rexCall.getOperator();
        jsonMap.put("exprType", SpecialExprType.PROCTIME);
        jsonMap.put("returnType", RexNodeUtil.RexTypeToIdMap.get(rexCall.getType().getSqlTypeName().toString()));
        LOG.info("The operator is* {} ", operator.getName());
        LOG.info("The type is* {} ", rexCall.getType().getSqlTypeName().toString());
        return jsonMap;
    }

    static Map<String, Object> handleDateFormat(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        Integer returnDataType = RexNodeUtil.RexTypeToIdMap.get(rexCall.getType().getSqlTypeName().toString());
        jsonMap.put("returnType", returnDataType);
        jsonMap.put("width", operands.get(1).getType().getPrecision());
        List<Map<String, Object>> argumentsList = new ArrayList<>();
        Map<String, Object> argMap1 = RexNodeUtil.buildJsonMap(operands.get(0));
        RexNodeUtil.setDataType(operands.get(0), argMap1, "dataType");
        if (!argMap1.getOrDefault("dataType", 2).equals(2)) {
            argMap1.put("value", "INVALID");
        }
        argMap1.put("dataType", 2);
        argumentsList.add(argMap1);
        Map<String, Object> argMap2 = RexNodeUtil.buildJsonMap(operands.get(1));
        argMap2.put("dataType", returnDataType);
        argumentsList.add(argMap2);
        if ("yyyy-MM-dd".equals(argMap2.get("value"))) {
            argMap2.put("value", "%Y-%m-%d");
        } else if ("HH:mm".equals(argMap2.get("value"))) {
            argMap2.put("value", "%H:%M");
        } else if ("HH".equals(argMap2.get("value"))) {
            argMap2.put("value", "%H");
        } else {
            argMap2.put("value", "INVALID");
        }
        if (operands.get(0).getType().getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            jsonMap.put("function_name", "from_unixtime_with_tz");
            Map<String, Object> argMap3 = new LinkedHashMap<>();
            argMap3.put("dataType", 15);
            argMap3.put("exprType", "LITERAL");
            argMap3.put("isNull", false);
            argMap3.put("value", CommonExecCalc.getZoneId().getId());
            argMap3.put("width", CommonExecCalc.getZoneId().getId().length());
            argumentsList.add(argMap3);
        } else {
            jsonMap.put("function_name", "from_unixtime_without_tz");
        }
        jsonMap.put("arguments", argumentsList);
        return jsonMap;
    }

    static Map<String, Object> handleCurrentTimestamp(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "current_timestamp");
        List<Map<String, Object>> currentTimestampArgs = new ArrayList<>();
        if (operands.size() > 0) {
            for (int i = 0; i < operands.size(); i++) {
                currentTimestampArgs.add(RexNodeUtil.buildJsonMap(operands.get(i)));
            }
        }
        jsonMap.put("arguments", currentTimestampArgs);
        LOG.info("The CURRENT_TIMESTAMP expression is {} ", rexCall.toString());
        return jsonMap;
    }

    static Map<String, Object> handleDateAdd(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        jsonMap.put("returnType", 2);
        jsonMap.put("function_name", "date_add_days");
        List<Map<String, Object>> dateAddArgs = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            Map<String, Object> argMap = RexNodeUtil.buildJsonMap(operands.get(i));
            if (i == 0) {
                SqlTypeName firstArgType = operands.get(0).getType().getSqlTypeName();
                if (firstArgType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                        || SqlTypeName.DATETIME_TYPES.contains(firstArgType)) {
                    argMap.put("dataType", 2);
                }
            }
            dateAddArgs.add(argMap);
        }
        jsonMap.put("arguments", dateAddArgs);
        LOG.info("The DATE_ADD expression is {} ", rexCall.toString());
        return jsonMap;
    }

    static Map<String, Object> handleFromUnixtime(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "from_unixtime");
        List<Map<String, Object>> fromUnixTimeArgs = new ArrayList<>();
        Map<String, Object> fromUnixTimeInputArg = RexNodeUtil.buildJsonMap(operands.get(0));
        RexNodeUtil.setDataType(operands.get(0), fromUnixTimeInputArg, "dataType");
        RexNodeUtil.normalizeCharLiteralToVarchar(fromUnixTimeInputArg);
        fromUnixTimeArgs.add(fromUnixTimeInputArg);
        Map<String, Object> fromUnixTimeFormatArg;
        if (operands.size() >= 2) {
            fromUnixTimeFormatArg = RexNodeUtil.buildJsonMap(operands.get(1));
            RexNodeUtil.normalizeCharLiteralToVarchar(fromUnixTimeFormatArg);
        } else {
            // Flink default: FROM_UNIXTIME(numeric) -> 'yyyy-MM-dd HH:mm:ss'
            fromUnixTimeFormatArg = new LinkedHashMap<>();
            fromUnixTimeFormatArg.put("exprType", "LITERAL");
            fromUnixTimeFormatArg.put("dataType", RexNodeUtil.RexTypeToIdMap.get("VARCHAR"));
            fromUnixTimeFormatArg.put("isNull", false);
            fromUnixTimeFormatArg.put("value", "yyyy-MM-dd HH:mm:ss");
            fromUnixTimeFormatArg.put("width", 19);
        }
        fromUnixTimeArgs.add(fromUnixTimeFormatArg);
        if (operands.get(0).getType().getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            Map<String, Object> fromUnixTimeTzArg = new LinkedHashMap<>();
            fromUnixTimeTzArg.put("dataType", RexNodeUtil.RexTypeToIdMap.get("VARCHAR"));
            fromUnixTimeTzArg.put("exprType", "LITERAL");
            fromUnixTimeTzArg.put("isNull", false);
            fromUnixTimeTzArg.put("value", CommonExecCalc.getZoneId().getId());
            fromUnixTimeTzArg.put("width", CommonExecCalc.getZoneId().getId().length());
            fromUnixTimeArgs.add(fromUnixTimeTzArg);
        }
        jsonMap.put("arguments", fromUnixTimeArgs);
        return jsonMap;
    }

    static Map<String, Object> handleExtract(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // Current hardcoded solution for extracting hour
        if (operands.get(0).toString().equals("FLAG(HOUR)")) { // && operands.get(1).toString().equals("CAST($2):TIMESTAMP(3)")
            jsonMap.put("exprType", "FUNCTION");
            // Returns int for 0-24
            jsonMap.put("returnType", 1);
            List<Object> args = new LinkedList<>();
            args.add(RexNodeUtil.buildJsonMap(operands.get(1)));
            if (operands.get(1).getType().getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                jsonMap.put("function_name", "get_hour_with_tz");
                Map<String, Object> argMap = new LinkedHashMap<>();
                argMap.put("dataType", 15);
                argMap.put("exprType", "LITERAL");
                argMap.put("isNull", false);
                argMap.put("value", CommonExecCalc.getZoneId().getId());
                argMap.put("width", CommonExecCalc.getZoneId().getId().length());
                args.add(argMap);
            } else {
                jsonMap.put("function_name", "get_hour");
            }
            jsonMap.put("arguments", args);
        } else {
            jsonMap.put("exprType", "INVALID");
        }
        return jsonMap;
    }
}
