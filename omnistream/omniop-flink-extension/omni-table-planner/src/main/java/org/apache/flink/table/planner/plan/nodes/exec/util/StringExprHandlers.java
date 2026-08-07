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
 * String-related special-expression handlers extracted from {@link RexNodeUtil}.
 *
 * <p>Each handler builds the OmniOperator JSON for one string function and is wired into
 * {@link RexNodeUtil#specialHandlerMap} by {@link #register()}. Functions whose native signature
 * matches their Flink operands one-to-one reuse the shared {@link RexNodeUtil#handleSimpleFunction}
 * and only need a {@link RexNodeUtil#simpleFunctionNameMap} entry here.
 */
final class StringExprHandlers {
    private static final Logger LOG = LoggerFactory.getLogger(StringExprHandlers.class);

    private StringExprHandlers() {
    }

    /** Register operator names, native function names and handlers for all string expressions. */
    static void register() {
        RexNodeUtil.specialOperatorMap.put("REGEXP_EXTRACT", SpecialExprType.REGEXP_EXTRACT);
        RexNodeUtil.specialOperatorMap.put("SPLIT_INDEX", SpecialExprType.SPLIT_INDEX);
        RexNodeUtil.specialOperatorMap.put("FROM_BASE64", SpecialExprType.FROM_BASE64);
        RexNodeUtil.specialOperatorMap.put("CHAR_LENGTH", SpecialExprType.CHAR_LENGTH);
        RexNodeUtil.specialOperatorMap.put("CHARACTER_LENGTH", SpecialExprType.CHAR_LENGTH);
        RexNodeUtil.specialOperatorMap.put("count_char", SpecialExprType.COUNT_CHAR);
        RexNodeUtil.specialOperatorMap.put("LOWER", SpecialExprType.LOWER);
        RexNodeUtil.specialOperatorMap.put("CONCAT", SpecialExprType.CONCAT);
        RexNodeUtil.specialOperatorMap.put("CONCAT_WS", SpecialExprType.CONCAT_WS);
        RexNodeUtil.specialOperatorMap.put("REPLACE", SpecialExprType.REPLACE);
        RexNodeUtil.specialOperatorMap.put("SUBSTRING", SpecialExprType.SUBSTR);
        RexNodeUtil.specialOperatorMap.put("SUBSTR", SpecialExprType.SUBSTR);
        RexNodeUtil.specialOperatorMap.put("INSTR", SpecialExprType.INSTR);
        RexNodeUtil.specialOperatorMap.put("LIKE", SpecialExprType.LIKE);
        RexNodeUtil.specialOperatorMap.put("RPAD", SpecialExprType.RPAD);
        RexNodeUtil.specialOperatorMap.put("LPAD", SpecialExprType.LPAD);
        RexNodeUtil.specialOperatorMap.put("REPEAT", SpecialExprType.REPEAT);
        RexNodeUtil.specialOperatorMap.put("OVERLAY", SpecialExprType.OVERLAY);
        RexNodeUtil.specialOperatorMap.put("BIN", SpecialExprType.BIN);
        RexNodeUtil.specialOperatorMap.put("HEX", SpecialExprType.HEX);
        RexNodeUtil.specialOperatorMap.put("UUID", SpecialExprType.UUID);
        RexNodeUtil.specialOperatorMap.put("IS_ALPHA", SpecialExprType.IS_ALPHA);
        RexNodeUtil.specialOperatorMap.put("IS_DECIMAL", SpecialExprType.IS_DECIMAL);

        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.CONCAT, "concat");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.CONCAT_WS, "concat_ws");
        // "flink_" prefixed natives exist where Flink semantics diverge from Spark's:
        //   REPLACE: Flink uses Java String.replace, so an empty search string inserts.
        //   SUBSTR:  Flink returns NULL for a negative length and '' for an out-of-range
        //            negative position.
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.REPLACE, "flink_replace");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.SUBSTR, "flink_substr");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.INSTR, "instr");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.RPAD, "rpad");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.LPAD, "lpad");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.REPEAT, "repeat");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.FROM_BASE64, "unbase64");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.IS_ALPHA, "is_alpha");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.IS_DECIMAL, "is_decimal");

        RexNodeUtil.specialHandlerMap.put(SpecialExprType.REGEXP_EXTRACT, StringExprHandlers::handleRegexpExtract);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.SPLIT_INDEX, StringExprHandlers::handleSplitIndex);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.COUNT_CHAR, StringExprHandlers::handleCountChar);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.LOWER, StringExprHandlers::handleLower);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.CHAR_LENGTH, StringExprHandlers::handleCharLength);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.OVERLAY, StringExprHandlers::handleOverlay);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.LIKE, StringExprHandlers::handleLike);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.BIN, StringExprHandlers::handleBin);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.HEX, StringExprHandlers::handleHex);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.UUID, StringExprHandlers::handleUuid);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.CONCAT, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.CONCAT_WS, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.REPLACE, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.SUBSTR, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.INSTR, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.RPAD, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.LPAD, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.REPEAT, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.FROM_BASE64, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.IS_ALPHA, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.IS_DECIMAL, RexNodeUtil::handleSimpleFunction);
    }

    static Map<String, Object> handleRegexpExtract(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "regex_extract_null");

        List<Map<String, Object>> regArgs = new ArrayList<>();
        regArgs.add(RexNodeUtil.buildJsonMap(operands.get(0)));
        regArgs.add(RexNodeUtil.buildJsonMap(operands.get(1)));
        regArgs.add(RexNodeUtil.buildJsonMap(operands.get(2)));
        jsonMap.put("arguments", regArgs);
        LOG.info("The expression is {} ", rexCall.toString());
        return jsonMap;
    }

    static Map<String, Object> handleSplitIndex(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        // todo:check VARCHAR(length) mapping to returnTypeID
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "SplitIndex");

        List<Map<String, Object>> stringList = new ArrayList<>();
        stringList.add(RexNodeUtil.buildJsonMap(operands.get(0)));
        stringList.add(RexNodeUtil.buildJsonMap(operands.get(1)));
        stringList.add(RexNodeUtil.buildJsonMap(operands.get(2)));

        LOG.info("List is {}", stringList.toString());
        jsonMap.put("arguments", stringList);
        LOG.info("The expresssion is {} ", rexCall.toString());
        return jsonMap;
    }

    static Map<String, Object> handleCountChar(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "CountChar");

        List<Map<String, Object>> stringList = new ArrayList<>();
        stringList.add(RexNodeUtil.buildJsonMap(operands.get(0)));
        stringList.add(RexNodeUtil.buildJsonMap(operands.get(1)));

        LOG.info("List is {}", stringList.toString());
        jsonMap.put("arguments", stringList);
        LOG.info("The expresssion is {} ", rexCall.toString());
        return jsonMap;
    }

    static Map<String, Object> handleLower(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "lower");
        List<Map<String, Object>> lowerArgList = new ArrayList<>();
        lowerArgList.add(RexNodeUtil.buildJsonMap(operands.get(0)));
        jsonMap.put("arguments", lowerArgList);
        return jsonMap;
    }

    static Map<String, Object> handleCharLength(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "char_length");
        List<Map<String, Object>> charLengthArgList = new ArrayList<>();
        charLengthArgList.add(RexNodeUtil.buildJsonMap(operands.get(0)));
        jsonMap.put("arguments", charLengthArgList);
        return jsonMap;
    }

    static Map<String, Object> handleLike(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // LIKE: 2-arg native via LikeFunction (vectorized); 3-arg ESCAPE -> INVALID fallback.
        // NOT LIKE arrives as NOT(LIKE(..)) via sql2rel convertlet, reuses UNARY/NOT branch.
        if (operands.size() != 2) {
            jsonMap.put("exprType", "INVALID");
            return jsonMap;
        }
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "LIKE");
        List<Map<String, Object>> likeArgList = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            Map<String, Object> argMap = RexNodeUtil.buildJsonMap(operands.get(i));
            RexNodeUtil.normalizeCharLiteralToVarchar(argMap);
            likeArgList.add(argMap);
        }
        jsonMap.put("arguments", likeArgList);
        return jsonMap;
    }

    static Map<String, Object> handleOverlay(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // OVERLAY(string1 PLACING string2 FROM integer1 [FOR integer2]) -> string.
        // Calcite lowers PLACING/FROM/FOR syntax to a RexCall named "OVERLAY" with
        // operands [string1, string2, integer1] (3-arg, FOR omitted) or
        // [string1, string2, integer1, integer2] (4-arg). Maps to the vectorized
        // "overlay" registered in OmniOperatorJIT vectorization as
        // {OMNI_VARCHAR, OMNI_VARCHAR, OMNI_INT, OMNI_INT} -> OMNI_VARCHAR
        // (functions/String.h, OverlayFunction). pos is 1-based, aligned with Flink.
        // The vectorized layer only registers the 4-arg overload; when FOR is
        // omitted, Flink defaults length to CHAR_LENGTH(string2). The native
        // OverlayFunction treats len < 0 as "use replace string length" (Unicode
        // chars), so we synthesize len = -1 to express that default, which is
        // semantically equivalent to Flink's behavior.
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "overlay");
        List<Map<String, Object>> overlayArgs = new ArrayList<>();
        Map<String, Object> overlayInputArg = RexNodeUtil.buildJsonMap(operands.get(0));
        RexNodeUtil.normalizeCharLiteralToVarchar(overlayInputArg);
        overlayArgs.add(overlayInputArg);
        Map<String, Object> overlayReplaceArg = RexNodeUtil.buildJsonMap(operands.get(1));
        RexNodeUtil.normalizeCharLiteralToVarchar(overlayReplaceArg);
        overlayArgs.add(overlayReplaceArg);
        Map<String, Object> overlayPosArg = RexNodeUtil.buildJsonMap(operands.get(2));
        overlayArgs.add(overlayPosArg);
        Map<String, Object> overlayLenArg;
        if (operands.size() >= 4) {
            overlayLenArg = RexNodeUtil.buildJsonMap(operands.get(3));
        } else {
            // Flink default: omitted FOR length == CHAR_LENGTH(string2).
            // Native treats len < 0 as "use replace length", so -1 expresses it.
            overlayLenArg = new LinkedHashMap<>();
            overlayLenArg.put("exprType", "LITERAL");
            overlayLenArg.put("dataType", RexNodeUtil.RexTypeToIdMap.get("INTEGER"));
            overlayLenArg.put("isNull", false);
            overlayLenArg.put("value", -1);
        }
        overlayArgs.add(overlayLenArg);
        jsonMap.put("arguments", overlayArgs);
        LOG.info("The OVERLAY expression is {} ", rexCall.toString());
        return jsonMap;
    }

    static Map<String, Object> handleBin(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // BIN(integer) -> VARCHAR (binary string). Maps to native "bin" ({INT}/{LONG}).
        // Flink derives the return type as VARCHAR with default precision (Integer.MAX_VALUE), so
        // the return type is set directly to avoid emitting that width.
        jsonMap.put("exprType", "FUNCTION");
        jsonMap.put("returnType", RexNodeUtil.RexTypeToIdMap.get("VARCHAR"));
        jsonMap.put("function_name", "bin");
        List<Map<String, Object>> binArgs = new ArrayList<>();
        Map<String, Object> binInputArg = RexNodeUtil.buildJsonMap(operands.get(0));
        binArgs.add(binInputArg);
        jsonMap.put("arguments", binArgs);
        return jsonMap;
    }

    static Map<String, Object> handleHex(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // HEX(numeric|string) -> VARCHAR (hex string). Maps to native "hex".
        // Native numeric overload is registered on {OMNI_LONG} only (HexBigintFunction
        // takes int64_t), so an INT-family input that is not BIGINT must be CAST to
        // BIGINT to match the native signature. String inputs ({VARCHAR}/{CHAR}) map
        // directly; normalizeCharLiteralToVarchar coerces CHAR literals to OMNI_VARCHAR.
        jsonMap.put("exprType", "FUNCTION");
        jsonMap.put("returnType", RexNodeUtil.RexTypeToIdMap.get("VARCHAR"));
        jsonMap.put("function_name", "hex");
        List<Map<String, Object>> hexArgs = new ArrayList<>();
        Map<String, Object> hexInputArg = RexNodeUtil.buildJsonMap(operands.get(0));
        RexNodeUtil.normalizeCharLiteralToVarchar(hexInputArg);
        SqlTypeName hexInputTypeName = operands.get(0).getType().getSqlTypeName();
        if (hexInputTypeName == SqlTypeName.INTEGER
                || hexInputTypeName == SqlTypeName.TINYINT
                || hexInputTypeName == SqlTypeName.SMALLINT) {
            // Wrap the arg in CAST(... AS BIGINT) so native hex({LONG}) resolves.
            Map<String, Object> hexCastArg = new LinkedHashMap<>();
            hexCastArg.put("exprType", "FUNCTION");
            hexCastArg.put("returnType", RexNodeUtil.RexTypeToIdMap.get("BIGINT"));
            hexCastArg.put("function_name", "CAST");
            hexCastArg.put("expr", hexInputArg);
            List<Map<String, Object>> hexCastArgList = new ArrayList<>();
            hexCastArgList.add(hexInputArg);
            hexCastArg.put("arguments", hexCastArgList);
            hexArgs.add(hexCastArg);
        } else {
            hexArgs.add(hexInputArg);
        }
        jsonMap.put("arguments", hexArgs);
        return jsonMap;
    }

    static Map<String, Object> handleUuid(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // UUID() -> VARCHAR (RFC4122 v4). 0-arg non-deterministic; maps to native "uuid" (0-arg overload).
        // Flink types UUID() as CHAR(36), so setDataType would emit OMNI_CHAR and miss the native
        // {}->OMNI_VARCHAR signature; the return type is pinned to VARCHAR instead.
        jsonMap.put("exprType", "FUNCTION");
        jsonMap.put("returnType", RexNodeUtil.RexTypeToIdMap.get("VARCHAR"));
        jsonMap.put("function_name", "uuid");
        jsonMap.put("arguments", new ArrayList<>());
        return jsonMap;
    }
}
