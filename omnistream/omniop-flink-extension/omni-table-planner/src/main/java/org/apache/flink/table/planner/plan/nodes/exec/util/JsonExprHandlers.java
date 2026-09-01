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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.nodes.exec.util.RexNodeUtil.SpecialExprType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * JSON-related special-expression handlers extracted from {@link RexNodeUtil}.
 *
 * <p>Each handler builds the OmniOperator JSON for one JSON function and is wired into
 * {@link RexNodeUtil#specialHandlerMap} by {@link #register()}. Keeping every JSON function in this
 * file means adding/adjusting a JSON expression only touches this file (plus the shared
 * {@link SpecialExprType} enum), avoiding conflicts on the central {@code RexNodeUtil} maps.
 */
final class JsonExprHandlers {
    private static final Logger LOG = LoggerFactory.getLogger(JsonExprHandlers.class);

    private JsonExprHandlers() {
    }

    /** Register operator names, native function names and handlers for all JSON expressions. */
    static void register() {
        RexNodeUtil.specialOperatorMap.put("JSON_VALUE", SpecialExprType.JSON_VALUE);
        RexNodeUtil.specialOperatorMap.put("JSON_QUERY", SpecialExprType.JSON_QUERY);
        RexNodeUtil.specialOperatorMap.put("JSON_EXISTS", SpecialExprType.JSON_EXISTS);
        RexNodeUtil.specialOperatorMap.put("JSON_SPLIT", SpecialExprType.JSON_SPLIT);
        RexNodeUtil.specialOperatorMap.put("JSON_STRING", SpecialExprType.JSON_STRING);
        RexNodeUtil.specialOperatorMap.put("JSON_ARRAY", SpecialExprType.JSON_ARRAY);
        RexNodeUtil.specialOperatorMap.put("JSON_OBJECT", SpecialExprType.JSON_OBJECT);
        RexNodeUtil.specialOperatorMap.put("IS JSON VALUE", SpecialExprType.IS_JSON_VALUE);
        RexNodeUtil.specialOperatorMap.put("IS JSON SCALAR", SpecialExprType.IS_JSON_SCALAR);
        RexNodeUtil.specialOperatorMap.put("IS JSON ARRAY", SpecialExprType.IS_JSON_ARRAY);
        RexNodeUtil.specialOperatorMap.put("IS JSON OBJECT", SpecialExprType.IS_JSON_OBJECT);
        RexNodeUtil.udfOperatorMap.put("jsontest", SpecialExprType.JSON_SPLIT);

        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.IS_JSON_VALUE, "is_json_value");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.IS_JSON_SCALAR, "is_json_scalar");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.IS_JSON_ARRAY, "is_json_array");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.IS_JSON_OBJECT, "is_json_object");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.JSON_STRING, "json_string");

        RexNodeUtil.specialHandlerMap.put(SpecialExprType.JSON_VALUE, JsonExprHandlers::handleJsonValue);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.JSON_QUERY, JsonExprHandlers::handleJsonQuery);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.JSON_EXISTS, JsonExprHandlers::handleJsonExists);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.JSON_SPLIT, JsonExprHandlers::handleJsonSplit);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.JSON_ARRAY, JsonExprHandlers::handleJsonArray);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.JSON_OBJECT, JsonExprHandlers::handleJsonObject);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.IS_JSON_VALUE, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.IS_JSON_SCALAR, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.IS_JSON_ARRAY, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.IS_JSON_OBJECT, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.JSON_STRING, RexNodeUtil::handleSimpleFunction);
    }

    static Map<String, Object> handleJsonValue(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "json_value");

        List<Map<String, Object>> jsonArgs = new ArrayList<>();
        jsonArgs.add(RexNodeUtil.buildJsonMap(operands.get(0))); // json input
        jsonArgs.add(RexNodeUtil.buildJsonMap(operands.get(1))); // path expression

        // Parse ON EMPTY behavior (operands 2-4)
        if (operands.size() > 2) {
            Map<String, Object> emptyBehavior = parseBehaviorOperands(operands, 2, "emptyBehavior");
            if (emptyBehavior != null) {
                jsonMap.put("emptyBehavior", emptyBehavior);
            }
        }

        // Parse ON ERROR behavior (operands 5-7)
        if (operands.size() > 5) {
            Map<String, Object> errorBehavior = parseBehaviorOperands(operands, 5, "errorBehavior");
            if (errorBehavior != null) {
                jsonMap.put("errorBehavior", errorBehavior);
            }
        }

        jsonMap.put("arguments", jsonArgs);
        LOG.info("The JSON_VALUE expression is {} ", rexCall.toString());
        return jsonMap;
    }

    static Map<String, Object> handleJsonQuery(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        if (operands.size() != 2 && operands.size() != 3 && operands.size() != 5) {
            LOG.warn("JSON_QUERY expects 2, 3, or 5 operands, but got {}", operands.size());
            jsonMap.put("exprType", "INVALID");
            return jsonMap;
        }

        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "json_query");

        List<Map<String, Object>> queryArgs = new ArrayList<>();
        queryArgs.add(RexNodeUtil.buildJsonMap(operands.get(0)));
        queryArgs.add(RexNodeUtil.buildJsonMap(operands.get(1)));

        if (operands.size() > 2) {
            Map<String, Object> wrapperBehavior = parseJsonQueryWrapperOperand(operands.get(2));
            if (wrapperBehavior == null) {
                LOG.warn("Failed to parse JSON_QUERY wrapper behavior from operand {}", operands.get(2));
                jsonMap.put("exprType", "INVALID");
                return jsonMap;
            }
            jsonMap.put("wrapperBehavior", wrapperBehavior);
        }

        if (operands.size() > 3) {
            Map<String, Object> emptyBehavior = parseJsonQueryBehaviorOperand(operands.get(3));
            if (emptyBehavior == null) {
                LOG.warn("Failed to parse JSON_QUERY empty behavior from operand {}", operands.get(3));
                jsonMap.put("exprType", "INVALID");
                return jsonMap;
            }
            jsonMap.put("emptyBehavior", emptyBehavior);
        }

        if (operands.size() > 4) {
            Map<String, Object> errorBehavior = parseJsonQueryBehaviorOperand(operands.get(4));
            if (errorBehavior == null) {
                LOG.warn("Failed to parse JSON_QUERY error behavior from operand {}", operands.get(4));
                jsonMap.put("exprType", "INVALID");
                return jsonMap;
            }
            jsonMap.put("errorBehavior", errorBehavior);
        }

        jsonMap.put("arguments", queryArgs);
        LOG.info("The JSON_QUERY expression is {} ", rexCall.toString());
        return jsonMap;
    }

    static Map<String, Object> handleJsonExists(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // JSON_EXISTS(jsonValue, path [, { TRUE | FALSE | UNKNOWN | ERROR } ON ERROR]) -> BOOLEAN.
        // Calcite models ON ERROR as a 3rd SYMBOL literal operand (see JsonExistsConverter).
        // The native json_exists takes it as an optional 3rd VARCHAR literal argument
        // (Flink itself passes ON ERROR as a method parameter, not operator identity), so we
        // synthesize a VARCHAR literal for the SYMBOL flag. When omitted, native defaults to
        // FALSE (2-arg form). NULL input -> NULL output (Flink argsNullable=false short-circuit).
        if (operands.size() != 2 && operands.size() != 3) {
            LOG.warn("JSON_EXISTS expects 2 or 3 operands, but got {}", operands.size());
            jsonMap.put("exprType", "INVALID");
            return jsonMap;
        }

        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "json_exists");

        List<Map<String, Object>> jsonExistsArgs = new ArrayList<>();
        Map<String, Object> jsonInputArg = RexNodeUtil.buildJsonMap(operands.get(0));
        RexNodeUtil.normalizeCharLiteralToVarchar(jsonInputArg);
        jsonExistsArgs.add(jsonInputArg); // json value
        Map<String, Object> pathArg = RexNodeUtil.buildJsonMap(operands.get(1));
        RexNodeUtil.normalizeCharLiteralToVarchar(pathArg);
        jsonExistsArgs.add(pathArg); // path expression

        // Optional ON ERROR behavior (operand 2): SYMBOL literal -> synthesized VARCHAR literal.
        if (operands.size() == 3) {
            String onErrorName = RexNodeUtil.getSymbolLiteralName(operands.get(2));
            if (onErrorName == null
                    || (!onErrorName.equalsIgnoreCase("TRUE")
                            && !onErrorName.equalsIgnoreCase("FALSE")
                            && !onErrorName.equalsIgnoreCase("UNKNOWN")
                            && !onErrorName.equalsIgnoreCase("ERROR"))) {
                LOG.warn("JSON_EXISTS has unsupported ON ERROR behavior: {}", onErrorName);
                jsonMap.put("exprType", "INVALID");
                return jsonMap;
            }
            // Native expects the canonical uppercase name (matches JsonExistsOnError).
            onErrorName = onErrorName.toUpperCase(Locale.ROOT);
            Map<String, Object> onErrorArg = new LinkedHashMap<>();
            onErrorArg.put("exprType", "LITERAL");
            onErrorArg.put("dataType", RexNodeUtil.RexTypeToIdMap.get("VARCHAR"));
            onErrorArg.put("isNull", false);
            onErrorArg.put("value", onErrorName);
            onErrorArg.put("width", onErrorName.length());
            jsonExistsArgs.add(onErrorArg);
        }

        jsonMap.put("arguments", jsonExistsArgs);
        LOG.info("The JSON_EXISTS expression is {} ", rexCall.toString());
        return jsonMap;
    }

    static Map<String, Object> handleJsonSplit(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // JsonSplit is a ScalarFunction: eval(String input) -> String
        // Signature: 1 argument of type STRING, returns STRING
        // The UDF registration name is "jsontest", which is mapped to JSON_SPLIT here
        if (operands.size() != 1) {
            LOG.warn("JSON_SPLIT expects exactly 1 argument, but got {}", operands.size());
            jsonMap.put("exprType", "INVALID");
            return jsonMap;
        }
        // Validate input type is STRING (VARCHAR)
        SqlTypeName inputType = operands.get(0).getType().getSqlTypeName();
        if (inputType != SqlTypeName.VARCHAR && inputType != SqlTypeName.CHAR) {
            LOG.warn("JSON_SPLIT expects STRING input, but got {}", inputType);
            jsonMap.put("exprType", "INVALID");
            return jsonMap;
        }
        // Validate return type is STRING (VARCHAR)
        SqlTypeName jsonSplitReturnType = rexCall.getType().getSqlTypeName();
        if (jsonSplitReturnType != SqlTypeName.VARCHAR && jsonSplitReturnType != SqlTypeName.CHAR) {
            LOG.warn("JSON_SPLIT expects STRING return type, but got {}", jsonSplitReturnType);
            jsonMap.put("exprType", "INVALID");
            return jsonMap;
        }

        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "json_split");

        List<Map<String, Object>> splitArgs = new ArrayList<>();
        splitArgs.add(RexNodeUtil.buildJsonMap(operands.get(0))); // json array input

        jsonMap.put("arguments", splitArgs);

        LOG.info("The JSON_SPLIT expression is {} ", rexCall.toString());
        return jsonMap;
    }

    /**
     * JSON_ARRAY([value]* [ { NULL | ABSENT } ON NULL ]) -> native json_array.
     *
     * Calcite RexCall layout: operand[0] is the ON NULL symbol clause
     * (NULL_ON_NULL / ABSENT_ON_NULL), operands[1..] are the value expressions (raw, NOT
     * wrapped by JSON_STRING; the native json_array serializes each value via json_string).
     *
     * Native argument layout (aligned with json_object / JsonArrayFunction::Apply):
     *   arg[0]    : VARCHAR literal "NULL" or "ABSENT" (ON NULL behavior)
     *   arg[1..]  : each value forwarded via buildJsonMap, in call order
     */
    static Map<String, Object> handleJsonArray(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        if (operands.isEmpty()) {
            LOG.warn("JSON_ARRAY expects at least the ON NULL clause operand, but got {}", operands.size());
            jsonMap.put("exprType", "INVALID");
            return jsonMap;
        }

        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "json_array");

        List<Map<String, Object>> arrayArgs = new ArrayList<>();
        // arg[0]: ON NULL flag as a synthetic VARCHAR literal ("NULL" / "ABSENT").
        arrayArgs.add(buildOnNullFlagLiteral(operands.get(0)));
        // arg[1..]: value expressions, preserving call order.
        for (int i = 1; i < operands.size(); i++) {
            arrayArgs.add(RexNodeUtil.buildJsonMap(operands.get(i)));
        }
        jsonMap.put("arguments", arrayArgs);

        LOG.info("The JSON_ARRAY expression is {} ", rexCall.toString());
        return jsonMap;
    }

    /**
     * JSON_OBJECT([[KEY] key VALUE value]* [ { NULL | ABSENT } ON NULL ]) -> native json_object.
     *
     * Calcite RexCall layout: operand[0] is the ON NULL symbol clause, then (key, value) pairs:
     * operands[1]=key1, [2]=value1, [3]=key2, [4]=value2, ... Values are raw (NOT wrapped by
     * JSON_STRING); the native json_object serializes each value via json_string, and inserts
     * nested JSON_OBJECT / JSON_ARRAY values as raw nodes (detected in OmniOperator's FuncExpr
     * constructor by inspecting the value child).
     *
     * Native argument layout (aligned with json_object / JsonObjectFunction::Apply):
     *   arg[0]         : VARCHAR literal "NULL" or "ABSENT" (ON NULL behavior)
     *   arg[1,3,5,...] : key (VARCHAR literal)
     *   arg[2,4,6,...] : value (any supported type), in call order
     */
    static Map<String, Object> handleJsonObject(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        if (operands.isEmpty() || (operands.size() - 1) % 2 != 0) {
            LOG.warn("JSON_OBJECT expects the ON NULL clause plus key/value pairs, but got {} operands",
                    operands.size());
            jsonMap.put("exprType", "INVALID");
            return jsonMap;
        }

        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "json_object");

        List<Map<String, Object>> objectArgs = new ArrayList<>();
        // arg[0]: ON NULL flag as a synthetic VARCHAR literal ("NULL" / "ABSENT").
        objectArgs.add(buildOnNullFlagLiteral(operands.get(0)));
        // arg[1..]: key/value pairs, preserving call order.
        for (int i = 1; i < operands.size(); i++) {
            objectArgs.add(RexNodeUtil.buildJsonMap(operands.get(i)));
        }
        jsonMap.put("arguments", objectArgs);

        LOG.info("The JSON_OBJECT expression is {} ", rexCall.toString());
        return jsonMap;
    }

    /**
     * Build a synthetic VARCHAR literal carrying the ON NULL behavior ("NULL" / "ABSENT") for the
     * JSON constructor functions. The symbol name contains "ABSENT" for ABSENT ON NULL; anything
     * else (including NULL_ON_NULL) maps to "NULL". The native side (JsonArrayFunction /
     * JsonObjectFunction IsAbsentOnNull) only recognizes "ABSENT", defaulting to NULL ON NULL.
     */
    private static Map<String, Object> buildOnNullFlagLiteral(RexNode onNullNode) {
        String symbolName = RexNodeUtil.getSymbolLiteralName(onNullNode);
        String flag = (symbolName != null && symbolName.toUpperCase(Locale.ROOT).contains("ABSENT"))
                ? "ABSENT" : "NULL";
        Map<String, Object> flagMap = new LinkedHashMap<>();
        flagMap.put("exprType", "LITERAL");
        flagMap.put("dataType", RexNodeUtil.RexTypeToIdMap.get("VARCHAR"));
        flagMap.put("width", flag.length());
        flagMap.put("isNull", false);
        flagMap.put("value", flag);
        return flagMap;
    }

    /**
     * Parse behavior operands for JSON_VALUE ON EMPTY/ERROR clauses
     *
     * @param operands The operand list from RexCall
     * @param startIndex The start index for behavior parsing
     * @param behaviorKey The key name for JSON output
     * @return Map containing behavior type and optional default value
     */
    private static Map<String, Object> parseBehaviorOperands(List<RexNode> operands, int startIndex, String behaviorKey) {
        if (operands.size() <= startIndex) {
            return null;
        }

        Map<String, Object> behaviorMap = new LinkedHashMap<>();

        try {
            RexNode behaviorNode = operands.get(startIndex);

            // Check if it's a literal (NULL, ERROR, or DEFAULT flag)
            if (behaviorNode instanceof RexLiteral) {
                RexLiteral behaviorLiteral = (RexLiteral) behaviorNode;
                String behaviorName = behaviorLiteral.getValue().toString();

                // Map behavior names
                if ("NULL".equalsIgnoreCase(behaviorName)) {
                    behaviorMap.put("type", "NULL");
                } else if ("ERROR".equalsIgnoreCase(behaviorName)) {
                    behaviorMap.put("type", "ERROR");
                } else if ("DEFAULT".equalsIgnoreCase(behaviorName)) {
                    behaviorMap.put("type", "DEFAULT");

                    // For DEFAULT, get the next operand as the default value
                    if (operands.size() > startIndex + 1) {
                        Map<String, Object> defaultValue = RexNodeUtil.buildJsonMap(operands.get(startIndex + 1));
                        behaviorMap.put("defaultValue", defaultValue);
                    }
                }
            }

            return behaviorMap;
        } catch (Exception e) {
            LOG.warn("Failed to parse behavior operands for {}: {}", behaviorKey, e.getMessage());
            return null;
        }
    }

    private static Map<String, Object> parseJsonQueryWrapperOperand(RexNode wrapperNode) {
        String wrapperName = RexNodeUtil.getSymbolLiteralName(wrapperNode);
        if (wrapperName == null) {
            return null;
        }

        Map<String, Object> wrapperMap = new LinkedHashMap<>();
        if ("WITHOUT_ARRAY".equalsIgnoreCase(wrapperName)
                || "WITH_CONDITIONAL_ARRAY".equalsIgnoreCase(wrapperName)
                || "WITH_UNCONDITIONAL_ARRAY".equalsIgnoreCase(wrapperName)) {
            wrapperMap.put("type", wrapperName.toUpperCase(Locale.ROOT));
            return wrapperMap;
        }
        return null;
    }

    private static Map<String, Object> parseJsonQueryBehaviorOperand(RexNode behaviorNode) {
        String behaviorName = RexNodeUtil.getSymbolLiteralName(behaviorNode);
        if (behaviorName == null) {
            return null;
        }

        Map<String, Object> behaviorMap = new LinkedHashMap<>();
        if ("NULL".equalsIgnoreCase(behaviorName)
                || "ERROR".equalsIgnoreCase(behaviorName)
                || "EMPTY_ARRAY".equalsIgnoreCase(behaviorName)
                || "EMPTY_OBJECT".equalsIgnoreCase(behaviorName)) {
            behaviorMap.put("type", behaviorName.toUpperCase(Locale.ROOT));
            return behaviorMap;
        }
        return null;
    }
}
