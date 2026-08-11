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
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Sarg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class RexNodeUtil {
    public static final Map<String, Integer> RexTypeToIdMap = new HashMap<>();
    public static final Map<String, SpecialExprType> specialOperatorMap = new HashMap<>();
    public static final Map<SpecialExprType, String> simpleFunctionNameMap = new HashMap<>();
    public static final Map<String, SpecialExprType> udfOperatorMap = new HashMap<>();
    public static final Map<String, UnaryExprType> unaryOperatorMap = new HashMap<>();
    public static final Map<String, BinaryExprType> binaryOperatorMap = new HashMap<>();
    // Strategy registry: maps a SpecialExprType to the handler that builds its JSON.
    // Handlers live in per-category classes (JsonExprHandlers, DateTimeExprHandlers,
    // StringExprHandlers, MathExprHandlers) plus the core structural handlers below.
    // Each category self-registers via its register() method (called in the static block),
    // so adding a function within a category touches only that category's file instead of
    // this shared class (reduces multi-developer merge conflicts).
    public static final Map<SpecialExprType, SpecialExprHandler> specialHandlerMap = new HashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(RexNodeUtil.class);
    public static HashMap<Integer, Integer> accessIndexMap = new HashMap<>();

    static {
        // This map converts Calcite RexNode SqlTypeName to OmniType Id
        RexTypeToIdMap.put("INT", 1);
        RexTypeToIdMap.put("INTEGER", 1);
        RexTypeToIdMap.put("BIGINT", 2);
        RexTypeToIdMap.put("DOUBLE", 3);
        RexTypeToIdMap.put("BOOLEAN", 4);
        RexTypeToIdMap.put("TINYINT", 5);
        RexTypeToIdMap.put("SMALLINT", 5);
        RexTypeToIdMap.put("DECIMAL", 6);
        RexTypeToIdMap.put("DECIMAL128", 7);
        RexTypeToIdMap.put("DATE", 8);
        RexTypeToIdMap.put("TIME", 10);
        RexTypeToIdMap.put("TIMESTAMP", 12);
        RexTypeToIdMap.put("INTERVAL_MONTH", 13);
        RexTypeToIdMap.put("INTERVAL_DAY", 14);
        RexTypeToIdMap.put("INTERVAL_SECOND", 14);
        RexTypeToIdMap.put("VARCHAR", 15);
        RexTypeToIdMap.put("CHAR", 16);
        RexTypeToIdMap.put("ROW", 17);
        RexTypeToIdMap.put("INVALID", 18);
        RexTypeToIdMap.put("TIME_WITHOUT_TIME_ZONE", 19); // TODO: Is this the same as TIME?
        RexTypeToIdMap.put("TIMESTAMP_WITHOUT_TIME_ZONE", 20); // TODO: Omni's TIMESTAMP uses int64_t, Flink has the possibility of accuracy>3
        RexTypeToIdMap.put("TIMESTAMP_TZ", 21); // TIMESTAMP_WITH_TIMEZONE
        RexTypeToIdMap.put("TIMESTAMP_WITH_LOCAL_TIME_ZONE", 24);
        RexTypeToIdMap.put("ARRAY", 23);
        RexTypeToIdMap.put("MULTISET", 24);
        RexTypeToIdMap.put("MAP", 25);
    }

    static {
        // Core structural operators handled in this class (see the handlers at the bottom).
        specialOperatorMap.put("CASE", SpecialExprType.SWITCH);
        specialOperatorMap.put("SEARCH", SpecialExprType.SEARCH);
        specialOperatorMap.put("HASH_CODE", SpecialExprType.HASH_CODE);
        specialOperatorMap.put("IS NOT NULL", SpecialExprType.IS_NOT_NULL);
        specialOperatorMap.put("CAST", SpecialExprType.CAST);
        specialOperatorMap.put("AND", SpecialExprType.AND);
        specialOperatorMap.put("OR", SpecialExprType.OR);
        specialOperatorMap.put("IF", SpecialExprType.IF);
        specialOperatorMap.put("COALESCE", SpecialExprType.COALESCE);
        // IFNULL reuses the COALESCE native path (equivalent to 2-arg COALESCE)
        specialOperatorMap.put("IFNULL", SpecialExprType.COALESCE);
    }

    static {
        unaryOperatorMap.put("-", UnaryExprType.NEGATION);
        unaryOperatorMap.put("IS TRUE", UnaryExprType.IS_TRUE);
        unaryOperatorMap.put("NOT", UnaryExprType.NOT);
    }

    static {
        binaryOperatorMap.put("OR", BinaryExprType.OR);
        binaryOperatorMap.put("AND", BinaryExprType.AND);
        binaryOperatorMap.put("+", BinaryExprType.ADD);
        binaryOperatorMap.put("-", BinaryExprType.SUBTRACT);
        binaryOperatorMap.put("*", BinaryExprType.MULTIPLY);
        binaryOperatorMap.put("/", BinaryExprType.DIVIDE);
        binaryOperatorMap.put("MOD", BinaryExprType.MODULUS);
        binaryOperatorMap.put(">", BinaryExprType.GREATER_THAN);
        binaryOperatorMap.put(">=", BinaryExprType.GREATER_THAN_OR_EQUAL);
        binaryOperatorMap.put("<", BinaryExprType.LESS_THAN);
        binaryOperatorMap.put("<=", BinaryExprType.LESS_THAN_OR_EQUAL);
        binaryOperatorMap.put("=", BinaryExprType.EQUAL);
        binaryOperatorMap.put("<>", BinaryExprType.NOT_EQUAL);
    }

    static {
        // Register the core structural handlers kept in this class.
        specialHandlerMap.put(SpecialExprType.SWITCH, RexNodeUtil::handleSwitch);
        specialHandlerMap.put(SpecialExprType.IF, RexNodeUtil::handleSwitch);
        specialHandlerMap.put(SpecialExprType.COALESCE, RexNodeUtil::handleCoalesce);
        specialHandlerMap.put(SpecialExprType.SEARCH, RexNodeUtil::handleSearch);
        specialHandlerMap.put(SpecialExprType.HASH_CODE, RexNodeUtil::handleHashCode);
        specialHandlerMap.put(SpecialExprType.IS_NOT_NULL, RexNodeUtil::handleIsNotNull);
        specialHandlerMap.put(SpecialExprType.AND, RexNodeUtil::handleAnd);
        specialHandlerMap.put(SpecialExprType.OR, RexNodeUtil::handleOr);
        specialHandlerMap.put(SpecialExprType.CAST, RexNodeUtil::handleCast);

        // Category handlers self-register their operator names, native function names and
        // handlers. Adding a function within a category touches only that category's file
        // (plus the shared SpecialExprType enum). Adding a new category = one line here.
        StringExprHandlers.register();
        DateTimeExprHandlers.register();
        MathExprHandlers.register();
        JsonExprHandlers.register();
    }

    private static <T> T resolveOperatorType(Map<String, T> operatorMap, String operatorName) {
        T operatorType = operatorMap.get(operatorName);
        if (operatorType != null) {
            return operatorType;
        }
        return operatorMap.get(operatorName.toUpperCase(Locale.ROOT));
    }

    /**
     * Strategy interface for translating a special RexCall into its OmniOperator JSON map.
     * Implemented by the {@code handleXxx} methods (in this class and the per-category handler
     * classes) and wired up in {@link #specialHandlerMap}. This replaces the former monolithic
     * switch-case so that adding a new special expression does not require editing a shared block.
     */
    @FunctionalInterface
    public interface SpecialExprHandler {
        Map<String, Object> handle(RexCall rexCall, List<RexNode> operands,
                Map<String, Object> jsonMap, SpecialExprType specialType);
    }

    public enum OperatorExprType {
        FIELD_REFERENCE,
        FIELD_ACCESS, // we can parse it, but currently we dont support it in operator-level operations.
        LITERAL,
        UNARY,
        BINARY,
        SPECIAL,
        INVALID
    }

    public enum UnaryExprType {
        NEGATION,
        IS_TRUE,
        NOT
    }


    public enum BinaryExprType {
        // binary logical operators
        OR,
        AND,
        // Arithmetic
        ADD,
        SUBTRACT,
        MULTIPLY,
        DIVIDE,
        MODULUS,
        // Comparison
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        EQUAL,
        NOT_EQUAL
    }

    public enum SpecialExprType {
        SWITCH,
        REGEXP_EXTRACT,
        SEARCH,
        LOWER,
        HASH_CODE,
        SPLIT_INDEX,
        FROM_BASE64,
        CHAR_LENGTH,
        IS_NOT_NULL,
        PROCTIME,
        EXTRACT,
        DATE_FORMAT,
        TO_TIMESTAMP_LTZ,
        TO_TIMESTAMP,
        COUNT_CHAR,
        CAST,
        OTHERS,
        AND,
        OR,
        IF,
        COALESCE,
        JSON_VALUE,
        JSON_QUERY,
        JSON_EXISTS,
        JSON_SPLIT,
        JSON_STRING,
        JSON_ARRAY,
        JSON_OBJECT,
        CURRENT_TIMESTAMP,
        DATE_ADD,
        ROUND,
        GREATEST,
        LEAST,
        CONCAT,
        CONCAT_WS,
        REPLACE,
        SUBSTR,
        INSTR,
        PARSE_URL,
        UNIX_TIMESTAMP,
        FROM_UNIXTIME,
        LIKE,
        TO_DATE,
        RPAD,
        LPAD,
        REPEAT,
        OVERLAY,
        SINH,
        COS,
        COT,
        ASIN,
        ACOS,
        ATAN,
        ATAN2,
        COSH,
        DEGREES,
        SIGN,
        SIN,
        TAN,
        TANH,
        RADIANS,
        PI,
        E,
        RAND,
        RAND_INTEGER,
        UUID,
        BIN,
        HEX,
        TRUNCATE,
        IS_ALPHA,
        IS_DECIMAL,
        IS_JSON_VALUE,
        IS_JSON_SCALAR,
        IS_JSON_ARRAY,
        IS_JSON_OBJECT,
        LEFT,
        RIGHT,
        STR_TO_MAP,
        CONVERT_TZ
    }


    public static Object[] getExpression(RexNode inputExpr) {
        RexNode input = null;
        RexNode value = null;
        if (inputExpr instanceof RexCall) {
            RexCall call = (RexCall) inputExpr;
            LOG.info("Current RexCall name {}", call.getOperator().getName());
            if (binaryOperatorMap.containsKey(call.getOperator().getName())
                    && (binaryOperatorMap.get(call.getOperator().getName()) == BinaryExprType.EQUAL
                    || binaryOperatorMap.get(call.getOperator().getName()) == BinaryExprType.AND)) {
                input = call.operands.get(0);
                value = call.operands.get(1);
                return new Object[]{input, value};
            } else {
                LOG.info("Cannot parse the expression of a CASE operation");
            }
        } else {
            LOG.info("Cannot parse the expression of a CASE operation");
        }
        return new Object[]{input, value};
    }

    public static void setDataType(RexNode rexNode, Map<String, Object> jsonMap, String keyStr) {
        if (rexNode.getType().getSqlTypeName() == SqlTypeName.DECIMAL) {
            // check for decimal precision
            int precision = rexNode.getType().getPrecision();
            if (precision < 19) {
                jsonMap.put(keyStr, 6);
            } else {
                jsonMap.put(keyStr, 7);
            }
            jsonMap.put("precision", precision);
            jsonMap.put("scale", rexNode.getType().getScale());
        } else if (rexNode.getType().getSqlTypeName() == SqlTypeName.CHAR
                || rexNode.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
            int precision = rexNode.getType().getPrecision();
            jsonMap.put(keyStr, RexTypeToIdMap.get(rexNode.getType().getSqlTypeName().toString()));
            jsonMap.put("width", precision);
        } else if (rexNode.getType().getSqlTypeName() == SqlTypeName.DATE) {
            jsonMap.put(keyStr, 1);
        } else if (SqlTypeName.DATETIME_TYPES.contains(rexNode.getType().getSqlTypeName())) {
            jsonMap.put(keyStr, 2);
        } else if (SqlTypeName.INTERVAL_TYPES.contains(rexNode.getType().getSqlTypeName())) {
            jsonMap.put(keyStr, 1);
        } else {
            jsonMap.put(keyStr, RexTypeToIdMap.get(rexNode.getType().getSqlTypeName().toString()));
        }
    }

    /**
     * Calcite may type string literals as CHAR(n); OmniOperator function signatures expect
     * OMNI_VARCHAR. Normalize CHAR literals in the JSON subtree so JSONParser lookup matches.
     * Package-private so the per-category handler classes can reuse it.
     */
    static void normalizeCharLiteralToVarchar(Map<String, Object> jsonMap) {
        if (jsonMap == null) {
            return;
        }
        if ("LITERAL".equals(jsonMap.get("exprType"))
                && RexTypeToIdMap.get("CHAR").equals(jsonMap.get("dataType"))) {
            jsonMap.put("dataType", RexTypeToIdMap.get("VARCHAR"));
        }
    }

    // Sarg endpoint (NlsString for VARCHAR/CHAR, TimestampString for TIMESTAMP) -> primitive,
    // so OmniOperator ParseJSONLiteral reads a plain string/number not a structured object.
    public static Object extractSargEndpoint(Object endpoint) {
        if (endpoint == null) {
            return null;
        }
        if (endpoint instanceof NlsString) {
            return ((NlsString) endpoint).getValue();
        }
        if (endpoint instanceof TimestampString) {
            return ((TimestampString) endpoint).getMillisSinceEpoch();
        }
        if (!(endpoint instanceof Number) && !(endpoint instanceof Boolean) && !(endpoint instanceof String)) {
            LOG.info("extractSargEndpoint unhandled class: {}", endpoint.getClass().getName());
        }
        return endpoint;
    }

    public static Map<String, Object> buildJsonMap(RexNode rexNode) {
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        if (rexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rexNode;
            List<RexNode> operands = rexCall.getOperands();
            SqlOperator operator = rexCall.getOperator();
            String operatorName = operator.getName();
            BinaryExprType binaryType = resolveOperatorType(binaryOperatorMap, operatorName);
            UnaryExprType unaryType = resolveOperatorType(unaryOperatorMap, operatorName);
            SpecialExprType udfType = resolveOperatorType(udfOperatorMap, operatorName);
            SpecialExprType specialType = udfType != null ? udfType : resolveOperatorType(specialOperatorMap, operatorName);
            LOG.info("Current rexNode is {}", rexCall.toString());
            if (rexCall.operands.size() == 2 && binaryType != null) {
                jsonMap.put("exprType",  OperatorExprType.BINARY.name());
                setDataType(rexCall,jsonMap, "returnType");
                jsonMap.put("operator", binaryType.name());
                Map<String, Object> leftMap = buildJsonMap(operands.get(0));
                jsonMap.put("left", leftMap);
                Map<String, Object> rightMap =  buildJsonMap(operands.get(1));
                jsonMap.put("right", rightMap);
                return jsonMap;
            } else if (rexCall.operands.size() == 1 && unaryType != null) {
                Map<String, Object> childMap = buildJsonMap(operands.get(0));
                if (unaryType.equals(UnaryExprType.IS_TRUE) ||
                        childMap.containsKey("exprType") && childMap.get("exprType").equals(SpecialExprType.SWITCH)) {
                    return childMap;
                }
                jsonMap.put("exprType",  OperatorExprType.UNARY.name());
                setDataType(rexCall,jsonMap, "returnType");
                jsonMap.put("operator", unaryType.name());
                jsonMap.put("expr", childMap);
                return jsonMap;
            } else if (specialType != null) {
                // Strategy dispatch: replaces the former switch(specialType) block.
                SpecialExprHandler handler = specialHandlerMap.get(specialType);
                if (handler == null) {
                    jsonMap.put("exprType", "INVALID");
                    return jsonMap;
                }
                return handler.handle(rexCall, operands, jsonMap, specialType);
            } else {
                LOG.info("The operator {} is not supported", operator.toString());
                LOG.info("The expression is {} ", rexCall.toString());
                jsonMap.put("operator","INVALID");
                return jsonMap;
            }
        } else if (rexNode instanceof RexLiteral) {
            RexLiteral rexLiteral = (RexLiteral) rexNode;
            jsonMap.put("exprType",  OperatorExprType.LITERAL.name());
            setDataType(rexLiteral, jsonMap, "dataType");
            jsonMap.put("isNull", rexLiteral.isNull());
            if (!rexLiteral.isNull()){
                Object value = rexLiteral.getValue2();
                jsonMap.put("value",value);
            }
            // todo: for DECIMAL64D and DECIMAL128, add fields: precision and scale
            return jsonMap;

        } else if (rexNode instanceof RexInputRef){
            RexInputRef inputRef = (RexInputRef) rexNode;
            if (inputRef instanceof RexPatternFieldRef || inputRef instanceof RexTableInputRef){
                // we may parse RexTableInputRef later.
                LOG.info("RexPatternFieldRef/RexTableInputRef is not supported.");
                jsonMap.put("exprType",  OperatorExprType.INVALID.name());
                return jsonMap;
            } else { // deal with $index
                RexInputRef  rexInputRef =(RexInputRef) rexNode;
                jsonMap.put("exprType",  OperatorExprType.FIELD_REFERENCE.name());
                setDataType(rexInputRef, jsonMap, "dataType");
                jsonMap.put("colVal", accessIndexMap.getOrDefault(rexInputRef.hashCode(), rexInputRef.hashCode()));
                // todo: add fields precision and scale for DECIMAL64D and DECIMAL128
                return jsonMap;
            }
        } else if (rexNode instanceof RexFieldAccess){
            RexFieldAccess fieldAccess = (RexFieldAccess) rexNode;
            // Create a field access based on accessIndexMap
            jsonMap.put("exprType",  OperatorExprType.FIELD_REFERENCE.name());
            setDataType(fieldAccess, jsonMap, "dataType");
            int colVal = fieldAccess.getReferenceExpr().hashCode();
            int fieldVal = fieldAccess.getField().getIndex();
            // offset + fieldVal
            jsonMap.put("colVal", accessIndexMap.get(colVal) + fieldVal);
            return jsonMap;
        } else { // todo: we may consider to parse other types of RexNode later.
            SqlKind kind = rexNode.getKind();
            LOG.info("The RexNode is {}", kind);
            jsonMap.put("exprType",  OperatorExprType.INVALID.name());
            LOG.info("The RexNode is not a RexCall/RexInputRef/RexLiteral. It is not recognized.");
            return jsonMap;
        }
    }

    // =========================================================================
    // Core structural handlers kept in this class (wired in specialHandlerMap above).
    // Category-specific handlers live in JsonExprHandlers / DateTimeExprHandlers /
    // StringExprHandlers / MathExprHandlers. The shared handleSimpleFunction below is
    // package-private so those classes can reference it for FUNCTION-forwarding expressions.
    // =========================================================================

    private static Map<String, Object> handleSwitch(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        int numOperands = operands.size();
        jsonMap.put("exprType",  "SWITCH_GENERAL");
        setDataType(rexCall,jsonMap, "returnType");
        // SWITCH([input_expr, condition1], 'result1', [input_expr, condition2], 'result2', 'optional other')
        int numOfCases = (numOperands - 1) / 2;
        jsonMap.put("numOfCases", numOfCases);

        for (int i = 0; i < numOfCases; i++) {
            Map<String, Object> caseMap = new LinkedHashMap<>();
            caseMap.put("when", buildJsonMap(operands.get(i * 2)));
            caseMap.put("result", buildJsonMap(operands.get(i * 2 + 1)));
            caseMap.put("exprType", "WHEN");
            setDataType(operands.get(i * 2 + 1), caseMap, "returnType");
            jsonMap.put("Case"+Integer.toString(i + 1), caseMap);
        }
        // The else
        jsonMap.put("else", buildJsonMap(operands.get(operands.size() - 1)));
        return jsonMap;
    }

    private static Map<String, Object> handleCoalesce(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        if (operands.isEmpty()) {
            LOG.warn("COALESCE expects at least 1 argument, but got {}", operands.size());
            jsonMap.put("exprType", "INVALID");
            return jsonMap;
        }

        if (operands.size() == 1) {
            return buildJsonMap(operands.get(0));
        }

        Map<String, Object> nested = null;
        for (int i = operands.size() - 1; i >= 1; i--) {
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("exprType", "COALESCE");
            setDataType(rexCall, node, "returnType");
            node.put("value1", buildJsonMap(operands.get(i - 1)));
            if (nested == null) {
                node.put("value2", buildJsonMap(operands.get(i)));
            } else {
                node.put("value2", nested);
            }
            nested = node;
        }

        if (nested != null) {
            jsonMap.putAll(nested);
        } else {
            jsonMap.put("exprType", "INVALID");
        }
        return jsonMap;
    }

    private static Map<String, Object> handleSearch(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        RexLiteral searchArg = (RexLiteral) operands.get(1);  // Sarg object
        // Get the Sarg value
        Sarg<?> sarg = ((RexLiteral) searchArg).getValueAs(Sarg.class);
        // Check if its a list or a range
        if (sarg.isPoints()) { // A point list
            jsonMap.put("exprType", "IN");
            setDataType(rexCall,jsonMap, "returnType");

            List<Map<String, Object>> stringList = new ArrayList<>();
            // first argument is the input
            stringList.add(buildJsonMap(operands.get(0)));
            // Extract all elements from the list
            List<?> values = new ArrayList<>(sarg.rangeSet.asRanges().stream()
                    .map(range -> range.lowerEndpoint()).collect(Collectors.toList()));

            for(int i = 0; i < values.size(); i++) {
                Map<String, Object> literalMap = new LinkedHashMap<>();
                literalMap.put("exprType", "LITERAL");
                literalMap.put("isNull", false);
                literalMap.put("value", extractSargEndpoint(values.get(i)));
                setDataType(operands.get(0), literalMap, "dataType");
                stringList.add(literalMap);
            }
            jsonMap.put("arguments", stringList);
        } else { // a range
            // Per-range bounds as [hasLower, hasUpper, lower, upper]; Guava Range not importable (Flink relocates) -> type inferred.
            List<Object[]> rangeInfo = new ArrayList<>();
            sarg.rangeSet.asRanges().forEach(r -> rangeInfo.add(new Object[] {
                r.hasLowerBound(), r.hasUpperBound(),
                r.hasLowerBound() ? r.lowerEndpoint() : null,
                r.hasUpperBound() ? r.upperEndpoint() : null
            }));
            if (rangeInfo.isEmpty()) {
                // Empty Sarg (e.g. SYMMETRIC with an impossible range) evaluates to FALSE
                jsonMap.put("exprType", "LITERAL");
                jsonMap.put("isNull", false);
                jsonMap.put("value", false);
                setDataType(rexCall, jsonMap, "dataType");
            } else {
                Object[] first = rangeInfo.get(0);
                // (-inf..+inf): always TRUE, e.g. reversed-bound NOT BETWEEN.
                boolean alwaysTrue = rangeInfo.size() == 1
                        && !(Boolean) first[0]
                        && !(Boolean) first[1];
                // NOT BETWEEN complement: (-inf..low) U (high..+inf) -> NOT BETWEEN(low, high).
                boolean isComplement = rangeInfo.size() == 2
                        && !(Boolean) first[0]
                        && !(Boolean) rangeInfo.get(1)[1];
                // Normal BETWEEN: single closed range [low..high].
                boolean isClosedRange = rangeInfo.size() == 1
                        && (Boolean) first[0]
                        && (Boolean) first[1];
                if (alwaysTrue) {
                    jsonMap.put("exprType", "LITERAL");
                    jsonMap.put("isNull", false);
                    jsonMap.put("value", true);
                    setDataType(rexCall, jsonMap, "dataType");
                } else if (!isComplement && !isClosedRange) {
                    // Half-bounded or non-complement multi-range Sarg is not representable as BETWEEN.
                    jsonMap.put("exprType", OperatorExprType.INVALID.name());
                } else {
                    // TODO: inclusive, exclusive problem not solved!
                    Object lowerBound = isComplement ? first[3] : first[2];
                    Object upperBound = isComplement ? rangeInfo.get(1)[2] : first[3];

                    Map<String, Object> betweenMap = new LinkedHashMap<>();
                    betweenMap.put("exprType", "BETWEEN");
                    setDataType(rexCall, betweenMap, "returnType");
                    betweenMap.put("value", buildJsonMap(operands.get(0)));

                    Map<String, Object> lowMap = new LinkedHashMap<>();
                    lowMap.put("exprType", "LITERAL");
                    lowMap.put("isNull", false);
                    lowMap.put("value", extractSargEndpoint(lowerBound));
                    setDataType(operands.get(0), lowMap, "dataType");
                    betweenMap.put("lower_bound", lowMap);

                    Map<String, Object> upMap = new LinkedHashMap<>();
                    upMap.put("exprType", "LITERAL");
                    upMap.put("isNull", false);
                    upMap.put("value", extractSargEndpoint(upperBound));
                    setDataType(operands.get(0), upMap, "dataType");
                    betweenMap.put("upper_bound", upMap);

                    if (isComplement) {
                        // NOT BETWEEN: wrap BETWEEN in UNARY NOT
                        jsonMap.put("exprType", OperatorExprType.UNARY.name());
                        setDataType(rexCall, jsonMap, "returnType");
                        jsonMap.put("operator", UnaryExprType.NOT.name());
                        jsonMap.put("expr", betweenMap);
                    } else {
                        jsonMap.putAll(betweenMap);
                    }
                }
            }
        }
        return jsonMap;
    }

    private static Map<String, Object> handleHashCode(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        Integer returnType = RexTypeToIdMap.get(rexCall.getType().getSqlTypeName().toString());
        if (returnType == 1){
            jsonMap.put("function_name", "mm3hash");
        } else {
            jsonMap.put("function_name", "xxhash64");
        }
        jsonMap.put("function_name", "mm3hash");
        setDataType(rexCall,jsonMap, "returnType");
        List<Object> arguments = new LinkedList<>();
        Map<String, Object> seed = new LinkedHashMap<>();
        // Set the seed to 0
        seed.put("value", 0);
        seed.put("isNull", false);
        seed.put("exprType", "LITERAL");
        seed.put("dataType", returnType);
        arguments.add(buildJsonMap(operands.get(0)));
        arguments.add(seed);
        jsonMap.put("arguments", arguments);
        return jsonMap;
    }

    private static Map<String, Object> handleIsNotNull(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "IS_NOT_NULL");
        setDataType(rexCall,jsonMap, "returnType");

        List<Map<String, Object>> notnullArgList = new ArrayList<>();
        notnullArgList.add(buildJsonMap(operands.get(0)));
        jsonMap.put("arguments", notnullArgList);
        return jsonMap;
    }

    private static Map<String, Object> handleAnd(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "MULTIPLE_AND_OR");
        jsonMap.put("operator", "AND");
        setDataType(rexCall, jsonMap, "returnType");
        List<Map<String, Object>> cond = new ArrayList<>();
        for (int i=0; i < rexCall.operands.size(); i++) {
            cond.add(buildJsonMap(operands.get(i)));
        }
        jsonMap.put("conditions", cond);
        LOG.info("List is {}", cond.toString());
        LOG.info("The expression is {} ", rexCall.toString());
        return jsonMap;
    }

    private static Map<String, Object> handleOr(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "MULTIPLE_AND_OR");
        jsonMap.put("operator", "OR");
        setDataType(rexCall, jsonMap, "returnType");
        List<Map<String, Object>> cond = new ArrayList<>();
        for (int i=0; i < rexCall.operands.size(); i++) {
            cond.add(buildJsonMap(operands.get(i)));
        }
        jsonMap.put("conditions", cond);
        LOG.info("List is {}", cond.toString());
        LOG.info("The expression is {} ", rexCall.toString());
        return jsonMap;
    }

    private static Map<String, Object> handleCast(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        Map<String, Object> childMap = buildJsonMap(operands.get(0));
        SqlTypeName currentTypeName = rexCall.getType().getSqlTypeName();
        SqlTypeName childTypeName = operands.get(0).getType().getSqlTypeName();
        boolean decimalSameScale = currentTypeName == SqlTypeName.DECIMAL
                && childTypeName == SqlTypeName.DECIMAL
                && rexCall.getType().getScale() == operands.get(0).getType().getScale();
        boolean nonDecimalSameType = currentTypeName != SqlTypeName.DECIMAL
                && childTypeName != SqlTypeName.DECIMAL
                && currentTypeName == childTypeName;
        if (nonDecimalSameType || decimalSameScale) {
            return childMap;
        }
        setDataType(rexCall,jsonMap, "returnType");
        jsonMap.put("function_name", specialType.name());
        jsonMap.put("expr", childMap);
        List<Map<String, Object>> castArgList = new ArrayList<>();
        castArgList.add(buildJsonMap(operands.get(0)));
        jsonMap.put("arguments", castArgList);
        return jsonMap;
    }

    /**
     * Shared handler for expressions that translate to a plain FUNCTION node: the function_name
     * comes from {@link #simpleFunctionNameMap} and every operand is forwarded as an argument,
     * with CHAR literals normalized to VARCHAR. Any expression whose native signature matches
     * its Flink operands one-to-one can reuse this by registering a name in
     * {@link #simpleFunctionNameMap} plus a handler entry in {@link #specialHandlerMap}.
     * Package-private so the per-category handler classes can reference it.
     */
    static Map<String, Object> handleSimpleFunction(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", simpleFunctionNameMap.get(specialType));
        List<Map<String, Object>> simpleArgList = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            Map<String, Object> argMap = buildJsonMap(operands.get(i));
            normalizeCharLiteralToVarchar(argMap);
            simpleArgList.add(argMap);
        }
        jsonMap.put("arguments", simpleArgList);
        return jsonMap;
    }
}
