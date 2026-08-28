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
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.types.logical.LogicalType;
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
    // native 的 TIME/TIMESTAMP 以 int64 毫秒存储，因此基于它们的算术运算需要用该值
    // 按天取模回绕。24 * 60 * 60 * 1000。
    private static final long MILLIS_PER_DAY = 86400000L;
    // native 的 int64-毫秒表示最多只能容纳精度 <= 3（毫秒）。TIME/TIMESTAMP 精度为 4-9
    // （微秒/纳秒）时会丢数据，因此这里直接拒绝，交还给 Flink 处理对应 RexNode。
    private static final int MAX_NATIVE_TIME_PRECISION = 3;
    // datetime + interval 的 native 函数名。年月间隔与天时间间隔需要不同的 kernel：
    // 一个月没有固定的毫秒宽度，无法复用天时间（纯毫秒）的实现路径。
    // 这些字符串必须与 Register*.cpp 中的注册名完全一致。
    private static final String DATETIME_PLUS_YEAR_MONTH = "datetime_plus_year_month";
    private static final String DATETIME_PLUS_DAY_TIME = "datetime_plus_day_time";
    private static final String TIME_PLUS_YEAR_MONTH = "time_plus_year_month";
    public static final Map<String, Integer> RexTypeToIdMap = new HashMap<>();
    public static final Map<String, SpecialExprType> specialOperatorMap = new HashMap<>();
    public static final Map<SpecialExprType, String> simpleFunctionNameMap = new HashMap<>();
    public static final Map<String, SpecialExprType> udfOperatorMap = new HashMap<>();
    public static final Map<String, UnaryExprType> unaryOperatorMap = new HashMap<>();
    public static final Map<String, BinaryExprType> binaryOperatorMap = new HashMap<>();
    // Strategy registry: maps a SpecialExprType to the handler that builds its JSON.
    // Adding a new special expression = add a handler method + one registration line here,
    // instead of editing a shared switch-case block (reduces multi-developer merge conflicts).
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
        specialOperatorMap.put("CASE", SpecialExprType.SWITCH);
        specialOperatorMap.put("REGEXP_EXTRACT", SpecialExprType.REGEXP_EXTRACT);
        specialOperatorMap.put("SPLIT_INDEX", SpecialExprType.SPLIT_INDEX);
        specialOperatorMap.put("CHAR_LENGTH", SpecialExprType.CHAR_LENGTH);
        specialOperatorMap.put("CHARACTER_LENGTH", SpecialExprType.CHAR_LENGTH);
        specialOperatorMap.put("count_char", SpecialExprType.COUNT_CHAR);
        specialOperatorMap.put("SEARCH", SpecialExprType.SEARCH);
        specialOperatorMap.put("LOWER", SpecialExprType.LOWER);
        specialOperatorMap.put("HASH_CODE", SpecialExprType.HASH_CODE);
        specialOperatorMap.put("IS NOT NULL", SpecialExprType.IS_NOT_NULL);
        specialOperatorMap.put("PROCTIME_MATERIALIZE", SpecialExprType.PROCTIME);
        specialOperatorMap.put("PROCTIME", SpecialExprType.PROCTIME);
        specialOperatorMap.put("EXTRACT", SpecialExprType.EXTRACT);
        specialOperatorMap.put("DATE_FORMAT", SpecialExprType.DATE_FORMAT);
        specialOperatorMap.put("TO_TIMESTAMP_LTZ", SpecialExprType.TO_TIMESTAMP_LTZ);
        specialOperatorMap.put("CAST", SpecialExprType.CAST);
        specialOperatorMap.put("AND", SpecialExprType.AND);
        specialOperatorMap.put("OR", SpecialExprType.OR);
        specialOperatorMap.put("IF", SpecialExprType.IF);
        specialOperatorMap.put("COALESCE", SpecialExprType.COALESCE);
        // IFNULL reuses the COALESCE native path (equivalent to 2-arg COALESCE)
        specialOperatorMap.put("IFNULL", SpecialExprType.COALESCE);
        specialOperatorMap.put("JSON_VALUE", SpecialExprType.JSON_VALUE);
        specialOperatorMap.put("JSON_QUERY", SpecialExprType.JSON_QUERY);
        specialOperatorMap.put("JSON_SPLIT", SpecialExprType.JSON_SPLIT);
        specialOperatorMap.put("CURRENT_TIMESTAMP", SpecialExprType.CURRENT_TIMESTAMP);
        specialOperatorMap.put("CURRENT_WATERMARK", SpecialExprType.CURRENT_WATERMARK);
        specialOperatorMap.put("DATE_ADD", SpecialExprType.DATE_ADD);
        specialOperatorMap.put("ROUND", SpecialExprType.ROUND);
        specialOperatorMap.put("GREATEST", SpecialExprType.GREATEST);
        specialOperatorMap.put("LEAST", SpecialExprType.LEAST);
        specialOperatorMap.put("CONCAT", SpecialExprType.CONCAT);
        specialOperatorMap.put("CONCAT_WS", SpecialExprType.CONCAT_WS);
        specialOperatorMap.put("REPLACE", SpecialExprType.REPLACE);
        specialOperatorMap.put("SUBSTRING", SpecialExprType.SUBSTR);
        specialOperatorMap.put("SUBSTR", SpecialExprType.SUBSTR);
        specialOperatorMap.put("INSTR", SpecialExprType.INSTR);
        specialOperatorMap.put("PARSE_URL", SpecialExprType.PARSE_URL);
        specialOperatorMap.put("UNIX_TIMESTAMP", SpecialExprType.UNIX_TIMESTAMP);
        specialOperatorMap.put("FROM_UNIXTIME", SpecialExprType.FROM_UNIXTIME);
        specialOperatorMap.put("LIKE", SpecialExprType.LIKE);
        specialOperatorMap.put("TYPEOF", SpecialExprType.TYPEOF);
        specialOperatorMap.put("LEFT", SpecialExprType.LEFT);
        specialOperatorMap.put("RIGHT", SpecialExprType.RIGHT);
        // SIMILAR TO: SQL regex match, Type B vectorized path (SimilarExpr + SimilarFunction)
        specialOperatorMap.put("SIMILAR TO", SpecialExprType.SIMILAR_TO);
    }

    static {
        // Map SpecialExprType to the OmniOperatorJIT C++ registered function_name.
        // Used by the generic FUNCTION case that forwards all operands as arguments.
        simpleFunctionNameMap.put(SpecialExprType.ROUND, "round");
        simpleFunctionNameMap.put(SpecialExprType.GREATEST, "Greatest");
        simpleFunctionNameMap.put(SpecialExprType.LEAST, "Least");
        simpleFunctionNameMap.put(SpecialExprType.CONCAT, "concat");
        simpleFunctionNameMap.put(SpecialExprType.CONCAT_WS, "concat_ws");
        simpleFunctionNameMap.put(SpecialExprType.REPLACE, "replace");
        simpleFunctionNameMap.put(SpecialExprType.SUBSTR, "substr");
        simpleFunctionNameMap.put(SpecialExprType.INSTR, "instr");
        simpleFunctionNameMap.put(SpecialExprType.PARSE_URL, "parse_url");
        simpleFunctionNameMap.put(SpecialExprType.UNIX_TIMESTAMP, "unix_timestamp");
        simpleFunctionNameMap.put(SpecialExprType.FROM_UNIXTIME, "from_unixtime");
        simpleFunctionNameMap.put(SpecialExprType.LEFT, "left");
        simpleFunctionNameMap.put(SpecialExprType.RIGHT, "right");
    }

    static {
        // Map UDF registration names to their corresponding SpecialExprType
        udfOperatorMap.put("jsontest", SpecialExprType.JSON_SPLIT);
        udfOperatorMap.put("DATE_ADD", SpecialExprType.DATE_ADD);
    }

    static {
        unaryOperatorMap.put("-", UnaryExprType.NEGATION);
        unaryOperatorMap.put("IS TRUE", UnaryExprType.IS_TRUE);
        unaryOperatorMap.put("IS NOT TRUE", UnaryExprType.IS_NOT_TRUE);
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
        // Register one handler per SpecialExprType. Each handler is a self-contained
        // private static method below. New expressions add a method + a line here.
        specialHandlerMap.put(SpecialExprType.SWITCH, RexNodeUtil::handleSwitch);
        specialHandlerMap.put(SpecialExprType.IF, RexNodeUtil::handleSwitch);
        specialHandlerMap.put(SpecialExprType.REGEXP_EXTRACT, RexNodeUtil::handleRegexpExtract);
        specialHandlerMap.put(SpecialExprType.COALESCE, RexNodeUtil::handleCoalesce);
        specialHandlerMap.put(SpecialExprType.JSON_VALUE, RexNodeUtil::handleJsonValue);
        specialHandlerMap.put(SpecialExprType.JSON_QUERY, RexNodeUtil::handleJsonQuery);
        specialHandlerMap.put(SpecialExprType.JSON_SPLIT, RexNodeUtil::handleJsonSplit);
        specialHandlerMap.put(SpecialExprType.SPLIT_INDEX, RexNodeUtil::handleSplitIndex);
        specialHandlerMap.put(SpecialExprType.COUNT_CHAR, RexNodeUtil::handleCountChar);
        specialHandlerMap.put(SpecialExprType.SEARCH, RexNodeUtil::handleSearch);
        specialHandlerMap.put(SpecialExprType.HASH_CODE, RexNodeUtil::handleHashCode);
        specialHandlerMap.put(SpecialExprType.EXTRACT, RexNodeUtil::handleExtract);
        specialHandlerMap.put(SpecialExprType.LOWER, RexNodeUtil::handleLower);
        specialHandlerMap.put(SpecialExprType.CHAR_LENGTH, RexNodeUtil::handleCharLength);
        specialHandlerMap.put(SpecialExprType.IS_NOT_NULL, RexNodeUtil::handleIsNotNull);
        specialHandlerMap.put(SpecialExprType.TO_TIMESTAMP_LTZ, RexNodeUtil::handleToTimestampLtz);
        specialHandlerMap.put(SpecialExprType.PROCTIME, RexNodeUtil::handleProctime);
        specialHandlerMap.put(SpecialExprType.DATE_FORMAT, RexNodeUtil::handleDateFormat);
        specialHandlerMap.put(SpecialExprType.CAST, RexNodeUtil::handleCast);
        specialHandlerMap.put(SpecialExprType.AND, RexNodeUtil::handleAnd);
        specialHandlerMap.put(SpecialExprType.OR, RexNodeUtil::handleOr);
        specialHandlerMap.put(SpecialExprType.CURRENT_TIMESTAMP, RexNodeUtil::handleCurrentTimestamp);
        specialHandlerMap.put(SpecialExprType.CURRENT_WATERMARK, RexNodeUtil::handleCurrentWatermark);
        specialHandlerMap.put(SpecialExprType.DATE_ADD, RexNodeUtil::handleDateAdd);
        specialHandlerMap.put(SpecialExprType.FROM_UNIXTIME, RexNodeUtil::handleFromUnixtime);
        // Simple FUNCTION-forwarding expressions share one handler (function_name via simpleFunctionNameMap).
        specialHandlerMap.put(SpecialExprType.ROUND, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.GREATEST, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.LEAST, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.CONCAT, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.CONCAT_WS, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.REPLACE, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.SUBSTR, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.INSTR, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.PARSE_URL, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.UNIX_TIMESTAMP, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.LIKE, RexNodeUtil::handleLike);
        specialHandlerMap.put(SpecialExprType.TYPEOF, RexNodeUtil::handleTypeOf);
        specialHandlerMap.put(SpecialExprType.LEFT, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.RIGHT, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.SIMILAR_TO, RexNodeUtil::handleSimilarTo);
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
     * Implemented by the private static {@code handleXxx} methods and wired up in
     * {@link #specialHandlerMap}. This replaces the former monolithic switch-case so that
     * adding a new special expression does not require editing a shared block.
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
        IS_NOT_TRUE,
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
        CHAR_LENGTH,
        IS_NOT_NULL,
        PROCTIME,
        EXTRACT,
        DATE_FORMAT,
        TO_TIMESTAMP_LTZ,
        COUNT_CHAR,
        CAST,
        OTHERS,
        AND,
        OR,
        IF,
        COALESCE,
        JSON_VALUE,
        JSON_QUERY,
        JSON_SPLIT,
        CURRENT_TIMESTAMP,
        CURRENT_WATERMARK,
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
        TYPEOF,
        LEFT,
        RIGHT,
        SIMILAR_TO
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
            jsonMap.put(keyStr, RexTypeToIdMap.get("DATE"));
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
     */
    private static void normalizeCharLiteralToVarchar(Map<String, Object> jsonMap) {
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
            int numOperands = operands.size();
            SqlOperator operator = rexCall.getOperator();
            String operatorName = operator.getName();
            BinaryExprType binaryType = resolveOperatorType(binaryOperatorMap, operatorName);
            UnaryExprType unaryType = resolveOperatorType(unaryOperatorMap, operatorName);
            SpecialExprType udfType = resolveOperatorType(udfOperatorMap, operatorName);
            SpecialExprType specialType = udfType != null ? udfType : resolveOperatorType(specialOperatorMap, operatorName);
            LOG.info("Current rexNode is {}", rexCall.toString());
            // IS NOT TRUE 在 Calcite 里是 postfix 单操作数调用，没有 native 对应物。
            // 翻成三值等价的 CASE WHEN x THEN FALSE ELSE TRUE END，
            // 使 OVERLAPS 展开出的 IS NOT TRUE 比较可用 native SWITCH_GENERAL 执行。
            if (unaryType == UnaryExprType.IS_NOT_TRUE) {
                return handleIsNotNullTrue(rexCall, operands, jsonMap);
            }
            // datetime + interval 在 Calcite 里被折叠成右操作数为 INTERVAL 类型的 PLUS 调用。
            // 它必须在通用 BINARY 分支之前处理：native 无法把 interval 当作普通 BIGINT 加数
            // （年月间隔没有固定毫秒宽度，且 TIME 需要按天回绕），因此下推为专用 FUNCTION。
            if (isDatetimePlus(rexCall)) {
                return handleDatetimePlus(rexCall, operands, jsonMap);
            } else if (rexCall.operands.size() == 2 && binaryType != null) {
                jsonMap.put("exprType",  OperatorExprType.BINARY.name());
                setDataType(rexCall,jsonMap, "returnType");
                jsonMap.put("operator", binaryType.name());
                // 当一侧是 DATE、另一侧是 TIMESTAMP 时，native 的 DATE（epoch 天）必须提升
                // 为 TIMESTAMP（epoch 毫秒）表示，使二元运算在相同单位上比较/相加。
                // 由目标操作数决定是否提升。
                Map<String, Object> leftMap = buildJsonMapWithTemporalPromotion(
                        operands.get(0), operands.get(1));
                jsonMap.put("left", leftMap);
                Map<String, Object> rightMap = buildJsonMapWithTemporalPromotion(
                        operands.get(1), operands.get(0));
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

    /**
     * 判定 {@code datetime + interval} 表达式。Calcite 把 SQL 的 interval 加法折叠成
     * 右操作数带 INTERVAL SqlTypeName 的 PLUS 调用。这类调用不能走通用 BINARY 路径
     * （对 native 而言 interval 不是普通数值加数），因此在此识别并路由到
     * {@link #handleDatetimePlus}。
     */
    private static boolean isDatetimePlus(RexCall rexCall) {
        return rexCall.getKind() == SqlKind.PLUS
                && rexCall.getOperands().size() == 2
                && SqlTypeName.INTERVAL_TYPES.contains(
                        rexCall.getOperands().get(1).getType().getSqlTypeName());
    }

    /**
     * 年月间隔（YEAR、YEAR TO MONTH、MONTH）没有固定的毫秒宽度——一个月视日期不同
     * 在 28~31 天之间，因此需要与天时间间隔（DAY/HOUR/MINUTE/SECOND，纯毫秒）不同的
     * native kernel。
     */
    private static boolean isYearMonthInterval(SqlTypeName typeName) {
        return SqlTypeName.YEAR_INTERVAL_TYPES.contains(typeName);
    }

    /**
     * native 的 TIME/TIMESTAMP 是 int64 毫秒，即精度最多为 3。任何精度超过
     * {@link #MAX_NATIVE_TIME_PRECISION} 的 RexNode（微秒/纳秒）在 native 侧会丢数据，
     * 因此直接拒绝并回退到 Flink。非时间类型在此一律放行（其精度与本判定无关）。
     */
    private static boolean hasSupportedNativePrecision(RexNode rexNode) {
        SqlTypeName typeName = rexNode.getType().getSqlTypeName();
        if (typeName != SqlTypeName.TIME && typeName != SqlTypeName.TIMESTAMP) {
            return true;
        }
        int precision = rexNode.getType().getPrecision();
        return precision >= 0 && precision <= MAX_NATIVE_TIME_PRECISION;
    }

    /** 构造 INVALID 标记 JSON；规划器据此判定该 RexNode "不被 native 支持"，
     *  回退到 vanilla Flink 算子执行。 */
    private static Map<String, Object> invalidExpression() {
        Map<String, Object> invalidMap = new LinkedHashMap<>();
        invalidMap.put("exprType", OperatorExprType.INVALID.name());
        return invalidMap;
    }

    /**
     * 将 {@code datetime + interval} RexCall 下推为 native JSON。
     *
     * <p>产出两种形态：
     * <ul>
     *   <li>DATE / TIMESTAMP 操作数 → FUNCTION 节点，调用 {@code datetime_plus_year_month}
     *       或 {@code datetime_plus_day_time}，参数为 (datetime, interval 字面量)。</li>
     *   <li>TIME + year-month → 专用 identity kernel，以区分物理签名相同但日历语义不同的
     *       TIMESTAMP；TIME + day-time → 合成算术树并按天回绕。</li>
     * </ul>
     *
     * <p>当前 interval 仅限字面量：OmniStream 尚未支持 interval 列的编解码，
     * 以错误宽度序列化 interval 列会导致值损坏。
     */
    private static Map<String, Object> handleDatetimePlus(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap) {
        RexNode datetime = operands.get(0);
        RexNode interval = operands.get(1);
        SqlTypeName datetimeType = datetime.getType().getSqlTypeName();
        SqlTypeName returnType = rexCall.getType().getSqlTypeName();
        if (isOutsideDatetimePlusScope(interval, datetime, datetimeType, returnType)) {
            LOG.info("Datetime plus expression is outside the native literal/precision/type scope: {}", rexCall);
            return invalidExpression();
        }

        // DATE 加上不足一天的 interval，其 Rex 返回类型可能是 TIMESTAMP(6)。
        // 但输入只含 epoch 天和整毫秒，因此生成的值是无损的。
        // TIMESTAMP/TIME 输入的精度上限仍维持在 3。
        boolean yearMonth = isYearMonthInterval(interval.getType().getSqlTypeName());
        Map<String, Object> intervalMap = buildIntervalLiteral((RexLiteral) interval, yearMonth);
        // TIME 与 TIMESTAMP 都映射为 BIGINT。年月间隔必须使用独立 identity kernel，
        // 天时间间隔则按 Flink 公式下推为回绕算术树。
        if (datetimeType == SqlTypeName.TIME) {
            return yearMonth
                    ? buildTimePlusYearMonth(datetime, intervalMap)
                    : buildTimePlusDayTime(datetime, intervalMap);
        }

        // DATE / TIMESTAMP：产出 FUNCTION 节点。native kernel 根据 function_name 选择年月
        // 或天时间变体，interval 以带类型字面量的形式传入。
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", yearMonth ? DATETIME_PLUS_YEAR_MONTH : DATETIME_PLUS_DAY_TIME);
        List<Map<String, Object>> arguments = new ArrayList<>();
        arguments.add(buildJsonMap(datetime));
        arguments.add(intervalMap);
        jsonMap.put("arguments", arguments);
        return jsonMap;
    }

    /**
     * 判断 datetime+interval 节点是否超出 native 首版支持范围（字面量/精度/类型）。
     * 每个条件对应一个真实的 native 限制：
     *   - interval 必须是 RexLiteral（暂不支持 interval 列编解码）
     *   - TIMESTAMP_WITH_LOCAL_TIME_ZONE 在 native 无对应表示
     *   - datetime 必须是 DATE/TIME/TIMESTAMP（其它类型与 interval 相加无意义）
     *   - datetime 精度必须 <= 3（native int64-毫秒）
     */
    private static boolean isOutsideDatetimePlusScope(
            RexNode interval, RexNode datetime, SqlTypeName datetimeType, SqlTypeName returnType) {
        return !(interval instanceof RexLiteral)
                || !SqlTypeName.INTERVAL_TYPES.contains(interval.getType().getSqlTypeName())
                || datetimeType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                || returnType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                || (datetimeType != SqlTypeName.DATE
                    && datetimeType != SqlTypeName.TIME
                    && datetimeType != SqlTypeName.TIMESTAMP)
                || !hasSupportedNativePrecision(datetime);
    }

    /**
     * 为 native kernel 序列化 interval 字面量。标量宽度与 interval 类别对应：
     * 年月间隔是总月数（可用 INT，且年月 native kernel 期望 INT）；
     * 天时间间隔是总毫秒数（需 BIGINT，对应天时间 kernel）。
     * {@code getValueAs} 返回的是已聚合好的总值（例如 "2 天 3 小时" → 毫秒数）。
     */
    private static Map<String, Object> buildIntervalLiteral(RexLiteral interval, boolean yearMonth) {
        Map<String, Object> literalMap = new LinkedHashMap<>();
        literalMap.put("exprType", OperatorExprType.LITERAL.name());
        literalMap.put("dataType", yearMonth ? RexTypeToIdMap.get("INT") : RexTypeToIdMap.get("BIGINT"));
        literalMap.put("isNull", interval.isNull());
        if (!interval.isNull()) {
            literalMap.put(
                    "value",
                    yearMonth
                            ? interval.getValueAs(Integer.class)
                            : interval.getValueAs(Long.class));
        }
        return literalMap;
    }

    /** 将 {@code TIME + 年月间隔} 下推为保留 interval NULL 传播的 identity kernel。 */
    private static Map<String, Object> buildTimePlusYearMonth(
            RexNode time, Map<String, Object> intervalMap) {
        Map<String, Object> functionMap = new LinkedHashMap<>();
        functionMap.put("exprType", "FUNCTION");
        functionMap.put("returnType", RexTypeToIdMap.get("BIGINT"));
        functionMap.put("function_name", TIME_PLUS_YEAR_MONTH);
        List<Map<String, Object>> arguments = new ArrayList<>();
        arguments.add(buildJsonMap(time));
        arguments.add(intervalMap);
        functionMap.put("arguments", arguments);
        return functionMap;
    }

    /**
     * 将 {@code TIME + 天时间间隔} 下推为 native 算术。
     *
     * <p>native TIME 是 int64 的"一天内的毫秒数"，取值范围 {@code [0, MILLIS_PER_DAY)}。
     * Flink 对正负 interval 都把结果按天回绕，采用正数取模公式：
     * {@code ((time + MILLIS_PER_DAY + interval % MILLIS_PER_DAY) % MILLIS_PER_DAY)}。
     * {@code + MILLIS_PER_DAY} 一项把可能为负的 {@code time + interval} 抬回非负区间，
     * 使 native 的 MODULUS（遵循 Java/C 向零截断语义，负操作数会得到负余数）仍落在范围内。
     * 先对 interval 按天取模可保持中间值幅度较小。标准的二元 NULL 传播仍然成立：
     * 任一操作数为 NULL 则整棵树为 NULL。
     */
    private static Map<String, Object> buildTimePlusDayTime(
            RexNode time, Map<String, Object> intervalMap) {
        Map<String, Object> day = buildLongLiteral(MILLIS_PER_DAY);
        // interval % MILLIS_PER_DAY —— 真正影响时间部分的"一天内的余数"。
        Map<String, Object> intervalInDay = buildBinaryJson(
                RexTypeToIdMap.get("BIGINT"),
                BinaryExprType.MODULUS,
                intervalMap,
                buildLongLiteral(MILLIS_PER_DAY));
        // time + MILLIS_PER_DAY —— 抬到零以上，保证最终取模结果非负。
        Map<String, Object> positiveTime = buildBinaryJson(
                RexTypeToIdMap.get("BIGINT"),
                BinaryExprType.ADD,
                buildJsonMap(time),
                day);
        // (time + MILLIS_PER_DAY) + intervalInDay —— 未回绕的（可能超过 1 天）总和。
        Map<String, Object> unwrappedResult = buildBinaryJson(
                RexTypeToIdMap.get("BIGINT"),
                BinaryExprType.ADD,
                positiveTime,
                intervalInDay);
        // ... % MILLIS_PER_DAY —— 回绕回 [0, MILLIS_PER_DAY)。
        return buildBinaryJson(
                RexTypeToIdMap.get("BIGINT"),
                BinaryExprType.MODULUS,
                unwrappedResult,
                buildLongLiteral(MILLIS_PER_DAY));
    }

    /** 合成的 BIGINT 字面量，用于下推后的 TIME 算术及 {@link #buildDateToTimestamp}
     *  中的天长常量与零偏移。 */
    private static Map<String, Object> buildLongLiteral(long value) {
        Map<String, Object> literalMap = new LinkedHashMap<>();
        literalMap.put("exprType", OperatorExprType.LITERAL.name());
        literalMap.put("dataType", RexTypeToIdMap.get("BIGINT"));
        literalMap.put("isNull", false);
        literalMap.put("value", value);
        return literalMap;
    }

    /** 组装一个 BINARY 表达式节点 (left op right) 并显式指定返回类型。供下推后的
     *  TIME 算术使用——该算术全程在 BIGINT 上运算，与操作数原类型无关。 */
    private static Map<String, Object> buildBinaryJson(int returnType, BinaryExprType operator,
            Map<String, Object> left, Map<String, Object> right) {
        Map<String, Object> binaryMap = new LinkedHashMap<>();
        binaryMap.put("exprType", OperatorExprType.BINARY.name());
        binaryMap.put("returnType", returnType);
        binaryMap.put("operator", operator.name());
        binaryMap.put("left", left);
        binaryMap.put("right", right);
        return binaryMap;
    }

    /**
     * 判定 {@code value}（DATE）是否需要提升为 TIMESTAMP 以匹配 {@code target} 的类型。
     * native 中 DATE 是 epoch 天、TIMESTAMP 是 epoch 毫秒，若在二元运算或 CASE 分支中
     * 不做提升就混用，会在不兼容的单位上比较/相加，静默产生错误结果。
     */
    private static boolean needsDateToTimestampPromotion(RexNode value, RexNode target) {
        return value.getType().getSqlTypeName() == SqlTypeName.DATE
                && target.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP;
    }

    /**
     * {@link #buildJsonMap} 的包装：当运算上下文（{@code target} RexNode，例如外层二元调用
     * 或 CASE）期望 TIMESTAMP 时，把 DATE 操作数提升为 TIMESTAMP。无需提升的操作数原样透传。
     */
    private static Map<String, Object> buildJsonMapWithTemporalPromotion(
            RexNode value, RexNode target) {
        if (!needsDateToTimestampPromotion(value, target)) {
            return buildJsonMap(value);
        }
        return buildDateToTimestamp(value);
    }

    /**
     * 将 DATE → TIMESTAMP 的提升下推为 {@code datetime_plus_day_time(date, 0)}。
     *
     * <p>DATE 是 epoch 天，而 TIMESTAMP 路径是 epoch 毫秒。加一个零天时间间隔可复用
     * 现有的 DATE32 + 天时间 kernel，把天数换算成毫秒（date_days * MILLIS_PER_DAY），
     * 无需新增 native cast。返回类型为 BIGINT（epoch 毫秒），与 native 的 TIMESTAMP 表示一致。
     */
    private static Map<String, Object> buildDateToTimestamp(RexNode date) {
        Map<String, Object> functionMap = new LinkedHashMap<>();
        functionMap.put("exprType", "FUNCTION");
        functionMap.put("returnType", RexTypeToIdMap.get("BIGINT"));
        functionMap.put("function_name", DATETIME_PLUS_DAY_TIME);
        List<Map<String, Object>> arguments = new ArrayList<>();
        arguments.add(buildJsonMap(date));
        arguments.add(buildLongLiteral(0L));
        functionMap.put("arguments", arguments);
        return functionMap;
    }

    // =========================================================================
    // Special-expression handlers (one per SpecialExprType, wired in specialHandlerMap).
    // Each handler receives the RexCall, its operands, a pre-created jsonMap and the resolved
    // SpecialExprType, and returns the fully-populated JSON map for that expression.
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
            RexNode result = operands.get(i * 2 + 1);
            // CASE 表达式会把整体类型提升为所有分支的共同父类型，因此一个 TIMESTAMP 值的
            // CASE 可能存在 DATE 分支。这样的分支必须提升为 TIMESTAMP（epoch 毫秒）以匹配
            // 其它分支的表示，否则 native 会在各分支间混用 epoch 天与 epoch 毫秒。
            caseMap.put("result", buildJsonMapWithTemporalPromotion(result, rexCall));
            caseMap.put("exprType", "WHEN");
            // 被提升的 DATE 分支必须声明 CASE 的 TIMESTAMP 返回类型，而不是自身的 DATE 类型，
            // 使每个分支向 native 声明同一类型。未提升的分支保留自身自然类型。
            if (needsDateToTimestampPromotion(result, rexCall)) {
                setDataType(rexCall, caseMap, "returnType");
            } else {
                setDataType(result, caseMap, "returnType");
            }
            jsonMap.put("Case"+Integer.toString(i + 1), caseMap);
        }
        // else 分支按与上面 WHEN-result 分支相同的规则做提升。
        jsonMap.put(
                "else",
                buildJsonMapWithTemporalPromotion(
                        operands.get(operands.size() - 1), rexCall));
        return jsonMap;
    }

    private static Map<String, Object> handleIsNotNullTrue(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap) {
        // CASE WHEN x THEN FALSE ELSE TRUE END == IS NOT TRUE(x) in SQL three-valued logic
        jsonMap.put("exprType", "SWITCH_GENERAL");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("numOfCases", 1);
        Map<String, Object> caseMap = new LinkedHashMap<>();
        caseMap.put("when", buildJsonMap(operands.get(0)));
        caseMap.put("result", buildBooleanLiteral(false));
        caseMap.put("exprType", "WHEN");
        setDataType(rexCall, caseMap, "returnType");
        jsonMap.put("Case1", caseMap);
        jsonMap.put("else", buildBooleanLiteral(true));
        return jsonMap;
    }

    private static Map<String, Object> buildBooleanLiteral(boolean value) {
        Map<String, Object> literal = new LinkedHashMap<>();
        literal.put("exprType", "LITERAL");
        literal.put("dataType", RexTypeToIdMap.get("BOOLEAN"));
        literal.put("isNull", false);
        literal.put("value", value);
        return literal;
    }

    private static Map<String, Object> handleRegexpExtract(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall,jsonMap, "returnType");
        jsonMap.put("function_name", "regex_extract_null");

        List<Map<String, Object>> regArgs = new ArrayList<>();
        regArgs.add(buildJsonMap(operands.get(0)));
        regArgs.add(buildJsonMap(operands.get(1)));
        regArgs.add(buildJsonMap(operands.get(2)));
        jsonMap.put("arguments", regArgs);
        LOG.info("The expression is {} ", rexCall.toString());
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

    private static Map<String, Object> handleJsonValue(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall,jsonMap, "returnType");
        jsonMap.put("function_name", "json_value");

        List<Map<String, Object>> jsonArgs = new ArrayList<>();
        jsonArgs.add(buildJsonMap(operands.get(0))); // json input
        jsonArgs.add(buildJsonMap(operands.get(1))); // path expression

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

    private static Map<String, Object> handleJsonQuery(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        if (operands.size() != 2 && operands.size() != 3 && operands.size() != 5) {
            LOG.warn("JSON_QUERY expects 2, 3, or 5 operands, but got {}", operands.size());
            jsonMap.put("exprType", "INVALID");
            return jsonMap;
        }

        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "json_query");

        List<Map<String, Object>> queryArgs = new ArrayList<>();
        queryArgs.add(buildJsonMap(operands.get(0)));
        queryArgs.add(buildJsonMap(operands.get(1)));

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

    private static Map<String, Object> handleJsonSplit(RexCall rexCall, List<RexNode> operands,
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
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "json_split");

        List<Map<String, Object>> splitArgs = new ArrayList<>();
        splitArgs.add(buildJsonMap(operands.get(0))); // json array input

        jsonMap.put("arguments", splitArgs);

        LOG.info("The JSON_SPLIT expression is {} ", rexCall.toString());
        return jsonMap;
    }

    private static Map<String, Object> handleSplitIndex(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        // todo:check VARCHAR(length) mapping to returnTypeID
        setDataType(rexCall,jsonMap, "returnType");
        jsonMap.put("function_name", "SplitIndex");

        List<Map<String, Object>> stringList = new ArrayList<>();
        stringList.add(buildJsonMap(operands.get(0)));
        stringList.add(buildJsonMap(operands.get(1)));
        stringList.add(buildJsonMap(operands.get(2)));

        LOG.info("List is {}", stringList.toString());
        jsonMap.put("arguments", stringList);
        LOG.info("The expresssion is {} ", rexCall.toString());
        return jsonMap;
    }

    private static Map<String, Object> handleCountChar(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "CountChar");

        List<Map<String, Object>> stringList = new ArrayList<>();
        stringList.add(buildJsonMap(operands.get(0)));
        stringList.add(buildJsonMap(operands.get(1)));

        LOG.info("List is {}", stringList.toString());
        jsonMap.put("arguments", stringList);
        LOG.info("The expresssion is {} ", rexCall.toString());
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
        } else if (sarg.isComplementedPoints()) {
            // NOT IN: complement point-set Sarg -> recover points -> UNARY(NOT, IN(points)).
            // Guava Range shaded (Flink relocates) -> lambda param type inferred.
            jsonMap.put("exprType", "UNARY");
            setDataType(rexCall, jsonMap, "returnType");
            jsonMap.put("operator", "NOT");
            Map<String, Object> inMap = new LinkedHashMap<>();
            inMap.put("exprType", "IN");
            setDataType(rexCall, inMap, "returnType");
            List<Map<String, Object>> inArgs = new ArrayList<>();
            inArgs.add(buildJsonMap(operands.get(0))); // value to test
            sarg.rangeSet.complement().asRanges().forEach(r -> {
                Map<String, Object> literalMap = new LinkedHashMap<>();
                literalMap.put("exprType", "LITERAL");
                literalMap.put("isNull", false);
                literalMap.put("value", extractSargEndpoint(r.lowerEndpoint()));
                setDataType(operands.get(0), literalMap, "dataType");
                inArgs.add(literalMap);
            });
            inMap.put("arguments", inArgs);
            jsonMap.put("expr", inMap);
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

    private static Map<String, Object> handleExtract(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // Current hardcoded solution for extracting hour
        if (operands.get(0).toString().equals("FLAG(HOUR)")) { // && operands.get(1).toString().equals("CAST($2):TIMESTAMP(3)")
            jsonMap.put("exprType", "FUNCTION");
            // Returns int for 0-24
            jsonMap.put("returnType", 1);
            List<Object> args = new LinkedList<>();
            args.add(buildJsonMap(operands.get(1)));
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

    private static Map<String, Object> handleLower(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall,jsonMap, "returnType");
        jsonMap.put("function_name", "lower");
        List<Map<String, Object>> lowerArgList = new ArrayList<>();
        lowerArgList.add(buildJsonMap(operands.get(0)));
        jsonMap.put("arguments", lowerArgList);
        return jsonMap;
    }

    private static Map<String, Object> handleCharLength(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "char_length");
        List<Map<String, Object>> charLengthArgList = new ArrayList<>();
        charLengthArgList.add(buildJsonMap(operands.get(0)));
        jsonMap.put("arguments", charLengthArgList);
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

    private static Map<String, Object> handleToTimestampLtz(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "to_timestamp_ltz");
        List<Map<String, Object>> toTimestampLtzArgList = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            toTimestampLtzArgList.add(buildJsonMap(operands.get(i)));
        }
        jsonMap.put("arguments", toTimestampLtzArgList);
        return jsonMap;
    }

    private static Map<String, Object> handleProctime(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        SqlOperator operator = rexCall.getOperator();
        jsonMap.put("exprType", SpecialExprType.PROCTIME);
        jsonMap.put("returnType", RexTypeToIdMap.get(rexCall.getType().getSqlTypeName().toString()));
        LOG.info("The operator is* {} ", operator.getName());
        LOG.info("The type is* {} ", rexCall.getType().getSqlTypeName().toString());
        return jsonMap;
    }

    private static Map<String, Object> handleDateFormat(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        Integer returnDataType = RexTypeToIdMap.get(rexCall.getType().getSqlTypeName().toString());
        jsonMap.put("returnType", returnDataType);
        jsonMap.put("width", operands.get(1).getType().getPrecision());
        List<Map<String, Object>> argumentsList = new ArrayList<>();
        Map<String, Object> argMap1 = buildJsonMap(operands.get(0));
        setDataType(operands.get(0), argMap1, "dataType");
        if (!argMap1.getOrDefault("dataType", 2).equals(2)) {
            argMap1.put("value", "INVALID");
        }
        argMap1.put("dataType", 2);
        argumentsList.add(argMap1);
        Map<String, Object> argMap2 = buildJsonMap(operands.get(1));
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

    private static Map<String, Object> handleCast(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // CAST(date AS TIMESTAMP) 是 DATE→TIMESTAMP 提升（epoch 天 → epoch 毫秒）。
        // 路由到专用提升 kernel，而非通用 cast 路径——通用路径没有 DATE 到 TIMESTAMP 的
        // 单位换算，会把天数当作毫秒处理。
        if (needsDateToTimestampPromotion(operands.get(0), rexCall)) {
            return buildDateToTimestamp(operands.get(0));
        }
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

    private static Map<String, Object> handleCurrentTimestamp(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "current_timestamp");
        List<Map<String, Object>> currentTimestampArgs = new ArrayList<>();
        if (operands.size() > 0) {
            for (int i = 0; i < operands.size(); i++) {
                currentTimestampArgs.add(buildJsonMap(operands.get(i)));
            }
        }
        jsonMap.put("arguments", currentTimestampArgs);
        LOG.info("The CURRENT_TIMESTAMP expression is {} ", rexCall.toString());
        return jsonMap;
    }

    private static Map<String, Object> handleCurrentWatermark(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        if (operands.size() != 1) {
            LOG.warn("CURRENT_WATERMARK expects exactly one rowtime operand, but got {}", operands.size());
            jsonMap.put("exprType", OperatorExprType.INVALID.name());
            return jsonMap;
        }

        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "current_watermark");
        // Flink validates the rowtime operand and derives the logical return type. At runtime,
        // CURRENT_WATERMARK reads operator context and therefore has no data arguments.
        jsonMap.put("arguments", new ArrayList<>());
        return jsonMap;
    }

    private static Map<String, Object> handleDateAdd(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        jsonMap.put("returnType", 2);
        jsonMap.put("function_name", "date_add_days");
        List<Map<String, Object>> dateAddArgs = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            Map<String, Object> argMap = buildJsonMap(operands.get(i));
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

    private static Map<String, Object> handleFromUnixtime(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "from_unixtime");
        List<Map<String, Object>> fromUnixTimeArgs = new ArrayList<>();
        Map<String, Object> fromUnixTimeInputArg = buildJsonMap(operands.get(0));
        setDataType(operands.get(0), fromUnixTimeInputArg, "dataType");
        normalizeCharLiteralToVarchar(fromUnixTimeInputArg);
        fromUnixTimeArgs.add(fromUnixTimeInputArg);
        Map<String, Object> fromUnixTimeFormatArg;
        if (operands.size() >= 2) {
            fromUnixTimeFormatArg = buildJsonMap(operands.get(1));
            normalizeCharLiteralToVarchar(fromUnixTimeFormatArg);
        } else {
            // Flink default: FROM_UNIXTIME(numeric) -> 'yyyy-MM-dd HH:mm:ss'
            fromUnixTimeFormatArg = new LinkedHashMap<>();
            fromUnixTimeFormatArg.put("exprType", "LITERAL");
            fromUnixTimeFormatArg.put("dataType", RexTypeToIdMap.get("VARCHAR"));
            fromUnixTimeFormatArg.put("isNull", false);
            fromUnixTimeFormatArg.put("value", "yyyy-MM-dd HH:mm:ss");
            fromUnixTimeFormatArg.put("width", 19);
        }
        fromUnixTimeArgs.add(fromUnixTimeFormatArg);
        if (operands.get(0).getType().getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            Map<String, Object> fromUnixTimeTzArg = new LinkedHashMap<>();
            fromUnixTimeTzArg.put("dataType", RexTypeToIdMap.get("VARCHAR"));
            fromUnixTimeTzArg.put("exprType", "LITERAL");
            fromUnixTimeTzArg.put("isNull", false);
            fromUnixTimeTzArg.put("value", CommonExecCalc.getZoneId().getId());
            fromUnixTimeTzArg.put("width", CommonExecCalc.getZoneId().getId().length());
            fromUnixTimeArgs.add(fromUnixTimeTzArg);
        }
        jsonMap.put("arguments", fromUnixTimeArgs);
        return jsonMap;
    }

    private static Map<String, Object> handleLike(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // LIKE: 2-arg native via LikeFunction (vectorized); 3-arg ESCAPE -> INVALID fallback.
        // NOT LIKE arrives as NOT(LIKE(..)) via sql2rel convertlet, reuses UNARY/NOT branch.
        if (operands.size() != 2) {
            jsonMap.put("exprType", "INVALID");
            return jsonMap;
        }
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "LIKE");
        List<Map<String, Object>> likeArgList = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            Map<String, Object> argMap = buildJsonMap(operands.get(i));
            normalizeCharLiteralToVarchar(argMap);
            likeArgList.add(argMap);
        }
        jsonMap.put("arguments", likeArgList);
        return jsonMap;
    }

    private static Map<String, Object> handleTypeOf(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        if (operands.size() < 1 || operands.size() > 2) {
            LOG.warn("TYPEOF expects one input and an optional BOOLEAN literal");
            jsonMap.put("exprType", OperatorExprType.INVALID.name());
            return jsonMap;
        }

        boolean forceSerializable = false;
        if (operands.size() == 2) {
            RexNode forceOperand = operands.get(1);
            if (!(forceOperand instanceof RexLiteral)
                    || forceOperand.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
                LOG.warn("TYPEOF force_serializable must be a BOOLEAN literal");
                jsonMap.put("exprType", OperatorExprType.INVALID.name());
                return jsonMap;
            }
            Boolean forceValue = ((RexLiteral) forceOperand).getValueAs(Boolean.class);
            forceSerializable = Boolean.TRUE.equals(forceValue);
        }

        LogicalType inputType = FlinkTypeFactory.toLogicalType(operands.get(0).getType());
        String typeString;
        if (forceSerializable) {
            try {
                typeString = inputType.asSerializableString();
            } catch (Exception exception) {
                typeString = null;
            }
        } else {
            typeString = inputType.asSummaryString();
        }

        jsonMap.put("exprType", OperatorExprType.LITERAL.name());
        setDataType(rexCall, jsonMap, "dataType");
        jsonMap.put("isNull", typeString == null);
        if (typeString != null) {
            jsonMap.put("value", typeString);
        }
        return jsonMap;
    }

    private static Map<String, Object> handleSimilarTo(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // SIMILAR TO: 2-arg native via SimilarExpr (vectorized); 3-arg with ESCAPE not supported -> fallback.
        if (operands.size() != 2) {
            jsonMap.put("exprType", "INVALID");
            return jsonMap;
        }
        jsonMap.put("exprType", "SIMILAR_TO");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("value", buildJsonMap(operands.get(0)));
        jsonMap.put("pattern", buildJsonMap(operands.get(1)));
        return jsonMap;
    }

    /**
     * Shared handler for simple FUNCTION-forwarding expressions (ROUND, GREATEST, LEAST,
     * CONCAT, CONCAT_WS, REPLACE, SUBSTR, INSTR, PARSE_URL, UNIX_TIMESTAMP): the function_name
     * comes from {@link #simpleFunctionNameMap} and every operand is forwarded as an argument.
     */
    private static Map<String, Object> handleSimpleFunction(RexCall rexCall, List<RexNode> operands,
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
                        Map<String, Object> defaultValue = buildJsonMap(operands.get(startIndex + 1));
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
        String wrapperName = getSymbolLiteralName(wrapperNode);
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
        String behaviorName = getSymbolLiteralName(behaviorNode);
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

    private static String getSymbolLiteralName(RexNode symbolNode) {
        if (symbolNode instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) symbolNode;
            Object literalValue = literal.getValue();
            if (literalValue != null) {
                return normalizeSymbolLiteralName(literalValue.toString());
            }

            Object value2 = literal.getValue2();
            if (value2 != null) {
                return normalizeSymbolLiteralName(value2.toString());
            }
        }

        return normalizeSymbolLiteralName(symbolNode.toString());
    }

    private static String normalizeSymbolLiteralName(String symbolText) {
        if (symbolText == null || symbolText.isEmpty()) {
            return null;
        }

        if (symbolText.startsWith("FLAG(") && symbolText.endsWith(")")) {
            symbolText = symbolText.substring(5, symbolText.length() - 1);
        }

        int bracketStart = symbolText.indexOf('[');
        int bracketEnd = symbolText.lastIndexOf(']');
        if (bracketStart >= 0 && bracketEnd > bracketStart) {
            return symbolText.substring(bracketStart + 1, bracketEnd);
        }
        return symbolText;
    }
}
