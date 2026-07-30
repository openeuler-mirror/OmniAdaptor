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
import org.apache.calcite.util.Sarg;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
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
        specialOperatorMap.put("FROM_BASE64", SpecialExprType.FROM_BASE64);
        specialOperatorMap.put("CHAR_LENGTH", SpecialExprType.CHAR_LENGTH);
        specialOperatorMap.put("CHARACTER_LENGTH", SpecialExprType.CHAR_LENGTH);
        specialOperatorMap.put("count_char", SpecialExprType.COUNT_CHAR);
        specialOperatorMap.put("SEARCH", SpecialExprType.SEARCH);
        specialOperatorMap.put("LOWER", SpecialExprType.LOWER);
        specialOperatorMap.put("HASH_CODE", SpecialExprType.HASH_CODE);
        specialOperatorMap.put("IS NOT NULL", SpecialExprType.IS_NOT_NULL);
        specialOperatorMap.put("IS JSON VALUE", SpecialExprType.IS_JSON_VALUE);
        specialOperatorMap.put("IS JSON SCALAR", SpecialExprType.IS_JSON_SCALAR);
        specialOperatorMap.put("IS JSON ARRAY", SpecialExprType.IS_JSON_ARRAY);
        specialOperatorMap.put("IS JSON OBJECT", SpecialExprType.IS_JSON_OBJECT);
        specialOperatorMap.put("PROCTIME_MATERIALIZE", SpecialExprType.PROCTIME);
        specialOperatorMap.put("PROCTIME", SpecialExprType.PROCTIME);
        specialOperatorMap.put("EXTRACT", SpecialExprType.EXTRACT);
        specialOperatorMap.put("DATE_FORMAT", SpecialExprType.DATE_FORMAT);
        specialOperatorMap.put("TO_TIMESTAMP_LTZ", SpecialExprType.TO_TIMESTAMP_LTZ);
        specialOperatorMap.put("TO_DATE", SpecialExprType.TO_DATE);
        specialOperatorMap.put("CAST", SpecialExprType.CAST);
        specialOperatorMap.put("AND", SpecialExprType.AND);
        specialOperatorMap.put("OR", SpecialExprType.OR);
        specialOperatorMap.put("IF", SpecialExprType.IF);
        specialOperatorMap.put("COALESCE", SpecialExprType.COALESCE);
        // IFNULL reuses the COALESCE native path (equivalent to 2-arg COALESCE)
        specialOperatorMap.put("IFNULL", SpecialExprType.COALESCE);
        specialOperatorMap.put("JSON_VALUE", SpecialExprType.JSON_VALUE);
        specialOperatorMap.put("JSON_QUERY", SpecialExprType.JSON_QUERY);
        specialOperatorMap.put("JSON_EXISTS", SpecialExprType.JSON_EXISTS);
        specialOperatorMap.put("JSON_SPLIT", SpecialExprType.JSON_SPLIT);
        specialOperatorMap.put("CURRENT_TIMESTAMP", SpecialExprType.CURRENT_TIMESTAMP);
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
        specialOperatorMap.put("UNIX_TIMESTAMP", SpecialExprType.UNIX_TIMESTAMP);
        specialOperatorMap.put("FROM_UNIXTIME", SpecialExprType.FROM_UNIXTIME);
        specialOperatorMap.put("RPAD", SpecialExprType.RPAD);
        specialOperatorMap.put("LPAD", SpecialExprType.LPAD);
        specialOperatorMap.put("REPEAT", SpecialExprType.REPEAT);
        specialOperatorMap.put("OVERLAY", SpecialExprType.OVERLAY);
        specialOperatorMap.put("SINH", SpecialExprType.SINH);
        specialOperatorMap.put("COS", SpecialExprType.COS);
        specialOperatorMap.put("COT", SpecialExprType.COT);
        specialOperatorMap.put("ASIN", SpecialExprType.ASIN);
        specialOperatorMap.put("ACOS", SpecialExprType.ACOS);
        specialOperatorMap.put("ATAN", SpecialExprType.ATAN);
        specialOperatorMap.put("ATAN2", SpecialExprType.ATAN2);
        specialOperatorMap.put("COSH", SpecialExprType.COSH);
        specialOperatorMap.put("DEGREES", SpecialExprType.DEGREES);
        specialOperatorMap.put("SIGN", SpecialExprType.SIGN);
        specialOperatorMap.put("SIN", SpecialExprType.SIN);
        specialOperatorMap.put("TAN", SpecialExprType.TAN);
        specialOperatorMap.put("TANH", SpecialExprType.TANH);
        specialOperatorMap.put("RADIANS", SpecialExprType.RADIANS);
        specialOperatorMap.put("PI", SpecialExprType.PI);
        specialOperatorMap.put("E", SpecialExprType.E);
        specialOperatorMap.put("RAND", SpecialExprType.RAND);
        specialOperatorMap.put("RAND_INTEGER", SpecialExprType.RAND_INTEGER);
        specialOperatorMap.put("UUID", SpecialExprType.UUID);
        specialOperatorMap.put("BIN", SpecialExprType.BIN);
        specialOperatorMap.put("HEX", SpecialExprType.HEX);
        specialOperatorMap.put("TRUNCATE", SpecialExprType.TRUNCATE);
        specialOperatorMap.put("IS_ALPHA", SpecialExprType.IS_ALPHA);
        specialOperatorMap.put("IS_DECIMAL", SpecialExprType.IS_DECIMAL);
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
        simpleFunctionNameMap.put(SpecialExprType.UNIX_TIMESTAMP, "unix_timestamp");
        simpleFunctionNameMap.put(SpecialExprType.RPAD, "rpad");
        simpleFunctionNameMap.put(SpecialExprType.LPAD, "lpad");
        simpleFunctionNameMap.put(SpecialExprType.REPEAT, "repeat");
        simpleFunctionNameMap.put(SpecialExprType.FROM_BASE64, "unbase64");
        simpleFunctionNameMap.put(SpecialExprType.SINH, "sinh");
        simpleFunctionNameMap.put(SpecialExprType.COS, "cos");
        simpleFunctionNameMap.put(SpecialExprType.COT, "cot");
        simpleFunctionNameMap.put(SpecialExprType.ASIN, "asin");
        simpleFunctionNameMap.put(SpecialExprType.ACOS, "acos");
        simpleFunctionNameMap.put(SpecialExprType.ATAN, "atan");
        simpleFunctionNameMap.put(SpecialExprType.ATAN2, "atan2");
        simpleFunctionNameMap.put(SpecialExprType.COSH, "cosh");
        simpleFunctionNameMap.put(SpecialExprType.DEGREES, "degrees");
        simpleFunctionNameMap.put(SpecialExprType.SIGN, "sign");
        simpleFunctionNameMap.put(SpecialExprType.SIN, "sin");
        simpleFunctionNameMap.put(SpecialExprType.TAN, "tan");
        simpleFunctionNameMap.put(SpecialExprType.TANH, "tanh");
        simpleFunctionNameMap.put(SpecialExprType.RADIANS, "radians");
        simpleFunctionNameMap.put(SpecialExprType.PI, "pi");
        simpleFunctionNameMap.put(SpecialExprType.E, "e");
        simpleFunctionNameMap.put(SpecialExprType.RAND, "rand");
        simpleFunctionNameMap.put(SpecialExprType.RAND_INTEGER, "rand_integer");
        simpleFunctionNameMap.put(SpecialExprType.TRUNCATE, "truncate");
        simpleFunctionNameMap.put(SpecialExprType.IS_ALPHA, "is_alpha");
        simpleFunctionNameMap.put(SpecialExprType.IS_DECIMAL, "is_decimal");
        simpleFunctionNameMap.put(SpecialExprType.IS_JSON_VALUE, "is_json_value");
        simpleFunctionNameMap.put(SpecialExprType.IS_JSON_SCALAR, "is_json_scalar");
        simpleFunctionNameMap.put(SpecialExprType.IS_JSON_ARRAY, "is_json_array");
        simpleFunctionNameMap.put(SpecialExprType.IS_JSON_OBJECT, "is_json_object");
    }

    static {
        // Map UDF registration names to their corresponding SpecialExprType
        udfOperatorMap.put("jsontest", SpecialExprType.JSON_SPLIT);
        udfOperatorMap.put("DATE_ADD", SpecialExprType.DATE_ADD);
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
        // Register one handler per SpecialExprType. Each handler is a self-contained
        // private static method below. New expressions add a method + a line here.
        specialHandlerMap.put(SpecialExprType.SWITCH, RexNodeUtil::handleSwitch);
        specialHandlerMap.put(SpecialExprType.IF, RexNodeUtil::handleSwitch);
        specialHandlerMap.put(SpecialExprType.REGEXP_EXTRACT, RexNodeUtil::handleRegexpExtract);
        specialHandlerMap.put(SpecialExprType.COALESCE, RexNodeUtil::handleCoalesce);
        specialHandlerMap.put(SpecialExprType.JSON_VALUE, RexNodeUtil::handleJsonValue);
        specialHandlerMap.put(SpecialExprType.JSON_QUERY, RexNodeUtil::handleJsonQuery);
        specialHandlerMap.put(SpecialExprType.JSON_EXISTS, RexNodeUtil::handleJsonExists);
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
        specialHandlerMap.put(SpecialExprType.TO_DATE, RexNodeUtil::handleToDate);
        specialHandlerMap.put(SpecialExprType.PROCTIME, RexNodeUtil::handleProctime);
        specialHandlerMap.put(SpecialExprType.DATE_FORMAT, RexNodeUtil::handleDateFormat);
        specialHandlerMap.put(SpecialExprType.CAST, RexNodeUtil::handleCast);
        specialHandlerMap.put(SpecialExprType.AND, RexNodeUtil::handleAnd);
        specialHandlerMap.put(SpecialExprType.OR, RexNodeUtil::handleOr);
        specialHandlerMap.put(SpecialExprType.CURRENT_TIMESTAMP, RexNodeUtil::handleCurrentTimestamp);
        specialHandlerMap.put(SpecialExprType.UUID, RexNodeUtil::handleUuid);
        specialHandlerMap.put(SpecialExprType.BIN, RexNodeUtil::handleBin);
        specialHandlerMap.put(SpecialExprType.HEX, RexNodeUtil::handleHex);
        specialHandlerMap.put(SpecialExprType.DATE_ADD, RexNodeUtil::handleDateAdd);
        specialHandlerMap.put(SpecialExprType.FROM_UNIXTIME, RexNodeUtil::handleFromUnixtime);
        specialHandlerMap.put(SpecialExprType.OVERLAY, RexNodeUtil::handleOverlay);
        // Simple FUNCTION-forwarding expressions share one handler (function_name via simpleFunctionNameMap).
        specialHandlerMap.put(SpecialExprType.ROUND, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.GREATEST, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.LEAST, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.CONCAT, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.CONCAT_WS, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.REPLACE, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.SUBSTR, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.INSTR, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.UNIX_TIMESTAMP, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.RPAD, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.LPAD, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.REPEAT, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.FROM_BASE64, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.SINH, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.COS, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.COT, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.ASIN, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.ACOS, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.ATAN, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.ATAN2, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.COSH, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.DEGREES, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.SIGN, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.SIN, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.TAN, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.TANH, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.RADIANS, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.PI, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.E, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.RAND, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.RAND_INTEGER, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.TRUNCATE, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.IS_ALPHA, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.IS_DECIMAL, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.IS_JSON_VALUE, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.IS_JSON_SCALAR, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.IS_JSON_ARRAY, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.IS_JSON_OBJECT, RexNodeUtil::handleSimpleFunction);
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
        UNIX_TIMESTAMP,
        FROM_UNIXTIME,
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
        IS_JSON_OBJECT
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
            caseMap.put("result", buildJsonMap(operands.get(i * 2 + 1)));
            caseMap.put("exprType", "WHEN");
            setDataType(operands.get(i * 2 + 1), caseMap, "returnType");
            jsonMap.put("Case"+Integer.toString(i + 1), caseMap);
        }
        // The else
        jsonMap.put("else", buildJsonMap(operands.get(operands.size() - 1)));
        return jsonMap;
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

    private static Map<String, Object> handleJsonExists(RexCall rexCall, List<RexNode> operands,
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
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "json_exists");

        List<Map<String, Object>> jsonExistsArgs = new ArrayList<>();
        Map<String, Object> jsonInputArg = buildJsonMap(operands.get(0));
        normalizeCharLiteralToVarchar(jsonInputArg);
        jsonExistsArgs.add(jsonInputArg); // json value
        Map<String, Object> pathArg = buildJsonMap(operands.get(1));
        normalizeCharLiteralToVarchar(pathArg);
        jsonExistsArgs.add(pathArg); // path expression

        // Optional ON ERROR behavior (operand 2): SYMBOL literal -> synthesized VARCHAR literal.
        if (operands.size() == 3) {
            String onErrorName = getSymbolLiteralName(operands.get(2));
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
            onErrorArg.put("dataType", RexTypeToIdMap.get("VARCHAR"));
            onErrorArg.put("isNull", false);
            onErrorArg.put("value", onErrorName);
            onErrorArg.put("width", onErrorName.length());
            jsonExistsArgs.add(onErrorArg);
        }

        jsonMap.put("arguments", jsonExistsArgs);
        LOG.info("The JSON_EXISTS expression is {} ", rexCall.toString());
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
                if (values.get(i) instanceof NlsString) {
                    String endpoint = ((NlsString) values.get(i)).getValue();
                    literalMap.put("value", endpoint);
                    setDataType(operands.get(0), literalMap, "dataType");
                } else {
                    literalMap.put("value", values.get(i));
                    setDataType(operands.get(0), literalMap, "dataType");
                }
                stringList.add(literalMap);
            }
            jsonMap.put("arguments", stringList);
        } else { // a range
            jsonMap.put("exprType", "BETWEEN");
            setDataType(rexCall,jsonMap, "returnType");
            jsonMap.put("value", buildJsonMap(operands.get(0)));
            // TODO: inclusive, exclusive problem not solved!

            // Extract lower and upper bounds
            Object lowerBound = sarg.rangeSet.asRanges().iterator().next().lowerEndpoint();
            Object upperBound = sarg.rangeSet.asRanges().iterator().next().upperEndpoint();

            Map<String, Object> lowMap = new LinkedHashMap<>();
            lowMap.put("exprType", "LITERAL");
            lowMap.put("isNull", false);
            lowMap.put("value", lowerBound);
            setDataType(operands.get(0), lowMap, "dataType");
            jsonMap.put("lower_bound", lowMap);

            Map<String, Object> upMap = new LinkedHashMap<>();
            upMap.put("exprType", "LITERAL");
            upMap.put("isNull", false);
            upMap.put("value", upperBound);
            setDataType(operands.get(0), upMap, "dataType");
            jsonMap.put("upper_bound", upMap);
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

    private static Map<String, Object> handleToDate(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // TO_DATE(string1[, string2]) -> DATE, default format 'yyyy-MM-dd'.
        // Maps to the vectorized to_date({VARCHAR,VARCHAR}) -> DATE32 function.
        jsonMap.put("exprType", "FUNCTION");
        // to_date is registered to return OMNI_DATE32(8); setDataType would collapse
        // the DATE return type to LONG(2), so set it explicitly.
        jsonMap.put("returnType", RexTypeToIdMap.get("DATE"));
        jsonMap.put("function_name", "to_date");
        List<Map<String, Object>> toDateArgs = new ArrayList<>();
        Map<String, Object> toDateInputArg = buildJsonMap(operands.get(0));
        normalizeCharLiteralToVarchar(toDateInputArg);
        toDateArgs.add(toDateInputArg);
        Map<String, Object> toDateFormatArg;
        if (operands.size() >= 2) {
            toDateFormatArg = buildJsonMap(operands.get(1));
            normalizeCharLiteralToVarchar(toDateFormatArg);
        } else {
            // Flink default: TO_DATE(string) -> 'yyyy-MM-dd'
            toDateFormatArg = new LinkedHashMap<>();
            toDateFormatArg.put("exprType", "LITERAL");
            toDateFormatArg.put("dataType", RexTypeToIdMap.get("VARCHAR"));
            toDateFormatArg.put("isNull", false);
            toDateFormatArg.put("value", "yyyy-MM-dd");
            toDateFormatArg.put("width", 10);
        }
        toDateArgs.add(toDateFormatArg);
        jsonMap.put("arguments", toDateArgs);
        LOG.info("The TO_DATE expression is {} ", rexCall.toString());
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

    private static Map<String, Object> handleUuid(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // UUID() -> VARCHAR (RFC4122 v4). 0-arg non-deterministic; maps to native "uuid" (0-arg overload).
        // Flink types UUID() as CHAR(36), so setDataType would emit OMNI_CHAR and miss the native
        // {}->OMNI_VARCHAR signature; the return type is pinned to VARCHAR instead.
        jsonMap.put("exprType", "FUNCTION");
        jsonMap.put("returnType", RexTypeToIdMap.get("VARCHAR"));
        jsonMap.put("function_name", "uuid");
        jsonMap.put("arguments", new ArrayList<>());
        return jsonMap;
    }

    private static Map<String, Object> handleBin(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // BIN(integer) -> VARCHAR (binary string). Maps to native "bin" ({INT}/{LONG}).
        // Flink derives the return type as VARCHAR with default precision (Integer.MAX_VALUE), so
        // the return type is set directly to avoid emitting that width.
        jsonMap.put("exprType", "FUNCTION");
        jsonMap.put("returnType", RexTypeToIdMap.get("VARCHAR"));
        jsonMap.put("function_name", "bin");
        List<Map<String, Object>> binArgs = new ArrayList<>();
        Map<String, Object> binInputArg = buildJsonMap(operands.get(0));
        binArgs.add(binInputArg);
        jsonMap.put("arguments", binArgs);
        return jsonMap;
    }

    private static Map<String, Object> handleHex(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // HEX(numeric|string) -> VARCHAR (hex string). Maps to native "hex".
        // Native numeric overload is registered on {OMNI_LONG} only (HexBigintFunction
        // takes int64_t), so an INT-family input that is not BIGINT must be CAST to
        // BIGINT to match the native signature. String inputs ({VARCHAR}/{CHAR}) map
        // directly; normalizeCharLiteralToVarchar coerces CHAR literals to OMNI_VARCHAR.
        jsonMap.put("exprType", "FUNCTION");
        jsonMap.put("returnType", RexTypeToIdMap.get("VARCHAR"));
        jsonMap.put("function_name", "hex");
        List<Map<String, Object>> hexArgs = new ArrayList<>();
        Map<String, Object> hexInputArg = buildJsonMap(operands.get(0));
        normalizeCharLiteralToVarchar(hexInputArg);
        SqlTypeName hexInputTypeName = operands.get(0).getType().getSqlTypeName();
        if (hexInputTypeName == SqlTypeName.INTEGER
                || hexInputTypeName == SqlTypeName.TINYINT
                || hexInputTypeName == SqlTypeName.SMALLINT) {
            // Wrap the arg in CAST(... AS BIGINT) so native hex({LONG}) resolves.
            Map<String, Object> hexCastArg = new LinkedHashMap<>();
            hexCastArg.put("exprType", "FUNCTION");
            hexCastArg.put("returnType", RexTypeToIdMap.get("BIGINT"));
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

    private static Map<String, Object> handleOverlay(RexCall rexCall, List<RexNode> operands,
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
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "overlay");
        List<Map<String, Object>> overlayArgs = new ArrayList<>();
        Map<String, Object> overlayInputArg = buildJsonMap(operands.get(0));
        normalizeCharLiteralToVarchar(overlayInputArg);
        overlayArgs.add(overlayInputArg);
        Map<String, Object> overlayReplaceArg = buildJsonMap(operands.get(1));
        normalizeCharLiteralToVarchar(overlayReplaceArg);
        overlayArgs.add(overlayReplaceArg);
        Map<String, Object> overlayPosArg = buildJsonMap(operands.get(2));
        overlayArgs.add(overlayPosArg);
        Map<String, Object> overlayLenArg;
        if (operands.size() >= 4) {
            overlayLenArg = buildJsonMap(operands.get(3));
        } else {
            // Flink default: omitted FOR length == CHAR_LENGTH(string2).
            // Native treats len < 0 as "use replace length", so -1 expresses it.
            overlayLenArg = new LinkedHashMap<>();
            overlayLenArg.put("exprType", "LITERAL");
            overlayLenArg.put("dataType", RexTypeToIdMap.get("INTEGER"));
            overlayLenArg.put("isNull", false);
            overlayLenArg.put("value", -1);
        }
        overlayArgs.add(overlayLenArg);
        jsonMap.put("arguments", overlayArgs);
        LOG.info("The OVERLAY expression is {} ", rexCall.toString());
        return jsonMap;
    }

    /**
     * Shared handler for expressions that translate to a plain FUNCTION node: the function_name
     * comes from {@link #simpleFunctionNameMap} and every operand is forwarded as an argument,
     * with CHAR literals normalized to VARCHAR. Any expression whose native signature matches
     * its Flink operands one-to-one can reuse this by registering a name in
     * {@link #simpleFunctionNameMap} plus a handler entry in {@link #specialHandlerMap}.
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