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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RexNodeUtilTypeOfTest {
    private static final SqlOperator TYPEOF =
            new SqlSpecialOperator("TYPEOF", SqlKind.OTHER_FUNCTION);

    private FlinkTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelDataType stringType;

    @Before
    public void setUp() {
        typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
        rexBuilder = new RexBuilder(typeFactory);
        stringType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }

    @Test
    public void testTypeOfIsRegisteredAsSpecialExpression() {
        assertEquals(
                RexNodeUtil.SpecialExprType.TYPEOF,
                RexNodeUtil.specialOperatorMap.get("TYPEOF"));
    }

    @Test
    public void testSummaryStringForInputType() {
        LogicalType inputType = new IntType(false);

        Map<String, Object> jsonMap = buildTypeOf(inputType);

        assertLiteral(jsonMap, inputType.asSummaryString());
    }

    @Test
    public void testFalseAndNullFlagsUseSummaryString() {
        LogicalType inputType = new VarCharType(32);

        assertLiteral(buildTypeOf(inputType, rexBuilder.makeLiteral(false)), "VARCHAR(32)");
        assertLiteral(
                buildTypeOf(
                        inputType,
                        rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.BOOLEAN))),
                "VARCHAR(32)");
    }

    @Test
    public void testTrueFlagUsesSerializableString() {
        LogicalType inputType = new CharType(false, 11);

        Map<String, Object> jsonMap = buildTypeOf(inputType, rexBuilder.makeLiteral(true));

        assertLiteral(jsonMap, inputType.asSerializableString());
    }

    @Test
    public void testNullInputReturnsNullTypeString() {
        LogicalType inputType = new NullType();
        RelDataType relDataType = typeFactory.createFieldTypeFromLogicalType(inputType);
        RexNode nullInput = rexBuilder.makeNullLiteral(relDataType);

        Map<String, Object> jsonMap = buildTypeOf(nullInput);

        assertLiteral(jsonMap, "NULL");
    }

    @Test
    public void testComplexTypesUseLogicalTypeSummary() {
        LogicalType arrayType = new ArrayType(new IntType(false));
        LogicalType rowType =
                RowType.of(
                        new LogicalType[] {new IntType(), new VarCharType(12)},
                        new String[] {"id", "name"});

        assertLiteral(buildTypeOf(arrayType), arrayType.asSummaryString());
        assertLiteral(buildTypeOf(rowType), rowType.asSummaryString());
    }

    @Test
    public void testAnonymousStructuredTypeHasNoSerializableString() {
        StructuredType inputType =
                StructuredType.newBuilder(AnonymousType.class)
                        .attributes(
                                Collections.singletonList(
                                        new StructuredType.StructuredAttribute(
                                                "value", new IntType())))
                        .build();

        Map<String, Object> jsonMap = buildTypeOf(inputType, rexBuilder.makeLiteral(true));

        assertEquals("LITERAL", jsonMap.get("exprType"));
        assertEquals(true, jsonMap.get("isNull"));
        assertFalse(jsonMap.containsKey("value"));
    }

    @Test
    public void testNonLiteralForceFlagIsRejected() {
        LogicalType inputType = new IntType();
        RelDataType booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        RexNode nonLiteralFlag = new RexInputRef(1, booleanType);

        Map<String, Object> jsonMap = buildTypeOf(inputType, nonLiteralFlag);

        assertEquals("INVALID", jsonMap.get("exprType"));
    }

    @Test
    public void testInvalidArityIsRejected() {
        RexCall noArgumentCall =
                (RexCall)
                        rexBuilder.makeCall(
                                stringType, TYPEOF, Collections.<RexNode>emptyList());
        RexNode input = inputRef(new IntType());
        RexCall tooManyArgumentsCall =
                (RexCall)
                        rexBuilder.makeCall(
                                stringType,
                                TYPEOF,
                                Arrays.asList(
                                        input,
                                        rexBuilder.makeLiteral(false),
                                        rexBuilder.makeLiteral(true)));

        assertEquals("INVALID", RexNodeUtil.buildJsonMap(noArgumentCall).get("exprType"));
        assertEquals("INVALID", RexNodeUtil.buildJsonMap(tooManyArgumentsCall).get("exprType"));
    }

    private Map<String, Object> buildTypeOf(LogicalType inputType, RexNode... flags) {
        return buildTypeOf(inputRef(inputType), flags);
    }

    private Map<String, Object> buildTypeOf(RexNode input, RexNode... flags) {
        RexNode[] operands = new RexNode[flags.length + 1];
        operands[0] = input;
        System.arraycopy(flags, 0, operands, 1, flags.length);
        RexCall call =
                (RexCall)
                        rexBuilder.makeCall(
                                stringType, TYPEOF, Arrays.asList(operands));
        return RexNodeUtil.buildJsonMap(call);
    }

    private RexNode inputRef(LogicalType inputType) {
        return new RexInputRef(0, typeFactory.createFieldTypeFromLogicalType(inputType));
    }

    private static void assertLiteral(Map<String, Object> jsonMap, String expectedValue) {
        assertEquals("LITERAL", jsonMap.get("exprType"));
        assertEquals(false, jsonMap.get("isNull"));
        assertEquals(expectedValue, jsonMap.get("value"));
        assertTrue(jsonMap.containsKey("dataType"));
        assertNull(jsonMap.get("operator"));
    }

    public static class AnonymousType {
        public Integer value;
    }
}
