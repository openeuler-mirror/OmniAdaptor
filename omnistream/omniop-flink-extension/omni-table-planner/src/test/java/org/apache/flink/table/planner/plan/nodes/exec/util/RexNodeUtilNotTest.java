package org.apache.flink.table.planner.plan.nodes.exec.util;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.Map;

import static org.junit.Assert.*;
import org.junit.Test;

public class RexNodeUtilNotTest {

    @Test
    public void testNotOperatorInUnaryMap() {
        assertTrue("NOT should be registered in unaryOperatorMap",
                RexNodeUtil.unaryOperatorMap.containsKey("NOT"));
        assertEquals("NOT should map to UnaryExprType.NOT",
                RexNodeUtil.UnaryExprType.NOT,
                RexNodeUtil.unaryOperatorMap.get("NOT"));
    }

    @Test
    public void testNotBooleanExpression() {
        RelDataTypeFactory typeFactory = new org.apache.calcite.sql.type.SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelDataType booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

        RexInputRef inputRef = rexBuilder.makeInputRef(booleanType, 0);
        SqlOperator notOperator = SqlStdOperatorTable.NOT;
        RexCall notCall = (RexCall) rexBuilder.makeCall(notOperator, inputRef);

        Map<String, Object> jsonMap = RexNodeUtil.buildJsonMap(notCall);

        assertNotNull("JSON map should not be null", jsonMap);
        assertEquals("exprType should be UNARY", "UNARY", jsonMap.get("exprType"));
        assertEquals("operator should be NOT", "NOT", jsonMap.get("operator"));
        assertEquals("returnType should be BOOLEAN (4)", 4, jsonMap.get("returnType"));
        assertNotNull("expr should not be null", jsonMap.get("expr"));
        assertTrue("expr should be a map", jsonMap.get("expr") instanceof Map);

        @SuppressWarnings("unchecked")
        Map<String, Object> exprMap = (Map<String, Object>) jsonMap.get("expr");
        assertEquals("expr's exprType should be FIELD_REFERENCE",
                "FIELD_REFERENCE", exprMap.get("exprType"));
        assertEquals("expr's dataType should be BOOLEAN (4)", 4, exprMap.get("dataType"));
    }

    @Test
    public void testNotWithTrueLiteral() {
        RelDataTypeFactory typeFactory = new org.apache.calcite.sql.type.SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);

        RexLiteral trueLiteral = rexBuilder.makeLiteral(true);
        SqlOperator notOperator = SqlStdOperatorTable.NOT;
        RexCall notCall = (RexCall) rexBuilder.makeCall(notOperator, trueLiteral);

        Map<String, Object> jsonMap = RexNodeUtil.buildJsonMap(notCall);

        assertNotNull("JSON map should not be null", jsonMap);
        assertEquals("exprType should be UNARY", "UNARY", jsonMap.get("exprType"));
        assertEquals("operator should be NOT", "NOT", jsonMap.get("operator"));

        @SuppressWarnings("unchecked")
        Map<String, Object> exprMap = (Map<String, Object>) jsonMap.get("expr");
        assertEquals("expr's exprType should be LITERAL", "LITERAL", exprMap.get("exprType"));
        assertEquals("expr's dataType should be BOOLEAN (4)", 4, exprMap.get("dataType"));
        assertEquals("expr's value should be true", true, exprMap.get("value"));
        assertEquals("expr's isNull should be false", false, exprMap.get("isNull"));
    }

    @Test
    public void testNotWithFalseLiteral() {
        RelDataTypeFactory typeFactory = new org.apache.calcite.sql.type.SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);

        RexLiteral falseLiteral = rexBuilder.makeLiteral(false);
        SqlOperator notOperator = SqlStdOperatorTable.NOT;
        RexCall notCall = (RexCall) rexBuilder.makeCall(notOperator, falseLiteral);

        Map<String, Object> jsonMap = RexNodeUtil.buildJsonMap(notCall);

        assertNotNull("JSON map should not be null", jsonMap);
        assertEquals("exprType should be UNARY", "UNARY", jsonMap.get("exprType"));
        assertEquals("operator should be NOT", "NOT", jsonMap.get("operator"));

        @SuppressWarnings("unchecked")
        Map<String, Object> exprMap = (Map<String, Object>) jsonMap.get("expr");
        assertEquals("expr's exprType should be LITERAL", "LITERAL", exprMap.get("exprType"));
        assertEquals("expr's value should be false", false, exprMap.get("value"));
    }

    @Test
    public void testNotWithNullLiteral() {
        RelDataTypeFactory typeFactory = new org.apache.calcite.sql.type.SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelDataType booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

        RexLiteral nullLiteral = rexBuilder.makeNullLiteral(booleanType);
        SqlOperator notOperator = SqlStdOperatorTable.NOT;
        RexCall notCall = (RexCall) rexBuilder.makeCall(notOperator, nullLiteral);

        Map<String, Object> jsonMap = RexNodeUtil.buildJsonMap(notCall);

        assertNotNull("JSON map should not be null", jsonMap);
        assertEquals("exprType should be UNARY", "UNARY", jsonMap.get("exprType"));
        assertEquals("operator should be NOT", "NOT", jsonMap.get("operator"));

        @SuppressWarnings("unchecked")
        Map<String, Object> exprMap = (Map<String, Object>) jsonMap.get("expr");
        assertEquals("expr's exprType should be LITERAL", "LITERAL", exprMap.get("exprType"));
        assertEquals("expr's isNull should be true", true, exprMap.get("isNull"));
    }
}
