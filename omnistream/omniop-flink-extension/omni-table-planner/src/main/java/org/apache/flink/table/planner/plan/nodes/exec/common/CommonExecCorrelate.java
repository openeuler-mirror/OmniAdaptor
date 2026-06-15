/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.CorrelateCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.utils.TableSqlFunction;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.util.DescriptionUtil;
import org.apache.flink.table.planner.plan.nodes.exec.util.RexNodeUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Base {@link ExecNode} which matches along with join a Java/Scala user defined table function. */
public abstract class CommonExecCorrelate extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    public static final String CORRELATE_TRANSFORMATION = "correlate";
    private static final Logger LOG = LoggerFactory.getLogger(CommonExecCorrelate.class);
    public static final String FIELD_NAME_JOIN_TYPE = "joinType";
    public static final String FIELD_NAME_FUNCTION_CALL = "functionCall";
    public static final String FIELD_NAME_CONDITION = "condition";

    @JsonProperty(FIELD_NAME_JOIN_TYPE)
    private final FlinkJoinType joinType;

    @JsonProperty(FIELD_NAME_FUNCTION_CALL)
    private final RexCall invocation;

    @JsonProperty(FIELD_NAME_CONDITION)
    private final @Nullable RexNode condition;

    private final Class<?> operatorBaseClass;
    private final boolean retainHeader;

    public CommonExecCorrelate(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            FlinkJoinType joinType,
            RexCall invocation,
            @Nullable RexNode condition,
            Class<?> operatorBaseClass,
            boolean retainHeader,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.joinType = checkNotNull(joinType);
        this.invocation = checkNotNull(invocation);
        this.condition = condition;
        this.operatorBaseClass = checkNotNull(operatorBaseClass);
        this.retainHeader = retainHeader;
    }

    /**
     * 构建 Correlate 算子的额外描述信息 JSON，供 C++ 原生算子使用。
     *
     * JSON 结构:
     * {
     *   "originDescription": "...",
     *   "joinType": "INNER" | "LEFT",
     *   "functionName": "split_func",
     *   "functionClass": "com.example.SplitFunction",
     *   "functionArgs": [ { RexNode JSON }, ... ],
     *   "functionArgIndices": [2, 3],
     *   "inputTypes": ["BIGINT", "INTEGER", "VARCHAR(2147483647)"],
     *   "outputTypes": ["BIGINT", "INTEGER", "VARCHAR(2147483647)", "VARCHAR(2147483647)"],
     *   "functionResultTypes": ["VARCHAR(2147483647)"],
     *   "condition": null | { RexNode JSON }
     * }
     */
    private String getExtraDescription(String oldDescription, RowType inputRowType) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();

        // 1. 输入类型
        List<String> inputTypeList = DescriptionUtil.getFieldTypeList(inputRowType.getFields());

        // 2. 输出类型
        List<String> outputTypeList = DescriptionUtil.getFieldTypeList(
                ((RowType) getOutputType()).getFields());

        // 3. UDTF 返回类型
        RowType functionResultType = FlinkTypeFactory.toLogicalRowType(invocation.getType());
        List<String> functionResultTypeList = DescriptionUtil.getFieldTypeList(
                functionResultType.getFields());

        // 4. 函数名和类名
        String functionName = extractFunctionName(invocation);
        String functionClass = extractFunctionClass(invocation);

        // 5. ★ 直接提取参数索引，不调用 buildJsonMap() ★
        List<Integer> functionArgIndices = new ArrayList<>();
        for (RexNode operand : invocation.getOperands()) {
            if (operand instanceof RexFieldAccess) {
                RexFieldAccess fieldAccess = (RexFieldAccess) operand;
                if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
                    functionArgIndices.add(fieldAccess.getField().getIndex());
                }
            } else if (operand instanceof RexInputRef) {
                functionArgIndices.add(((RexInputRef) operand).getIndex());
            }
        }

        // 6. 可选的过滤条件
        //    注意：condition 中也可能包含 RexFieldAccess(RexCorrelVariable)，
        //    如果有 condition，也需要同样的处理（或暂时不序列化）
        Map<String, Object> conditionMap = null;
        if (condition != null) {
            // 先构建 accessIndexMap 再调用 buildJsonMap
            // 但如果 condition 中也有 RexCorrelVariable，同样会 NPE
            // 建议初期先不支持带 condition 的 Correlate，或者单独处理
            try {
                HashMap<Integer, Integer> accessIndexMap = new HashMap<>();
                for (int i = 0; i < inputRowType.getFieldCount(); i++) {
                    accessIndexMap.put(i, i);
                }
                RexNodeUtil.accessIndexMap = accessIndexMap;
                conditionMap = RexNodeUtil.buildJsonMap(condition);
                RexNodeUtil.accessIndexMap.clear();
            } catch (Exception e) {
                LOG.warn("Failed to serialize correlate condition, skipping", e);
                conditionMap = null;
            }
        }

        // 7. 组装 JSON
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        jsonMap.put("originDescription", oldDescription);
        jsonMap.put("joinType", joinType.toString());
        jsonMap.put("functionName", functionName);
        jsonMap.put("functionClass", functionClass);
        jsonMap.put("functionArgIndices", functionArgIndices);
        jsonMap.put("inputTypes", inputTypeList);
        jsonMap.put("outputTypes", outputTypeList);
        jsonMap.put("functionResultTypes", functionResultTypeList);
        jsonMap.put("condition", conditionMap);

        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            LOG.warn("getExtraDescription error", e);
        }
        return jsonString;
    }

    /**
     * 从 RexCall 的 operator 中提取函数名。
     * 支持两种函数类型：
     * - BridgingSqlFunction（新版 Table Function）
     * - TableSqlFunction（旧版 Legacy Table Function）
     */
    private String extractFunctionName(RexCall rexCall) {
        SqlOperator operator = rexCall.getOperator();
        if (operator instanceof BridgingSqlFunction) {
            return ((BridgingSqlFunction) operator).getName();
        } else if (operator instanceof TableSqlFunction) {
            return operator.toString();
        }
        return operator.getName();
    }

    /**
     * 从 RexCall 的 operator 中提取函数实现类的全限定类名。
     * C++ 侧可通过此类名在 JNI 中实例化 TableFunction。
     */
    private String extractFunctionClass(RexCall rexCall) {
        SqlOperator operator = rexCall.getOperator();
        if (operator instanceof BridgingSqlFunction) {
            BridgingSqlFunction func = (BridgingSqlFunction) operator;
            FunctionDefinition definition = func.getDefinition();
            if (definition instanceof UserDefinedFunction) {
                return definition.getClass().getName();
            }
            // 对于 legacy TableFunctionDefinition
            if (definition instanceof TableFunctionDefinition) {
                return ((TableFunctionDefinition) definition)
                        .getTableFunction().getClass().getName();
            }
            return definition.getClass().getName();
        } else if (operator instanceof TableSqlFunction) {
            TableSqlFunction tsf = (TableSqlFunction) operator;
            return tsf.udtf().getClass().getName();
        }
        return "";
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader())
                        .setOperatorBaseClass(operatorBaseClass);
        Transformation<RowData> transformation =
                CorrelateCodeGenerator.generateCorrelateTransformation(
                        config,
                        ctx,
                        inputTransform,
                        (RowType) inputEdge.getOutputType(),
                        invocation,
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(condition)),
                        (RowType) getOutputType(),
                        joinType,
                        inputTransform.getParallelism(),
                        retainHeader,
                        getClass().getSimpleName(),
                        createTransformationMeta(CORRELATE_TRANSFORMATION, config),
                        false);
        String oldDescription = transformation.getDescription();
        transformation.setDescription(getExtraDescription(oldDescription, inputRowType));
        return transformation;
    }
}
