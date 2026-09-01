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

package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ValidateAggOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateAggOPStrategy.class);

    private static final Set<String> SUPPORT_AGG_FUNCTION_NAME = new HashSet<>(Arrays.asList(
            "MAX",
            "AVG",
            "COUNT",
            "MIN",
            "SUM",
            "SUM0",
            "last_string_value_without_retract",
            "JSON_OBJECTAGG",
            "JSON_ARRAYAGG"));

    // Value/element types supported by the native JSON_OBJECTAGG / JSON_ARRAYAGG handlers.
    private static final List<String> SUPPORT_JSON_VALUE_TYPES = Arrays.asList(
            "VARCHAR(2147483647)", "BIGINT", "INTEGER", "BOOLEAN", "DOUBLE");

    private static final Map<String, List<String>> SUPPORT_AGG_FUNCTION_DATATYPE = new HashMap<>();

    static {
        SUPPORT_AGG_FUNCTION_DATATYPE.put("MAX", Arrays.asList("BIGINT", "VARCHAR(2147483647)"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("AVG", Collections.singletonList("BIGINT"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("COUNT", Collections.singletonList("BIGINT"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("MIN", Arrays.asList("BIGINT", "VARCHAR(2147483647)"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("SUM", Collections.singletonList("BIGINT"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("SUM0", Collections.singletonList("BIGINT"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("last_string_value_without_retract", Collections.singletonList("VARCHAR(2147483647)"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("JSON_OBJECTAGG", SUPPORT_JSON_VALUE_TYPES);
        SUPPORT_AGG_FUNCTION_DATATYPE.put("JSON_ARRAYAGG", SUPPORT_JSON_VALUE_TYPES);
    }

    private static final List<String> SUPPORT_GROUP_KEY_TYPES = Arrays.asList("BIGINT", "INTEGER", "VARCHAR(2147483647)");

    /**
     * Map the ON NULL-suffixed JSON aggregate names produced by the Flink planner back to their
     * base name so the native whitelist / per-function validation can match them. Only the
     * non-retract NULL/ABSENT variants are normalized; retract variants (e.g. *_RETRACT) are left
     * untouched so they fail the whitelist and fall back to Java.
     */
    private static String normalizeAggFunctionName(String functionName) {
        if ("JSON_OBJECTAGG_NULL_ON_NULL".equals(functionName)
                || "JSON_OBJECTAGG_ABSENT_ON_NULL".equals(functionName)) {
            return "JSON_OBJECTAGG";
        }
        if ("JSON_ARRAYAGG_NULL_ON_NULL".equals(functionName)
                || "JSON_ARRAYAGG_ABSENT_ON_NULL".equals(functionName)) {
            return "JSON_ARRAYAGG";
        }
        return functionName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        Map<String, Object> aggInfoListMap = (Map<String, Object>) operatorInfoMap.get("aggInfoList");
        List<Map<String, Object>> aggregateCalls = (ArrayList<Map<String, Object>>) aggInfoListMap.get("aggregateCalls");
        if (CollectionUtil.isNullOrEmpty(aggregateCalls)) {
            return false;
        }

        List<String> inputTypeList = (ArrayList<String>) operatorInfoMap.get("inputTypes");
        boolean inputTypesEmpty = CollectionUtil.isNullOrEmpty(inputTypeList);
        for (Map<String, Object> aggregateCallMap : aggregateCalls) {
            String name = aggregateCallMap.get("name").toString();
            // aggCallName like "MAX($1)","AVG($2)","COUNT($1)"
            String functionName = name.substring(0, name.indexOf("("));
            // some name like "$SUM()" need to delete "$"
            if (functionName.contains("$")) {
                functionName = functionName.replace("$", "");
            }
            // Flink encodes the ON NULL clause into the aggregate name, e.g.
            // JSON_OBJECTAGG_NULL_ON_NULL / JSON_OBJECTAGG_ABSENT_ON_NULL. Normalize the
            // supported (non-retract) variants back to the base name for whitelist matching;
            // any other variant (incl. *_RETRACT) stays as-is and falls back to Java.
            functionName = normalizeAggFunctionName(functionName);
            if (!SUPPORT_AGG_FUNCTION_NAME.contains(functionName)) {
                LOG.info("validateVertexChainInfoForOmniTask not support aggCall is {}", name);
                return false;
            }
            List<Integer> argIndexes = (ArrayList<Integer>) aggregateCallMap.get("argIndexes");
            if (inputTypesEmpty || CollectionUtil.isNullOrEmpty(argIndexes)) {
                continue;
            }
            if ("JSON_OBJECTAGG".equals(functionName)) {
                // arg0 = key (must be VARCHAR), arg1 = value (must be a supported JSON value type).
                if (argIndexes.size() < 2) {
                    LOG.info("JSON_OBJECTAGG requires KEY and VALUE arguments: {}", name);
                    return false;
                }
                String keyType = inputTypeList.get(argIndexes.get(0));
                String valueType = inputTypeList.get(argIndexes.get(1));
                if (!"VARCHAR(2147483647)".equals(keyType)) {
                    LOG.info("JSON_OBJECTAGG key type {} is not supported (must be VARCHAR).", keyType);
                    return false;
                }
                if (!SUPPORT_JSON_VALUE_TYPES.contains(valueType)) {
                    LOG.info("JSON_OBJECTAGG value type {} is not supported.", valueType);
                    return false;
                }
            } else if ("JSON_ARRAYAGG".equals(functionName)) {
                String itemType = inputTypeList.get(argIndexes.get(0));
                if (!SUPPORT_JSON_VALUE_TYPES.contains(itemType)) {
                    LOG.info("JSON_ARRAYAGG item type {} is not supported.", itemType);
                    return false;
                }
            } else {
                int argIndex = argIndexes.get(0);
                String argType = stripNotNull(inputTypeList.get(argIndex));
                List<String> supportDataTypes = SUPPORT_AGG_FUNCTION_DATATYPE.get(functionName);
                if (!supportDataTypes.contains(argType)) {
                    LOG.info("The aggregate data type {} is not supported in aggregate function {}.",
                            argType, functionName);
                    return false;
                }
            }
            List<Integer> uniqueKeys = (ArrayList<Integer>) operatorInfoMap.get("grouping");
            if (!CollectionUtil.isNullOrEmpty(uniqueKeys) && !CollectionUtil.isNullOrEmpty(inputTypeList)) {
                for (int uniqueKey : uniqueKeys) {
                    String keyType = stripNotNull(inputTypeList.get(uniqueKey));
                    if (!SUPPORT_GROUP_KEY_TYPES.contains(keyType)) {
                        LOG.info("The group key type {} is not supported.", keyType);
                        return false;
                    }
                }
            }
        }

        // if function support,then validate dataTypes
        List<List<String>> dataTypesList = new ArrayList<>(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));
        return validateDataTypes(dataTypesList);
    }
}
