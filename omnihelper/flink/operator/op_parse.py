"""
   Flink 算子解析模块

   Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
   You can use this software according to the terms and conditions of the Mulan PSL v2.
   You may obtain a copy of Mulan PSL v2 at:
              http://license.coscl.org.cn/MulanPSL2
   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
   See the Mulan PSL v2 for more details.

模块功能说明:
    本模块是 Flink 作业分析的核心解析引擎，负责以下功能:
    1. 解析 Flink 物理计划描述，提取算子和函数信息
    2. 构建和维护数据流 schema 链，追踪字段类型变化
    3. 解析 JSON 格式的算子元数据
    4. 计算算子性能指标 (吞吐量、延迟、数据量等)
    5. 生成结构化的分析报告数据

核心概念:
    - Schema Chain: 算子链中每个节点的输入/输出 schema 追踪
    - 拓扑排序: 按照数据流方向处理算子，确保上游信息先于下游可用
    - 类型推断: 根据表达式和上下文推断字段/表达式的返回类型

架构设计:
    本模块采用分层架构:
    1. 数据层: 从 Flink REST API 获取原始数据
    2. 解析层: 解析算子描述、函数调用、类型信息
    3. 分析层: 聚合指标、判断支持性、构建 schema 链
    4. 输出层: 生成 Excel 格式的分析报告

依赖模块:
    - FlinkFunctionParser: 函数解析
    - FlinkTypeResolver: 类型解析
    - TableSchemaReader: 表结构读取
    - TypeNormalizer: 类型标准化
"""

import json
import re
import os
import html
from collections import defaultdict
from json import JSONDecodeError
from omnihelper.flink.function.function_parse import FlinkFunctionParser
from omnihelper.flink.schema.type_resolver import FlinkTypeResolver
from omnihelper.flink.schema.table_schema_reader import TableSchemaReader
from omnihelper.flink.schema.type_normalizer import TypeNormalizer
from omnihelper.util.common_util import CommonUtil
from omnihelper.util.log import logger
from omnihelper.constants.flink_constants import ExcelColumns, TaskStatus


class FlinkParser:
    """
    Flink 物理计划解析器 - 核心类

    核心职责:
    1. 加载和维护算子字典 (flink_op_dictionary.json)
    2. 解析作业计划描述，提取算子信息和函数调用
    3. 构建 schema 链追踪数据流中的类型变化
    4. 聚合和分析算子性能指标
    5. 生成符合 Excel 导出格式的数据行

    类常量说明:
    - EXCEL_COLUMNS: Excel 输出列定义，按输出顺序排列
    - SOURCE_OP_TYPES: 数据源算子类型集合，这些算子产生数据而非转换数据
    - PASS_THROUGH_OP_TYPES: 透传类型算子，不改变 schema 结构

    成员变量详解:
    - function_parser: FlinkFunctionParser 实例，用于函数解析
    - op_dictionary: dict，算子支持信息字典 {算子类型: {is_supported: bool}}
    - dictionary_path: str，算子字典文件路径
    - supported_operators: set，支持的算子类型集合
    - type_resolver: FlinkTypeResolver 实例，用于类型解析
    - schema_chain: dict，当前作业的 schema 链缓存
    """

    # Excel 输出列定义，按输出顺序排列
    # 用于规范输出数据结构，确保列顺序一致
    EXCEL_COLUMNS = [
        ExcelColumns.JOB_ID, ExcelColumns.TASK_ID, ExcelColumns.STATUS,
        ExcelColumns.OPERATOR_NAME, ExcelColumns.IS_SUPPORTED, ExcelColumns.INPUT,
        ExcelColumns.OUTPUT, ExcelColumns.FREQUENCY, ExcelColumns.RUNTIME,
        ExcelColumns.INPUT_DATA_SIZE, ExcelColumns.OUTPUT_DATA_SIZE,
        ExcelColumns.FUNC_NAME, ExcelColumns.FUNC_INPUT,
        ExcelColumns.NESTED_CONTENT, ExcelColumns.FUNC_FREQUENCY
    ]

    # 数据源算子类型 - 这些算子产生数据，是数据流的起点
    SOURCE_OP_TYPES = {"Csv Source", "KafKa Source", "TableSourceScan", "Source"}

    # 透传算子类型 - 不改变 schema 结构，直接传递上游的输出
    # 这类算子包括去重、时间水印、时间戳插入等
    PASS_THROUGH_OP_TYPES = {"Deduplicate", "Expand", "WatermarkAssigner",
                             "StreamRecordTimestampInserter", "ConstraintEnforcer"}

    def __init__(self, table_schema=None, column_type=None, table_column_type=None):
        """
        初始化解析器

        参数说明:
        :param table_schema: dict，表结构字典 {表名: [字段列表]}
        :param column_type: dict，字段名→类型 的全局映射
        :param table_column_type: dict，表名.字段名→类型 的映射

        初始化流程:
        1. 创建函数解析器实例
        2. 初始化算子字典路径并加载字典
        3. 构建支持算子集合
        4. 创建类型解析器实例
        5. 初始化 schema 链缓存

        设计考虑:
        - 表结构信息用于类型推断，可选参数允许延迟加载
        - 算子字典在初始化时加载，避免运行时重复读取
        """
        # 创建函数解析器实例
        self.function_parser = FlinkFunctionParser()
        # 算子字典容器
        self.op_dictionary = {}
        # 算子字典文件路径
        self.dictionary_path = os.path.join(self.get_resource_path(), "flink_op_dictionary.json")
        # 加载算子字典
        self._load_op_dictionary()
        # 构建支持算子集合（用于快速查找）
        self.supported_operators = set(self.op_dictionary.keys())
        # 创建类型解析器
        self.type_resolver = FlinkTypeResolver(table_schema, column_type, table_column_type)
        # schema 链缓存
        self.schema_chain = {}

    @staticmethod
    def get_resource_path():
        """
        获取资源文件目录路径

        返回值:
        :return: str，资源目录的绝对路径

        实现原理:
        通过 CommonUtil.get_execute_path() 获取执行路径，拼接 resources 目录
        """
        return os.path.join(CommonUtil.get_execute_path(), "resources")

    @staticmethod
    def parse_single_description_line(line):
        """
        解析单行描述内容

        参数说明:
        :param line: str，原始行内容
        :return: 解析后的对象 (dict/list/str) 或 None

        实现逻辑:
        1. 去除行首尾的空白字符和基本分隔符 (:, +, -, 空格, tab)
        2. 检查是否以 { 或 [ 开头，尝试 JSON 解析
        3. 解析成功返回对象，失败返回原始字符串
        4. 空内容返回 None

        边界处理:
        - 空行返回 None
        - 纯空白行返回 None
        - JSON 解析失败返回原始字符串（不抛出异常）
        """
        if not line:
            return None
        # 清理首尾的分隔符和空白
        clean_part = line.strip(" :+- \t")
        if not clean_part:
            return None

        if (clean_part.startswith('{') and clean_part.endswith('}')) or \
                (clean_part.startswith('[') and clean_part.endswith(']')):
            try:
                return json.loads(clean_part)
            except (JSONDecodeError, TypeError) as e:
                logger.debug(f"Failed to parse JSON: {e}")
        # 返回原始字符串
        return clean_part

    @staticmethod
    def get_description(job_detail, job_id):
        """
        从作业详情中提取节点描述信息

        参数说明:
        :param job_detail: dict，Flink 作业详情 JSON
        :param job_id: str，作业 ID
        :return: dict，{job_id: {vertex_id: {plan_desc: [解析后的描述列表]}}}

        实现流程:
        1. 从 plan.nodes 提取每个节点的描述
        2. 对 HTML 转义字符进行反转义（处理 &lt;, &gt; 等）
        3. 按 <br/> 或换行符分割描述文本
        4. 对每行调用 parse_single_description_line 解析

        数据结构:
        返回的字典结构便于按 job_id 和 vertex_id 快速查找描述信息
        """
        plan = job_detail.get("plan", {})
        if not isinstance(plan, dict) or "nodes" not in plan:
            return {job_id: {}}

        vertex_map = {}
        for node in plan.get("nodes", []):
            vertex_id = node.get("id")
            if not vertex_id:
                continue
            description = node.get("description", "")
            # HTML 反转义
            if description:
                description = html.unescape(description)
            if not description:
                continue
            # 分割描述文本
            raw_parts = re.split(r"<br/>[\s:*\+\-]*|\n", description)
            description_data = [
                parsed_line for line in raw_parts
                if (parsed_line := FlinkParser.parse_single_description_line(line)) is not None
            ]
            vertex_map[vertex_id] = {"plan_desc": description_data}

        return {job_id: vertex_map}

    @staticmethod
    def filter_num_data(available, target_metrics):
        """
        过滤可用的指标 ID

        参数说明:
        :param available: list，可用指标列表 [{id: str, value: any}]
        :param target_metrics: list，目标指标 ID 后缀列表
        :return: list，匹配的指标 ID 列表

        过滤规则:
        - 指标的 id 以 target_metrics 中任一后缀结尾
        - 用于从大量可用指标中筛选出需要的指标

        使用场景:
        Flink API 返回的指标数量较多，通过后缀匹配筛选关注的指标
        例如: ["numRecordsIn", "numRecordsOut"] 将匹配 "operator.1.numRecordsIn"
        """
        if not available:
            return []
        return [m['id'] for m in available if any(m['id'].endswith(s) for s in target_metrics)]

    @staticmethod
    def operator_analysis(jobs, metrics):
        """
        分析作业中的算子信息及其关联指标

        参数说明:
        :param jobs: dict，作业数据 {job_id: {vertex_id: {plan_desc: []}}}
        :param metrics: dict，指标数据 {vertex_id: {metric_key: value}}
        :return: dict，按 job_id 和 vertex_id 分组的算子分析结果

        实现原理:
        1. 从描述中提取算子 ID 和类型: 模式 [数字]:字母（如 [1]:Calc）
        2. 从指标键中解析 vertex_id, op_id, metric_name
           - 指标键格式: vertex_id.operator[op_id].metric_name
        3. 按 vertex_id 和 op_id 聚合指标值（求和）
        4. 返回嵌套结构的分析结果

        返回结构:
        {
            job_id: {
                vertex_id: [
                    {"op_id": int, "op_type": str, "metrics": {metric_name: sum_value}}
                ]
            }
        }
        """
        # 正则模式: 匹配 [数字]:字母 格式
        # 用于从描述中提取算子 ID 和类型
        op_pattern = r"\[(\d+)\]:([A-Za-z]+)"
        ops = {
            m.group(1): {"type": m.group(2), "vertex": vertex_id, "job": job_id}
            for job_id, vertices in jobs.items()
            for vertex_id, vertex in vertices.items()
            for desc in vertex["plan_desc"]
            if (m := re.match(op_pattern, desc))
        }

        # 正则模式: 匹配指标键格式
        # 格式: vertex_id.operator[op_id].metric_name
        metric_pattern = r"(\d+)\.([A-Za-z_]+)\[(\d+)\]\.(\w+)"
        # 使用三层嵌套 defaultdict: agg[vertex_id][op_id][metric] = [values]
        agg = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for vertex_id, vertex_metrics in metrics.items():
            for key, value in vertex_metrics.items():
                if m := re.match(metric_pattern, key):
                    _, _, op_id, metric = m.groups()
                    agg[vertex_id][op_id][metric].append(float(value))

        # 构建输出结构
        out_put = {}
        for vertex_id, ops_metrics in agg.items():
            job_id = None
            for op_id, info in ops.items():
                if info["vertex"] == vertex_id:
                    job_id = info["job"]
                    break
            if job_id is None:
                continue
            out_put.setdefault(job_id, {})
            out_put[job_id].setdefault(vertex_id, [])

            for op_id, metrics_dict in ops_metrics.items():
                op_type = ops[op_id]["type"]
                out_put[job_id][vertex_id].append({
                    "op_id": int(op_id),
                    "op_type": op_type,
                    "metrics": {metric: sum(vals) for metric, vals in metrics_dict.items()}
                })
        return out_put

    @staticmethod
    def safe_float(val):
        """
        安全地将值转换为浮点数

        参数说明:
        :param val: any，待转换的值
        :return: float，转换后的浮点数，失败返回 0.0

        转换规则:
        - None 或空值: 返回 0.0
        - 数字类型: 直接转换
        - 字符串数字: 解析转换
        - 其他类型: 返回 0.0（不抛出异常）

        设计目的:
        处理来自 API 的数据，可能包含各种格式，避免转换异常
        """
        if not val:
            return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def group_metrics_by_operator(raw_map):
        """
        将原始指标按算子分组

        参数说明:
        :param raw_map: dict，{完整指标ID: 值}，如 {"operator.1.numRecordsIn": "12345"}
        :return: dict，{算子ID: {metric_name: 值}}

        解析规则:
        - 以 "." 分割，取前 n-1 部分组成算子ID，最后一部分为指标名
        - 支持的指标: numRecordsIn/Out, numRecordsIn/OutPerSecond, numBytesIn/Out, numBytesIn/OutPerSecond
        - PerSecond 指标保留两位小数，其他转为整数

        数据转换:
        - 字符串值转换为数字（使用 safe_float）
        - 吞吐量指标（PerSecond）保留两位小数
        - 计数指标（无 PerSecond）转换为整数

        返回结构:
        {
            "operator.1": {
                "numRecordsIn": 1000,
                "numRecordsInPerSecond": 100.50,
                ...
            }
        }
        """
        operator_stats = {}
        for full_id, val in raw_map.items():
            parts = full_id.split(".")
            if len(parts) < 2:
                continue
            # 前 n-1 部分组成算子ID
            operator = ".".join(parts[:-1])
            metric = parts[-1]
            # 初始化算子统计结构
            if operator not in operator_stats:
                operator_stats[operator] = {
                    "numRecordsIn": 0,
                    "numRecordsInPerSecond": 0.0,
                    "numRecordsOut": 0,
                    "numRecordsOutPerSecond": 0.0,
                    "numBytesIn": 0,
                    "numBytesInPerSecond": 0.0,
                    "numBytesOut": 0,
                    "numBytesOutPerSecond": 0.0,
                }
            # 更新指标值
            if metric in operator_stats[operator]:
                val_f = FlinkParser.safe_float(val)
                if "PerSecond" in metric:
                    # 吞吐量指标保留两位小数
                    operator_stats[operator][metric] = round(val_f, 2)
                else:
                    # 计数指标转为整数
                    operator_stats[operator][metric] = int(val_f)
        return operator_stats

    @staticmethod
    def calc_active_duration(operator_stats):
        """
        计算算子的活跃持续时间

        参数说明:
        :param operator_stats: dict，算子统计字典
        :return: dict，添加了 active_duration_in/out 字段的统计字典

        计算公式:
        - active_duration_in = numRecordsIn / numRecordsInPerSecond
        - active_duration_out = numRecordsOut / numRecordsOutPerSecond
        - 当速率 <= 0 时，持续时间为 0.0（避免除零错误）

        设计目的:
        估算算子实际处理数据的时间，用于性能分析

        返回结构（新增字段）:
        - active_duration_in: float，输入阶段活跃时间（秒）
        - active_duration_out: float，输出阶段活跃时间（秒）
        """
        for op, stats in operator_stats.items():
            rps_in = stats["numRecordsInPerSecond"]
            cnt_in = stats["numRecordsIn"]
            # 避免除零错误
            stats["active_duration_in"] = round(cnt_in / rps_in, 2) if rps_in > 0 else 0.0

            cnt_out = stats["numRecordsOut"]
            rps_out = stats["numRecordsOutPerSecond"]
            stats["active_duration_out"] = round(cnt_out / rps_out, 2) if rps_out > 0 else 0.0
        return operator_stats

    @staticmethod
    def calc_summary(operator_stats):
        """
        计算算子统计的总和汇总

        参数说明:
        :param operator_stats: dict，算子统计字典
        :return: dict，汇总指标字典

        返回结构:
        {
            "totalRecordsIn": int,      # 总输入记录数
            "totalRecordsOut": int,     # 总输出记录数
            "totalBytesIn": int,        # 总输入字节数
            "totalBytesOut": int,       # 总输出字节数
            "avgRecordsInPerSecond": int,   # 平均输入速率
            "avgRecordsOutPerSecond": int,  # 平均输出速率
        }

        计算逻辑:
        - total*: 所有算子对应指标求和
        - avg*: 所有算子对应指标求和后取整
        """
        return {
            "totalRecordsIn": sum(stats["numRecordsIn"] for stats in operator_stats.values()),
            "totalRecordsOut": sum(stats["numRecordsOut"] for stats in operator_stats.values()),
            "totalBytesIn": sum(stats["numBytesIn"] for stats in operator_stats.values()),
            "totalBytesOut": sum(stats["numBytesOut"] for stats in operator_stats.values()),
            "avgRecordsInPerSecond": round(sum(stats["numRecordsInPerSecond"] for stats in operator_stats.values())),
            "avgRecordsOutPerSecond": round(sum(stats["numRecordsOutPerSecond"] for stats in operator_stats.values())),
        }

    @staticmethod
    def restructure_by_op_type(analysis):
        """
        按算子类型重新组织分析结果

        参数说明:
        :param analysis: dict，原始分析结果 {job_id: {vertex_id: [算子信息]}}
        :return: dict，按算子类型分组的结果 {op_type: [算子信息列表]}

        用于统计同类型算子的总体情况，便于分析各类型算子的性能表现

        返回结构:
        {
            "Calc": [{"op_id": 1, "metrics": {...}}, ...],
            "GroupAggregate": [...],
            ...
        }
        """
        operators_by_type = {}
        for job_id, vertices in analysis.items():
            for vertex_id, ops_list in vertices.items():
                for op in ops_list:
                    op_type = op["op_type"]
                    operators_by_type.setdefault(op_type, [])
                    operators_by_type[op_type].append({
                        "op_id": op["op_id"],
                        "metrics": op["metrics"]
                    })
        return operators_by_type

    @staticmethod
    def aggregate_metrics(op_list):
        """
        聚合同类型算子的指标

        参数说明:
        :param op_list: list，同类型算子列表
        :return: tuple，(num_in, num_in_sec, num_out, num_out_sec)

        对列表中所有算子的吞吐量指标求和，用于后续计算运行时间

        返回值详解:
        - num_in: int，总输入记录数
        - num_in_sec: float，总输入速率
        - num_out: int，总输出记录数
        - num_out_sec: float，总输出速率
        """
        num_in = sum(op["metrics"].get("numRecordsIn", 0) for op in op_list)
        num_in_sec = sum(op["metrics"].get("numRecordsInPerSecond", 0.0) for op in op_list)
        num_out = sum(op["metrics"].get("numRecordsOut", 0) for op in op_list)
        num_out_sec = sum(op["metrics"].get("numRecordsOutPerSecond", 0.0) for op in op_list)
        return num_in, num_in_sec, num_out, num_out_sec

    @staticmethod
    def compute_runtime(num_in, num_in_sec, num_out, num_out_sec):
        """
        计算算子预估运行时间

        参数说明:
        :param num_in: int，输入记录数
        :param num_in_sec: float，输入每秒记录数
        :param num_out: int，输出记录数
        :param num_out_sec: float，输出每秒记录数
        :return: float，预估运行时间（秒，保留两位小数）

        计算原理:
        - 输入阶段耗时: num_in / num_in_sec
        - 输出阶段耗时: num_out / num_out_sec
        - 总耗时 = 输入耗时 + 输出耗时
        - 当速率 <= 0 时，对应阶段耗时为 0（避免除零错误）

        设计假设:
        假设输入和输出是串行处理的，总时间为两者之和
        """
        run_time = 0.0
        if num_in_sec > 0:
            run_time += num_in / num_in_sec
        if num_out_sec > 0:
            run_time += num_out / num_out_sec
        return round(run_time, 2)

    @staticmethod
    def bytes_to_mb(value):
        """
        将字节数转换为 MB

        参数说明:
        :param value: int，字节数
        :return: float，MB 值（保留两位小数）

        计算公式: MB = bytes / (1024 * 1024)

        边界处理:
        - 空值或 0 返回 0.0
        """
        if not value:
            return 0.0
        return round(value / (1024 * 1024), 2)

    @staticmethod
    def parse_performance_stats(vid, metrics_raw, jobs=None):
        if not metrics_raw or not isinstance(metrics_raw, list):
            return {"operators": {}, "summary": {}, "analysis": {}}
        valid_metrics = [item for item in metrics_raw if isinstance(item, dict) and 'id' in item and 'value' in item]
        if not valid_metrics:
            return {"operators": {}, "summary": {}, "analysis": {}}
        raw_map = {item['id']: item['value'] for item in valid_metrics}
        operator_stats = FlinkParser.group_metrics_by_operator(raw_map)
        # 计算活跃持续时间
        operator_stats = FlinkParser.calc_active_duration(operator_stats)
        # 计算汇总统计
        summary = FlinkParser.calc_summary(operator_stats)
        analysis = {}
        operators_by_type = {}
        # 如有作业数据，进行算子分析
        if jobs is not None:
            analysis = FlinkParser.operator_analysis(jobs, {vid: raw_map})
            operators_by_type = FlinkParser.restructure_by_op_type(analysis)

        return {
            "operators": operators_by_type,
            "summary": summary,
            "analysis": analysis
        }

    @staticmethod
    def _aggregate_functions_by_name(func_analysis):
        """
        按函数名聚合函数分析结果

        参数说明:
        :param func_analysis: list，函数分析列表
        :return: list，[(函数名, {count, inputs})]

        用于合并相同函数的多次出现，inputs 取并集

        返回结构:
        [
            ("upper", {"count": 2, "inputs": {"VARCHAR", "CHAR"}}),
            ...
        ]
        """
        aggregated = {}
        for func_info in func_analysis:
            name = func_info.get("func_name")
            if not name:
                continue
            input_types = func_info.get("input", [])
            # 使用 (函数名, 类型元组) 作为键
            key = (name, tuple(input_types)) if input_types else (name, ())
            if key not in aggregated:
                aggregated[key] = {"count": 0, "inputs": set(), "name": name}
            aggregated[key]["count"] += func_info.get("times", 0)
            aggregated[key]["inputs"].update(input_types)
        return [(info["name"], {"count": info["count"], "inputs": info["inputs"]}) for info in aggregated.values()]

    @staticmethod
    def _create_empty_row(job_id, task_id, status):
        """
        创建空的数据行

        参数说明:
        :param job_id: str，作业 ID
        :param task_id: str，任务 ID
        :param status: str，任务状态
        :return: dict，初始化为空字符串的行字典

        返回结构:
        包含 EXCEL_COLUMNS 中定义的所有字段，值为空字符串
        """
        return {
            ExcelColumns.JOB_ID: job_id,
            ExcelColumns.TASK_ID: task_id,
            ExcelColumns.STATUS: status,
            ExcelColumns.OPERATOR_NAME: "",
            ExcelColumns.IS_SUPPORTED: "",
            ExcelColumns.INPUT: "",
            ExcelColumns.OUTPUT: "",
            ExcelColumns.FREQUENCY: "",
            ExcelColumns.RUNTIME: "",
            ExcelColumns.INPUT_DATA_SIZE: "",
            ExcelColumns.OUTPUT_DATA_SIZE: "",
            ExcelColumns.FUNC_NAME: "",
            ExcelColumns.FUNC_INPUT: "",
            ExcelColumns.NESTED_CONTENT: "",
            ExcelColumns.FUNC_FREQUENCY: ""
        }

    @staticmethod
    def _create_row(job_id, task_id, status, op=None, func_name="", func_inputs_str="",
                    func_nested="", func_count=""):
        """
        创建包含算子或函数信息的数据行

        参数说明:
        :param job_id: str，作业 ID
        :param task_id: str，任务 ID
        :param status: str，状态
        :param op: dict，算子信息字典（可选）
        :param func_name: str，函数名
        :param func_inputs_str: str，函数输入类型字符串
        :param func_nested: str，嵌套内容
        :param func_count: str，函数出现次数
        :return: dict，填充了数据的行字典

        数据转换:
        - is_supported: bool -> "是"/"否"
        - 字节数 -> "X MB" 格式
        - 运行时间保持浮点值

        返回结构:
        包含 EXCEL_COLUMNS 中定义的所有字段，值已填充
        """
        is_supported_str = "是" if op.get("is_supported", False) else "否"
        task_status = (status == TaskStatus.SUCCESS)
        return {
            ExcelColumns.JOB_ID: job_id,
            ExcelColumns.TASK_ID: task_id,
            ExcelColumns.STATUS: status,
            ExcelColumns.OPERATOR_NAME: op["op_type"],
            ExcelColumns.IS_SUPPORTED: is_supported_str,
            ExcelColumns.INPUT: op.get("input_types_str", ""),
            ExcelColumns.OUTPUT: op.get("output_types_str", ""),
            ExcelColumns.FREQUENCY: op["count"],
            ExcelColumns.RUNTIME: op["run_time"] if task_status else "",
            ExcelColumns.INPUT_DATA_SIZE: f"{FlinkParser.bytes_to_mb(op['num_in'])}MB" if task_status else "",
            ExcelColumns.OUTPUT_DATA_SIZE: f"{FlinkParser.bytes_to_mb(op['num_out'])}MB" if task_status else "",
            ExcelColumns.FUNC_NAME: func_name,
            ExcelColumns.FUNC_INPUT: func_inputs_str,
            ExcelColumns.NESTED_CONTENT: func_nested,
            ExcelColumns.FUNC_FREQUENCY: func_count
        }

    @staticmethod
    def _strip_outer_parens(expr_str):
        """
        去除表达式最外层的成对括号

        参数说明:
        :param expr_str: str，表达式字符串
        :return: str，去除最外层括号后的字符串

        实现逻辑:
        1. 检查字符串是否以 ( 开头且以 ) 结尾
        2. 如果是，去除后检查内部括号是否匹配
        3. 重复此过程直到无法继续

        算法说明:
        使用深度计数器判断括号是否匹配
        - 遇到 ( 深度+1
        - 遇到 ) 深度-1
        - 如果深度在到达末尾前回到 0，说明括号不匹配

        示例:
        "((a + b))" -> "(a + b)" -> "a + b"
        "(a + b)" -> "a + b"
        "(a + b" -> "(a + b"（无法去除）
        """
        expr_str = expr_str.strip()
        while expr_str.startswith("(") and expr_str.endswith(")"):
            depth = 0
            matched = True
            for idx, ch in enumerate(expr_str):
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                # 如果深度在到达末尾前回到 0，说明括号不匹配
                if depth == 0 and idx < len(expr_str) - 1:
                    matched = False
                    break
            if matched:
                expr_str = expr_str[1:-1].strip()
            else:
                break
        return expr_str

    @staticmethod
    def _is_operator_func(func_name):
        """
        判断函数名是否为操作符形式

        参数说明:
        :param func_name: str，函数名
        :return: bool，True 表示是操作符

        判断标准:
        - 长度 <= 2
        - 不包含字母

        典型操作符: =, <, >, <=, >=, <>, !=, +, -, *, /, %
        """
        if not func_name:
            return False
        return len(func_name) <= 2 and not any(c.isalpha() for c in func_name)

    @staticmethod
    def _extract_operator_operands_from_desc(func_name, description):
        """
        从描述中提取操作符的左右操作数

        参数说明:
        :param func_name: str，操作符
        :param description: str，描述文本
        :return: tuple，(左操作数字符串, 右操作数字符串) 或 (None, None)

        实现逻辑:
        1. 清理 HTML 标签
        2. 查找操作符位置（应用误报过滤）
        3. 从操作符位置向左查找左操作数起点
        4. 从操作符位置向右查找右操作数终点
        5. 处理括号和引号的嵌套深度

        误报过滤（与 _is_operator_false_positive 相同）:
        - "=" 后紧跟 "["（数组索引）
        - "=" 前是字母（赋值语句）
        - 与相邻操作符组合（如 >=）
        - "*" 被括号包围（通配符）

        操作数提取规则:
        - 向左查找：遇到 ), ], }, =, , 停止
        - 向右查找：遇到 (, [, {, ), ], , 停止
        - 处理嵌套括号深度
        """
        if not description or not func_name:
            return None, None
        # 清理 HTML 标签
        clean_desc = re.sub(r'<[^>]+>[\s:*\+\-]*', ' ', description)
        op_pattern = re.compile(re.escape(func_name))
        for match in op_pattern.finditer(clean_desc):
            pos = match.start()
            op_end = match.end()
            # 误报过滤
            if func_name == '=' and op_end < len(clean_desc) and clean_desc[op_end] == '[':
                continue
            if func_name == '=' and pos > 0 and clean_desc[pos - 1].isalpha():
                continue
            if func_name in ('=', '<', '>') and pos > 0 and clean_desc[pos - 1] in ('<', '>', '!'):
                continue
            if func_name in ('=', '<', '>') and op_end < len(clean_desc) and clean_desc[op_end] == '=':
                continue
            if func_name == '*' and pos > 0 and clean_desc[pos - 1] == '(' and op_end < len(clean_desc) and clean_desc[
                op_end] == ')':
                continue

            # 向左查找左操作数起点
            left_start = pos - 1
            depth = 0
            while left_start >= 0:
                ch = clean_desc[left_start]
                if ch in (')', ']', '}'):
                    depth += 1
                elif ch in ('(', '[', '{'):
                    if depth == 0:
                        left_start += 1
                        break
                    depth -= 1
                elif depth == 0 and ch == '=':
                    if left_start + 1 < len(clean_desc) and clean_desc[left_start + 1] == '[':
                        depth += 1
                        left_start -= 1
                        continue
                    left_start += 1
                    break
                elif depth == 0 and ch == ',':
                    left_start += 1
                    break
                left_start -= 1
            else:
                left_start = 0
            if left_start < 0:
                left_start = 0

            # 向右查找右操作数终点
            right_end = op_end
            depth = 0
            while right_end < len(clean_desc):
                ch = clean_desc[right_end]
                if ch in ('(', '[', '{'):
                    depth += 1
                elif ch in (')', ']', '}'):
                    if depth == 0:
                        break
                    depth -= 1
                elif depth == 0 and ch in (',', ')', ']'):
                    break
                right_end += 1

            left_expr = clean_desc[left_start:pos].strip()
            right_expr = clean_desc[op_end:right_end].strip()
            if left_expr and right_expr:
                return left_expr, right_expr
        return None, None

    @staticmethod
    def _extract_function_args_text_from_desc(func_name, description):
        """
        从描述中提取函数的参数文本

        参数说明:
        :param func_name: str，函数名
        :param description: str，描述文本
        :return: str，参数字符串（不含括号）或 None

        实现逻辑:
        1. 操作符类型调用 _extract_operator_operands_from_desc，返回 "left,right" 格式
        2. 函数类型使用正则匹配函数调用 func_name(
        3. 解析括号匹配，提取括号内的参数

        括号匹配算法:
        - 使用深度计数器追踪嵌套
        - 从 ( 后开始，遇到 ) 且深度为 0 时结束
        """
        if not description or not func_name:
            return None
        # 操作符类型
        if FlinkParser._is_operator_func(func_name):
            left, right = FlinkParser._extract_operator_operands_from_desc(func_name, description)
            if left is not None and right is not None:
                return f"{left},{right}"
            return None
        # 函数类型
        func_pattern = re.compile(
            re.escape(func_name) + r'\s*\(', re.I
        )
        match = func_pattern.search(description)
        if not match:
            return None
        start = match.end()
        depth = 1
        for i in range(start, len(description)):
            if description[i] == "(":
                depth += 1
            elif description[i] == ")":
                depth -= 1
                if depth == 0:
                    return description[start:i]
        return description[start:]

    @staticmethod
    def _split_function_args(text):
        """
        按逗号分割函数参数

        参数说明:
        :param text: str，参数字符串
        :return: list，参数列表

        关键处理:
        - 忽略括号内的逗号（使用深度计数器）
        - 支持嵌套括号

        示例:
        "a, b, func(c, d), e" -> ["a", " b", " func(c, d)", " e"]
        """
        args = []
        current = []
        depth = 0
        for char in text:
            if char == "(":
                depth += 1
                current.append(char)
            elif char == ")":
                depth -= 1
                current.append(char)
            elif char == "," and depth == 0:
                args.append("".join(current))
                current = []
            else:
                current.append(char)
        if current:
            args.append("".join(current))
        return args

    @staticmethod
    def _extract_func_expression(func_name, description):
        """
        提取完整函数表达式

        参数说明:
        :param func_name: str，函数名
        :param description: str，描述文本
        :return: str，完整函数表达式或函数名

        包括函数名和参数部分的完整字符串

        实现逻辑:
        - 操作符类型返回 "left func right" 格式
        - 函数类型返回 "func_name(args)" 格式
        """
        if not description:
            return ""
        if FlinkParser._is_operator_func(func_name):
            left, right = FlinkParser._extract_operator_operands_from_desc(func_name, description)
            if left is not None and right is not None:
                return f"{left} {func_name} {right}"
            return func_name
        func_pattern = re.compile(
            re.escape(func_name) + r'\s*\(', re.I
        )
        match = func_pattern.search(description)
        if not match:
            return func_name
        start = match.start()
        depth = 0
        for i in range(match.end() - 1, len(description)):
            if description[i] == "(":
                depth += 1
            elif description[i] == ")":
                depth -= 1
                if depth == 0:
                    return description[start:i + 1]
        return description[start:]

    @staticmethod
    def _expr_to_string(expr):
        """
        将表达式对象转换为字符串表示

        参数说明:
        :param expr: dict 或 str，表达式对象
        :return: str，表达式字符串

        支持的表达式类型:
        - FIELD_REFERENCE: field[列名]
        - LITERAL: 字面量值
        - FUNCTION: 函数名(参数)
        - BINARY: 左 操作符 右
        - UNARY: 操作符(表达式)

        递归处理:
        对于嵌套表达式（如函数参数），递归调用此方法
        """
        if isinstance(expr, str):
            return expr
        if isinstance(expr, dict):
            expr_type = expr.get("exprType", "")
            if expr_type == "FIELD_REFERENCE":
                return f"field[{expr.get('colVal', '?')}]"
            if expr_type == "LITERAL":
                return str(expr.get("value", "?"))
            if expr_type == "FUNCTION":
                name = expr.get("function_name", "?")
                args = expr.get("arguments", [])
                args_str = ", ".join(FlinkParser._expr_to_string(a) for a in args)
                return f"{name}({args_str})"
            if expr_type == "BINARY":
                op = expr.get("operator", "?")
                left = FlinkParser._expr_to_string(expr.get("left", {}))
                right = FlinkParser._expr_to_string(expr.get("right", {}))
                return f"{left} {op} {right}"
            if expr_type == "UNARY":
                op = expr.get("operator", "?")
                inner = FlinkParser._expr_to_string(expr.get("expr", {}))
                return f"{op}({inner})"
        return str(expr)

    @staticmethod
    def _topological_sort(vertices):
        """
        对顶点进行拓扑排序

        参数说明:
        :param vertices: dict，{vertex_id: {upstream_ids: [...]}}
        :return: list，排序后的 vertex_id 列表

        实现算法: Kahn 算法（拓扑排序的经典算法）

        算法步骤:
        1. 计算所有顶点的入度（上游依赖数量）
        2. 将入度为 0 的顶点入队（无依赖的节点）
        3. 依次弹出顶点，加入结果列表
        4. 减少该顶点所有下游顶点的入度
        5. 如果下游顶点入度变为 0，加入队列
        6. 处理环（无法访问的顶点追加到末尾）

        时间复杂度: O(V + E)，V 为顶点数，E 为边数

        设计目的:
        确保处理顺序符合数据流方向，上游顶点先处理，下游顶点后处理
        """
        # 计算入度
        in_degree = {vid: 0 for vid in vertices}
        for vid, info in vertices.items():
            for uid in info.get("upstream_ids", []):
                if uid in in_degree:
                    in_degree[vid] += 1

        # 入度为 0 的顶点入队
        queue = [vid for vid, deg in in_degree.items() if deg == 0]
        result = []
        while queue:
            vid = queue.pop(0)
            result.append(vid)
            # 更新下游顶点的入度
            for other_id, info in vertices.items():
                if vid in info.get("upstream_ids", []):
                    in_degree[other_id] -= 1
                    if in_degree[other_id] == 0:
                        queue.append(other_id)

        # 处理环（无法访问的顶点）
        for vid in vertices:
            if vid not in result:
                result.append(vid)

        return result

    @staticmethod
    def _merge_upstream_context(upstream_ids, vertex_context):
        """
        合并上游顶点的上下文信息

        参数说明:
        :param upstream_ids: list，上游顶点 ID 列表
        :param vertex_context: dict，{vertex_id: {alias_map, output_schema}}
        :return: dict，合并后的上下文

        合并策略:
        - alias_map: 取所有上游的并集
        - output_schema: 取第一个有 schema 的上游的 schema（多个上游时取第一个）

        设计原因:
        - 别名需要合并，因为可能有多个上游提供不同的别名
        - Schema 取第一个是因为通常只有一个主要数据源，多个上游时需要特殊处理

        返回结构:
        {
            "alias_map": dict，合并后的别名映射,
            "output_schema": list，输出 schema
        }
        """
        merged_alias_map = {}
        merged_output_schema = []
        for uid in upstream_ids:
            ctx = vertex_context.get(uid)
            if not ctx:
                continue
            # 合并别名映射
            merged_alias_map.update(ctx.get("alias_map", {}))
            # 取第一个有 schema 的上游
            if ctx.get("output_schema"):
                if not merged_output_schema:
                    merged_output_schema = list(ctx["output_schema"])
                else:
                    merged_output_schema.extend(ctx["output_schema"])
        return {
            "alias_map": merged_alias_map,
            "output_schema": merged_output_schema,
        }

    @staticmethod
    def _aggregate_ops_by_name_and_types(ops):
        """
        按算子类型聚合

        参数说明:
        :param ops: list，算子列表
        :return: list，聚合后的算子列表

        聚合规则:
        - 按 (op_type, input_types_str, output_types_str) 分组
        - 相同组的 count、num_in、num_out 累加

        设计目的:
        合并相同类型、相同输入输出类型的算子，减少输出行数
        """
        aggregated = {}
        for op in ops:
            key = (op["op_type"], op["input_types_str"], op["output_types_str"])
            if key in aggregated:
                aggregated[key]["count"] += op["count"]
                aggregated[key]["num_in"] += op["num_in"]
                aggregated[key]["num_out"] += op["num_out"]
            else:
                aggregated[key] = dict(op)
        return list(aggregated.values())

    @staticmethod
    def _extract_op_type(desc_item):

        if not isinstance(desc_item, str):
            return None

        op_match = re.match(r'\[(\d+)\]:([A-Za-z]+)', desc_item)

        if not op_match:
            return None

        return op_match.group(2)

    @staticmethod
    def _aggregate_metrics_by_type(operators_metrics):
        """
        将 operators_metrics 聚合成 {op_type: {num_in, num_out, run_time, count}}
        """
        metrics_by_type = {}
        for op_type, op_list in operators_metrics.items():
            num_in, num_in_sec, num_out, num_out_sec = FlinkParser.aggregate_metrics(op_list)
            metrics_by_type[op_type] = {
                "num_in": num_in,
                "num_out": num_out,
                "run_time": FlinkParser.compute_runtime(num_in, num_in_sec, num_out, num_out_sec),
                "count": len(op_list),
            }
        return metrics_by_type

    def set_table_schema(self, table_schema, column_type, table_column_type):
        """
        更新表 schema 配置

        参数说明:
        :param table_schema: dict，表结构
        :param column_type: dict，字段类型
        :param table_column_type: dict，表字段类型

        设计目的:
        允许延迟加载表结构信息，在解析过程中动态更新
        """
        self.type_resolver = FlinkTypeResolver(table_schema, column_type, table_column_type)

    def _analyze_operators_functions(self, description, task_id, input_schema=None):
        """
        分析算子描述中的函数

        参数说明:
        :param description: str，算子描述文本
        :param task_id: str，任务 ID
        :param input_schema: list，输入 schema（可选）
        :return: list，不支持函数的列表

        分析流程:
        1. 构建函数参数类型映射
        2. 分析不支持的函数
        3. 解析每个不支持函数的参数类型和嵌套内容
        4. 返回标准化的结果列表

        返回结构:
        [
            {
                "func_name": str，函数名,
                "task_id": str，任务 ID,
                "input": list，输入类型列表,
                "nested_content": str，嵌套内容,
                "times": int，出现次数,
                "unsupported_types": list（可选），不支持的类型
            }
        ]
        """
        if not description:
            return []

        # 构建函数参数类型映射
        param_types_map = self._build_func_param_types_map(description, input_schema)

        # 分析不支持的函数
        unsupported_funcs = self.function_parser.analyze_unsupported_functions(
            description, param_types_map
        )
        result = []
        for func in unsupported_funcs:
            func_name = func["func_name"]
            # 解析参数类型和嵌套内容
            func_input_types, nested_content = self._resolve_func_param_types(
                func_name, description, input_schema
            )
            # 如果所有类型都已知，清空嵌套内容
            if func_input_types and all(t != "unknown" for t in func_input_types):
                nested_content = ""
            entry = {
                "func_name": func_name,
                "task_id": task_id,
                "input": func_input_types,
                "nested_content": nested_content,
                "times": func["times"]
            }
            # 添加不支持的类型列表
            unsupported_types = func.get("unsupported_types", [])
            if unsupported_types:
                entry["unsupported_types"] = unsupported_types
            result.append(entry)
        return result

    def _build_func_param_types_map(self, description, input_schema=None):
        """
        构建函数参数类型映射

        参数说明:
        :param description: str，描述文本
        :param input_schema: list，输入 schema
        :return: dict，{函数名: [参数类型列表]}

        实现逻辑:
        1. 解析描述中的所有函数
        2. 对每个函数解析其参数类型
        3. 构建映射字典

        设计目的:
        为函数分析提供参数类型信息，用于判断函数是否支持
        """
        param_types_map = {}
        all_funcs = self.function_parser.parse_plan_description(description)
        for func_info in all_funcs:
            func_name = func_info.get("func", "")
            if not func_name:
                continue
            types, _ = self._resolve_func_param_types(func_name, description, input_schema)
            if types:
                param_types_map[func_name] = types
        return param_types_map

    def _resolve_func_param_types(self, func_name, description, input_schema=None):
        """
        解析函数的参数类型

        参数说明:
        :param func_name: str，函数名
        :param description: str，描述文本
        :param input_schema: list，输入 schema
        :return: tuple，(参数类型列表, 嵌套内容)

        解析策略:
        1. 先检查函数返回类型字典
        2. 尝试从 JSON 描述中解析（如果存在）
        3. 从文本描述中解析（备用方案）

        返回值:
        - input_types: list，参数类型列表
        - nested_content: str，无法解析的嵌套内容（用于调试）
        """
        input_types = []
        nested_content = ""

        func_name_lower = func_name.lower()
        # 检查返回类型字典
        dict_entry = self.type_resolver.return_type_dict.get(func_name_lower)

        if dict_entry:
            return_type = dict_entry.get("return_type", "unknown")
            if return_type == "unknown":
                # 需要提取嵌套内容
                nested_content = self._extract_func_expression(func_name, description)

        # 尝试从 JSON 描述中解析
        json_desc = self._find_json_desc_in_text(description)
        if json_desc:
            input_types, nested_content = self._resolve_func_types_from_json(
                func_name, json_desc, input_schema, nested_content
            )
        else:
            # 从文本描述中解析
            input_types, nested_content = self._resolve_func_types_from_text(
                func_name, description, input_schema, nested_content
            )

        return input_types, nested_content

    def _find_json_desc_in_text(self, description):
        """
        在描述中查找 JSON 对象

        参数说明:
        :param description: str/dict/list，描述
        :return: dict，第一个匹配的 JSON 对象

        识别条件:
        - 包含 inputTypes、outputTypes 或 originDescription 字段的字典

        支持的输入类型:
        - dict: 直接返回
        - list: 查找列表中的 JSON 对象
        - str: 先解析为列表，再查找
        """
        if not description:
            return None
        if isinstance(description, dict):
            return description

        if isinstance(description, list):
            json_descs = self.type_resolver.find_json_descriptions(description)
            if json_descs:
                return json_descs[0]
            return None

        if isinstance(description, str):
            desc_data = []
            for line in re.split(r"<br/>[\s:*\+\-]*|\n", description):
                parsed = FlinkParser.parse_single_description_line(line)
                if parsed is not None:
                    desc_data.append(parsed)
            json_descs = self.type_resolver.find_json_descriptions(desc_data)
            if json_descs:
                return json_descs[0]
        return None

    def _resolve_func_types_from_json(self, func_name, json_desc, input_schema, existing_nested):
        """
        从 JSON 描述中解析函数类型

        参数说明:
        :param func_name: str，函数名
        :param json_desc: dict，JSON 描述对象
        :param input_schema: list，输入 schema
        :param existing_nested: str，已有的嵌套内容
        :return: tuple，(参数类型列表, 嵌套内容)

        解析逻辑:
        1. 从 indices 和 condition 中查找函数调用
        2. 对于 FUNCTION 类型表达式，解析参数类型
        3. 对于 BINARY 类型表达式，解析左右操作数类型

        返回值:
        - input_types: list，解析出的参数类型
        - nested_content: str，无法解析的表达式字符串（用于调试）
        """
        input_types = []
        nested_content = existing_nested

        indices = json_desc.get("indices", [])
        condition = json_desc.get("condition")

        all_exprs = list(indices)
        if condition:
            all_exprs.append(condition)

        for expr in all_exprs:
            if isinstance(expr, dict):
                expr_type = expr.get("exprType", "")
                # FUNCTION 类型
                if expr_type == "FUNCTION" and expr.get("function_name", "").lower() == func_name.lower():
                    arguments = expr.get("arguments", [])
                    for arg in arguments:
                        t = self.type_resolver.resolve_expression_type(arg, input_schema)
                        if t == "unknown":
                            nested_content = nested_content or self._expr_to_string(arg)
                        input_types.append(t)
                # BINARY 类型（操作符）
                elif expr_type == "BINARY" and expr.get("operator", "").lower() == func_name.lower():
                    left_expr = expr.get("left", {})
                    right_expr = expr.get("right", {})
                    left_type = self.type_resolver.resolve_expression_type(left_expr, input_schema)
                    right_type = self.type_resolver.resolve_expression_type(right_expr, input_schema)
                    if left_type == "unknown":
                        nested_content = nested_content or self._expr_to_string(left_expr)
                    if right_type == "unknown":
                        nested_content = nested_content or self._expr_to_string(right_expr)
                    input_types.append(left_type)
                    input_types.append(right_type)

        return input_types, nested_content

    def _resolve_func_types_from_text(self, func_name, description, input_schema, existing_nested):
        """
        从文本描述中解析函数类型

        参数说明:
        :param func_name: str，函数名
        :param description: str，描述文本
        :param input_schema: list，输入 schema
        :param existing_nested: str，已有的嵌套内容
        :return: tuple，(参数类型列表, 嵌套内容)

        解析策略:
        1. 操作符类型：调用 _resolve_operator_param_types_from_text
        2. CASE 语句：特殊处理
        3. CAST/TRY_CAST：特殊处理
        4. 普通函数：解析参数并逐个判断类型

        返回值:
        - input_types: list，参数类型列表
        - nested_content: str，无法解析的内容
        """
        input_types = []
        nested_content = existing_nested

        # 操作符类型
        if self._is_operator_func(func_name):
            input_types, nested_content = self._resolve_operator_param_types_from_text(
                func_name, description, input_schema, existing_nested
            )
        else:
            args_str = self._extract_function_args_text_from_desc(func_name, description)
            if args_str:
                # CASE 语句
                if func_name.lower() == "case":
                    input_types, nested_content = self._resolve_case_param_types_from_text(
                        args_str, input_schema, nested_content
                    )
                # CAST/TRY_CAST
                elif func_name.lower() in ("cast", "try_cast"):
                    input_types, nested_content = self._resolve_cast_param_types_from_text(
                        args_str, input_schema, nested_content
                    )
                # 普通函数
                else:
                    args = self._split_function_args(args_str)
                    for arg in args:
                        arg = arg.strip()
                        if not arg:
                            continue
                        t = self.type_resolver.resolve_expression_type(arg, input_schema)
                        if t == "unknown":
                            nested_content = nested_content or arg
                        input_types.append(t)

        # 如果没有嵌套内容，尝试提取完整表达式
        if not nested_content:
            full_expr = self._extract_func_expression(func_name, description)
            if full_expr and full_expr != func_name:
                nested_content = full_expr

        return input_types, nested_content

    def _resolve_operator_param_types_from_text(self, func_name, description, input_schema, existing_nested):
        """
        解析操作符的参数类型

        参数说明:
        :param func_name: str，操作符名
        :param description: str，描述文本
        :param input_schema: list，输入 schema
        :param existing_nested: str，已有的嵌套内容
        :return: tuple，(参数类型列表, 嵌套内容)

        实现逻辑:
        1. 提取操作符的左右操作数
        2. 分别解析左右操作数的类型
        3. 返回类型列表和嵌套内容
        """
        input_types = []
        nested_content = existing_nested

        left_expr, right_expr = self._extract_operator_operands_from_desc(func_name, description)
        if left_expr is not None and right_expr is not None:
            left_type = self.type_resolver.resolve_expression_type(left_expr, input_schema)
            right_type = self.type_resolver.resolve_expression_type(right_expr, input_schema)
            if left_type == "unknown":
                nested_content = nested_content or left_expr
            if right_type == "unknown":
                nested_content = nested_content or right_expr
            input_types.append(left_type)
            input_types.append(right_type)

        return input_types, nested_content

    def _resolve_case_param_types_from_text(self, args_str, input_schema, existing_nested):
        """
        解析 CASE 语句的参数类型

        参数说明:
        :param args_str: str，参数字符串
        :param input_schema: list，输入 schema
        :param existing_nested: str，已有的嵌套内容
        :return: tuple，(参数类型列表, 嵌套内容)

        CASE 语法:
        CASE 条件1 值1 条件2 值2 ... [ELSE 值]

        解析逻辑:
        - 按逗号分割参数
        - 跳过条件，只处理值部分（索引为奇数的参数）
        - 最后一个参数如果是 ELSE 值，也需要处理
        """
        input_types = []
        nested_content = existing_nested

        args = FlinkTypeResolver._split_function_args(args_str)
        i = 0
        while i < len(args):
            if i + 1 < len(args):
                # 处理条件（索引为偶数）
                condition_arg = args[i].strip()
                condition_arg = self._strip_outer_parens(condition_arg)
                t = self.type_resolver.resolve_expression_type(condition_arg, input_schema)
                if t == "unknown":
                    nested_content = nested_content or condition_arg
                input_types.append(t)
                i += 2
            else:
                break

        return input_types, nested_content

    def _resolve_cast_param_types_from_text(self, args_str, input_schema, existing_nested):
        """
        解析 CAST 函数的参数类型

        参数说明:
        :param args_str: str，参数字符串
        :param input_schema: list，输入 schema
        :param existing_nested: str，已有的嵌套内容
        :return: tuple，(参数类型列表, 嵌套内容)

        CAST 语法: CAST(expr AS type)

        解析逻辑:
        1. 使用 AS 分割表达式和目标类型
        2. 解析原始表达式的类型
        3. 返回类型列表（只包含输入类型，目标类型不作为参数）
        """
        input_types = []
        nested_content = existing_nested

        # 使用类型解析器的方法分割
        original_expr, target_type_str = FlinkTypeResolver._split_alias_from_expr(args_str)
        # 如果没有分割成功，尝试用 AS 分割
        if original_expr == args_str and " AS " in args_str.upper():
            parts = re.split(r'\s+AS\s+', args_str, maxsplit=1, flags=re.I)
            if len(parts) == 2:
                original_expr = parts[0].strip()
                target_type_str = parts[1].strip()

        # 解析原始表达式的类型
        t = self.type_resolver.resolve_expression_type(original_expr, input_schema)
        if t == "unknown":
            nested_content = nested_content or original_expr
        input_types.append(t)

        return input_types, nested_content

    def _load_op_dictionary(self):
        """
        加载算子字典配置

        错误处理:
        - 文件不存在: 抛出 FileNotFoundError
        - JSON 格式错误: 抛出 ValueError
        - 其他错误: 抛出 Exception

        配置文件格式:
        {
            "Calc": {"is_supported": true},
            "GroupAggregate": {"is_supported": false},
            ...
        }
        """
        try:
            with open(self.dictionary_path, "r", encoding="utf-8") as f:
                self.op_dictionary = json.load(f)
            logger.info(f"Loaded op dictionary from {self.dictionary_path}, total {len(self.op_dictionary)} entries")
        except FileNotFoundError:
            raise FileNotFoundError(f"Op dictionary file not found: {self.dictionary_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON format in dictionary file: {self.dictionary_path}")
        except Exception as e:
            raise Exception(f"Unexpected error while loading dictionary file: {self.dictionary_path}, error: {e}")

    def is_operator_supported(self, operator_type):
        """
        判断算子是否支持

        参数说明:
        :param operator_type: str，算子类型名称
        :return: bool，True 表示支持

        查找逻辑:
        - 在 op_dictionary 中查找算子类型
        - 返回 is_supported 字段，默认为 False
        """
        op_info = self.op_dictionary.get(operator_type, {})
        return op_info.get("is_supported", False)

    def filter_operators_by_whitelist(self, operators_metrics):
        """
        按白名单过滤算子

        参数说明:
        :param operators_metrics: dict，算子指标字典
        :return: dict，不在白名单中的算子

        过滤规则:
        - 保留不支持的算子（is_supported=False）
        - 移除支持的算子（is_supported=True）

        设计目的:
        只关注不支持的算子，减少输出数据量
        """
        filtered = {}
        for op_type, op_list in operators_metrics.items():
            if self.is_operator_supported(op_type):
                logger.debug(f"Operator {op_type} is supported, keeping")
            else:
                filtered[op_type] = op_list
                logger.debug(f"Operator {op_type} is not supported, filtering out")
        return filtered

    def parse_job_data(self, json_data):
        """
        解析作业数据

        参数说明:
        :param json_data: dict，作业 JSON 数据
        :return: list，分析结果行列表

        处理流程:
        1. 遍历每个作业
        2. 调用 _process_job 处理单个作业
        3. 收集所有结果行

        返回结构:
        符合 Excel 导出格式的行列表，每行为一个字典
        """
        result = []
        logger.info(f"Starting to parse job data, total {len(json_data)} jobs")
        for job_id, job_info in json_data.items():
            self._process_job(job_id, job_info, result)
        logger.info(f"Finished parsing job data, total {len(result)} records")
        return result

    def _collect_valid_operators(self, operators_metrics):
        """
        收集有效算子信息

        参数说明:
        :param operators_metrics: dict，算子指标字典
        :return: list，算子信息列表

        处理流程:
        1. 遍历每个算子类型
        2. 聚合指标
        3. 计算运行时间
        4. 构建算子信息字典

        返回结构:
        [
            {
                "op_type": str，算子类型,
                "is_supported": bool，是否支持,
                "num_in": int，输入记录数,
                "num_out": int，输出记录数,
                "run_time": float，运行时间,
                "count": int，出现次数,
                "input_types_str": str，输入类型字符串,
                "output_types_str": str，输出类型字符串,
            }
        ]
        """
        ops = []
        for op_type, op_list in operators_metrics.items():
            # 聚合指标
            num_in, num_in_sec, num_out, num_out_sec = FlinkParser.aggregate_metrics(op_list)
            # 计算运行时间
            run_time = FlinkParser.compute_runtime(num_in, num_in_sec, num_out, num_out_sec)
            ops.append({
                "op_type": op_type,
                "is_supported": self.is_operator_supported(op_type),
                "num_in": num_in,
                "num_out": num_out,
                "run_time": run_time,
                "count": len(op_list),
                "input_types_str": "",
                "output_types_str": "",
            })
        return ops

    def _build_schema_chain_for_vertex(self, description_data, upstream_context=None):
        """构建当前 vertex 的 schema chain"""

        schema_chain = []

        upstream_context = upstream_context or {}
        upstream_alias_map = upstream_context.get("alias_map", {})
        upstream_output_schema = upstream_context.get("output_schema")

        # 当前输入 schema（来自上游）
        current_input = upstream_output_schema
        # 累积的别名映射
        accumulated_alias_map = dict(upstream_alias_map)

        if accumulated_alias_map:
            self.type_resolver.update_alias_map(accumulated_alias_map)

        for desc_item in description_data:
            op_type = self._extract_op_type(desc_item)
            # source 节点处理
            if self._is_source_or_first_op(op_type, schema_chain, current_input):
                tables, output_schema = (self.type_resolver.extract_table_source_info(description_data))

                if output_schema:
                    self._append_source_schema_chain(
                        schema_chain=schema_chain,
                        op_type=op_type,
                        tables=tables,
                        output_schema=output_schema,
                        alias_map=accumulated_alias_map,
                    )
                    current_input = output_schema
                continue

            # 普通 operator
            if op_type and current_input is not None:
                output_schema = self.type_resolver.build_output_schema(op_type, description_data, current_input)
                op_alias_map = (self.type_resolver.extract_alias_map_from_description(description_data))
                accumulated_alias_map.update(op_alias_map)
                self.type_resolver.update_alias_map(accumulated_alias_map)
                schema_chain.append({
                    "op_type": op_type,
                    "input_schema": current_input,
                    "output_schema": output_schema,
                    "alias_map": dict(accumulated_alias_map),
                })

                current_input = output_schema

        # fallback
        if not schema_chain:
            self._append_upstream_or_source(
                schema_chain=schema_chain,
                current_input=current_input,
                description_data=description_data,
                alias_map=accumulated_alias_map,
            )

        return schema_chain

    def _is_source_or_first_op(self, op_type, schema_chain, current_input):

        return (
                op_type in self.SOURCE_OP_TYPES
                or (
                        not schema_chain
                        and op_type is None
                        and not current_input
                )
        )

    def _append_source_schema_chain(self, schema_chain, op_type, tables, output_schema, alias_map):
        schema_chain.append({
            "op_type": op_type or "Source",
            "input_schema": [],
            "output_schema": output_schema,
            "tables": tables,
            "alias_map": dict(alias_map),
        })

        self._update_table_column_types(tables)

    def _update_table_column_types(self, tables):

        for table_name in tables:

            table_cols = self.type_resolver.table_schema.get(table_name, [])

            if not table_cols:
                continue

            col_type, table_col_type = (
                TableSchemaReader.build_column_type_mapping(self.type_resolver.table_schema, {table_name})
            )

            self.type_resolver.update_column_type(col_type, table_col_type)

    def _append_upstream_or_source(self, schema_chain, current_input, description_data, alias_map):

        # upstream fallback
        if current_input:
            schema_chain.append({
                "op_type": "Upstream",
                "input_schema": [],
                "output_schema": current_input,
                "alias_map": dict(alias_map),
            })

            return

        # source fallback
        if not description_data:
            return

        tables, output_schema = (self.type_resolver.extract_table_source_info(description_data))

        if not output_schema:
            return

        self._append_source_schema_chain(
            schema_chain=schema_chain,
            op_type="Source",
            tables=tables,
            output_schema=output_schema,
            alias_map=alias_map,
        )

    def _get_input_schema_for_op(self, op_type, schema_chain):
        """
        获取算子的输入 schema

        参数说明:
        :param op_type: str，算子类型
        :param schema_chain: list，schema 链
        :return: list 或 None，输入 schema

        查找逻辑:
        1. 在 schema 链中查找匹配的算子类型
        2. 如果找到，返回其输入 schema
        3. 如果未找到，返回最后一个条目（透传算子）的输出 schema

        特殊处理:
        - PASS_THROUGH_OP_TYPES: 直接返回上游 schema
        """
        if not schema_chain:
            return None

        for i, entry in enumerate(schema_chain):
            if entry.get("op_type") == op_type:
                return entry.get("input_schema")

        if schema_chain:
            last = schema_chain[-1]
            if op_type in self.PASS_THROUGH_OP_TYPES:
                return last.get("output_schema")
            return last.get("output_schema")

        return None

    def _process_job(self, job_id, job_info, result):
        """
        处理单个作业

        参数说明:
        :param job_id: str，作业 ID
        :param job_info: dict，作业信息
        :param result: list，结果列表（用于收集输出）

        处理流程:
        1. 从作业信息中提取顶点列表
        2. 对顶点进行拓扑排序
        3. 按顺序处理每个顶点
        4. 收集任务数据并构建结果行

        核心逻辑:
        - 使用拓扑排序确保处理顺序符合数据流方向
        - 维护 vertex_context 保存每个顶点的输出 schema 和别名映射
        - 合并上游上下文用于类型推断
        """
        vertices = job_info.get("vertices")
        if not vertices:
            logger.warning(f"No vertices found for job {job_id}")
            return
        logger.debug(f"Processing job {job_id}, total {len(vertices)} vertices")

        # 对顶点进行拓扑排序
        sorted_ids = self._topological_sort(vertices)

        # 顶点上下文，保存每个顶点的输出 schema 和别名映射
        vertex_context = {}
        task_data = {}
        for task_id in sorted_ids:
            task_info = vertices.get(task_id)
            if not task_info:
                continue

            # 获取上游顶点 ID
            upstream_ids = task_info.get("upstream_ids", [])
            # 合并上游上下文
            upstream_context = self._merge_upstream_context(upstream_ids, vertex_context)

            # 收集任务数据
            data = self._collect_task_data(task_id, task_info, upstream_context)
            if not data:
                continue

            # 保存当前顶点的上下文
            vertex_context[task_id] = {
                "output_schema": data.get("output_schema"),
                "alias_map": data.get("alias_map", {}),
            }

            # 初始化任务数据结构
            if task_id not in task_data:
                task_data[task_id] = {"jobid": job_id, "taskid": task_id, "status": data["status"],
                                      "ops": [], "func_list": []}

            # 累加算子和函数信息
            task_data[task_id]["ops"].extend(data["ops"])
            task_data[task_id]["func_list"].extend(data["func_list"])

        # 为每个任务构建 Excel 行
        for data in task_data.values():
            result.extend(self._build_rows_for_task(data))

    def _collect_task_data(self, task_id, task_info, upstream_context=None):
        """
        收集任务数据

        参数说明:
        :param task_id: str，任务 ID
        :param task_info: dict，任务信息
        :param upstream_context: dict，上游上下文（可选）
        :return: dict 或 None，任务数据

        收集内容:
        1. 任务状态
        2. 算子指标
        3. 逻辑元数据和描述
        4. 构建 schema 链
        5. 提取算子和函数信息

        返回结构:
        {
            "status": str，任务状态,
            "ops": list，算子列表,
            "func_list": list，函数列表,
            "output_schema": list（可选），输出 schema,
            "alias_map": dict（可选），别名映射
        }
        """
        status = task_info.get("status", "UNKNOWN")
        operators_metrics = task_info.get("summary_metrics", {}).get("operators", {})
        logic_metadata = task_info.get("logic_metadata", {})
        full_description = logic_metadata.get("full_description", "")
        description_data = task_info.get("description_data", [])

        upstream_context = upstream_context or {}
        # 构建 schema 链
        schema_chain = self._build_schema_chain_for_vertex(description_data,
                                                           upstream_context) if description_data else []

        # 从 schema 链构建算子信息
        ops = self._build_ops_from_schema_chain(schema_chain, operators_metrics)

        # 获取函数分析所需的输入 schema
        func_input_schema = None
        final_alias_map = {}
        if schema_chain:
            last = schema_chain[-1]
            func_input_schema = last.get("output_schema")
            final_alias_map = last.get("alias_map", {})

        # 分析算子中的函数
        func_list = [
            {
                "func_name": f.get("func_name"),
                "times": f.get("times", 0),
                "input": f.get("input", []),
                "nested_content": f.get("nested_content", ""),
            }
            for f in self._analyze_operators_functions(full_description, task_id, func_input_schema)
            if f.get("func_name")
        ]

        # 构建结果
        result = {"status": status, "ops": ops, "func_list": func_list}
        if schema_chain:
            result["output_schema"] = schema_chain[-1].get("output_schema")
            result["alias_map"] = final_alias_map
        # ★ 修改点：任务指标获取失败时不返回 None，确保 jobID、taskID 和状态信息能传递到输出
        return result

    def _build_ops_from_schema_chain(self, schema_chain, operators_metrics, description_data=None):
        """
        构建 ops 列表：
          - schema_chain 指定了输入输出 schema
          - operators_metrics 提供实际执行 metrics
          - description_data 用于 fallback type 提取
        """
        if not schema_chain:
            # schema_chain 不存在时直接从 metrics 收集
            ops = self._collect_valid_operators(operators_metrics)
            return self._aggregate_ops_by_name_and_types(ops)

        metrics_by_type = self._aggregate_metrics_by_type(operators_metrics)
        type_occurrence = {}
        ops = []

        # 遍历 schema_chain 构建 ops
        for entry in schema_chain:
            op_type = entry.get("op_type", "")
            if op_type == "Upstream":
                continue

            input_types_str = self._extract_input_types_from_schema_entry(
                entry, description_data
            )

            # 获取指标信息
            metrics = metrics_by_type.get(op_type, {})
            type_occurrence[op_type] = type_occurrence.get(op_type, 0) + 1
            total_of_type = sum(1 for e in schema_chain if e.get("op_type") == op_type)
            count = 1 if total_of_type > 1 else metrics.get("count", 1)

            ops.append(self._build_op_entry(op_type, input_types_str, metrics, count))

        # 补充 metrics 中存在但 schema_chain 没有的 ops
        self._append_missing_metrics_ops(ops, metrics_by_type, schema_chain, description_data)

        return self._aggregate_ops_by_name_and_types(ops)

    def _extract_input_types_from_schema_entry(self, chain_entry, description_data):
        """
        从 schema_chain entry 中提取 input_types_str：
          - 展开 ROW 类型
          - fallback description_data
        """
        op_type = chain_entry.get("op_type", "")
        output_schema = chain_entry.get("output_schema", [])
        input_types = []

        if output_schema:
            for f in output_schema:
                field_type = f.get("field_type", "unknown")
                if field_type == "ROW":
                    original_type = f.get("original_type", "")
                    if original_type and original_type.upper().startswith("ROW"):
                        input_types.extend(TypeNormalizer.expand_row_type(original_type))
                    else:
                        input_types.append(field_type)
                else:
                    input_types.append(field_type)

        input_str = ",".join(t for t in input_types if t)

        # fallback: schema_chain 没有类型但 description_data 有
        if not input_str and not self.is_operator_supported(op_type) and description_data:
            input_str = self._extract_types_from_description(op_type, description_data, chain_entry.get("input_schema"))

        return input_str

    def _build_op_entry(self, op_type, input_types_str, metrics, count):
        """
        构建单条 op dict
        """
        return {
            "op_type": op_type,
            "is_supported": self.is_operator_supported(op_type),
            "num_in": metrics.get("num_in", 0),
            "num_out": metrics.get("num_out", 0),
            "run_time": metrics.get("run_time", 0.0),
            "count": count,
            "input_types_str": input_types_str,
            "output_types_str": "",
        }

    def _append_missing_metrics_ops(self, ops, metrics_by_type, schema_chain, description_data):
        """
        补充 metrics 中存在但 schema_chain 没有的 op
        """
        for op_type, metrics in metrics_by_type.items():
            if any(e.get("op_type") == op_type for e in schema_chain):
                continue

            input_str = ""
            if not self.is_operator_supported(op_type) and description_data:
                input_str = self._extract_types_from_description(op_type, description_data, None)

            ops.append(self._build_op_entry(op_type, input_str, metrics, metrics["count"]))

    def _build_rows_for_task(self, data):
        """
        构建任务的 Excel 行数据

        参数说明:
        :param data: dict，任务数据
        :return: list，Excel 行列表

        构建逻辑:
        1. 获取作业 ID、任务 ID 和状态
        2. 获取算子列表和函数列表
        3. 按最大长度循环，将算子和函数信息合并到同一行
        4. 创建空行作为占位符

        返回结构:
        [
            {
                ExcelColumns.JOB_ID: str,
                ExcelColumns.TASK_ID: str,
                ExcelColumns.STATUS: str,
                ExcelColumns.OPERATOR_NAME: str,
                ExcelColumns.IS_SUPPORTED: str,
                ...其他字段
            }
        ]
        """
        job_id, task_id, status = data["jobid"], data["taskid"], data["status"]
        ops, func_list = data["ops"], data["func_list"]
        if not ops and not func_list:
            # ★ 修改点：任务指标获取失败时（如 VERTEX_METRICS_FAILED），生成空行保留 jobID、taskID 和状态
            if status != "SUCCESS":
                return [self._create_empty_row(job_id, task_id, status)]
            return []
        rows, max_len = [], max(len(ops), len(func_list))
        for i in range(max_len):
            row = self._create_empty_row(job_id, task_id, status)
            if i < len(ops):
                op = ops[i]
                row.update(self._create_row(job_id, task_id, status, op=op))
            if i < len(func_list):
                func = func_list[i]
                row.update({
                    ExcelColumns.FUNC_NAME: func["func_name"],
                    ExcelColumns.FUNC_INPUT: ",".join(func["input"]) if func["input"] else "",
                    ExcelColumns.NESTED_CONTENT: func.get("nested_content", ""),
                    ExcelColumns.FUNC_FREQUENCY: str(func["times"]),
                })
            rows.append(row)
        return rows

    def _get_original_type_from_schema(self, field_expr, input_schema):
        """从 input_schema 中查找字段的原始类型（保留 ROW 等复杂类型的完整定义）"""
        if not input_schema or not field_expr:
            return None

        field_name = field_expr.strip().lower()
        # 去除可能的 [index] 后缀
        field_name = re.sub(r'\[\d+\]$', '', field_name)

        # 先从 table_schema 中查找原始类型
        if field_name in self.type_resolver.column_type:
            col_name = field_name
        else:
            col_name = None

        if col_name:
            for table_name, columns in self.type_resolver.table_schema.items():
                for col_info in columns:
                    if col_info["field_name"].lower() == col_name:
                        original = col_info.get("original_type", "")
                        if original and original.upper().startswith("ROW"):
                            return original

        # 从 input_schema 中查找
        for field in input_schema:
            if field.get("field_name", "").lower() == field_name:
                return field.get("original_type")

        return None

    def _extract_types_from_select(self, select_str, input_schema):
        types = []
        items = self.type_resolver._split_select_items(select_str)
        for item in items:
            item = item.strip()
            if not item:
                continue
            original_expr, alias_name = self.type_resolver._split_alias_from_expr(item)
            field_type = self.type_resolver.resolve_text_expr_type(original_expr, input_schema, 0)
            if field_type != "unknown":
                # ROW 类型展开嵌套字段类型
                original_type = self._get_original_type_from_schema(original_expr, input_schema)
                if original_type and original_type.upper().startswith("ROW"):
                    expanded = TypeNormalizer.expand_row_type(original_type)
                    types.extend(expanded)
                else:
                    types.append(field_type)
            elif input_schema:
                types.append("unknown")
            else:
                types.append("unknown")
        return types

    def _extract_types_from_field(self, field_str, input_schema):
        types = []
        fields = field_str.split(",")
        for field in fields:
            field = field.strip()
            if not field:
                continue
            if input_schema:
                field_type = self.type_resolver.resolve_expression_type(field, input_schema)
                if field_type == "ROW":
                    # ROW 类型展开嵌套字段类型
                    original_type = self._get_original_type_from_schema(field, input_schema)
                    if original_type and original_type.upper().startswith("ROW"):
                        expanded = TypeNormalizer.expand_row_type(original_type)
                        types.extend(expanded)
                    else:
                        types.append(field_type if field_type else "unknown")
                else:
                    types.append(field_type if field_type else "unknown")
            else:
                types.append("unknown")
        return types

    def _extract_types_from_description(self, op_type, description_data, input_schema):
        for desc in description_data:
            # ★ 修改点：处理 JSON 格式描述
            if isinstance(desc, dict):
                input_types = desc.get("inputTypes", [])
                if input_types:
                    return ",".join(TypeNormalizer.normalize_type(t) for t in input_types if t)
                output_types = desc.get("outputTypes", [])
                if output_types:
                    return ",".join(TypeNormalizer.normalize_type(t) for t in output_types if t)
            # 处理字符串格式描述
            if isinstance(desc, str):
                select_match = re.search(r'select=\[(.*?)\]', desc, re.I)
                if select_match:
                    select_content = select_match.group(1)
                    types = self._extract_types_from_select(select_content, input_schema)
                    if types:
                        return ",".join(types)
                field_match = re.search(r'field=\[(.*?)\]', desc, re.I)
                if field_match:
                    field_content = field_match.group(1)
                    types = self._extract_types_from_field(field_content, input_schema)
                    if types:
                        return ",".join(types)
        return ""
