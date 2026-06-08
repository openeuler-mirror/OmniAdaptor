"""
   Flink 类型解析模块

   Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
   You can use this software according to the terms and conditions of the Mulan PSL v2.
   You may obtain a copy of Mulan PSL v2 at:
            http://license.coscl.org.cn/MulanPSL2
   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
   See the Mulan PSL v2 for more details.

模块功能说明:
    本模块是 Flink 类型系统的核心解析引擎，负责以下功能:
    1. 解析和推断表达式的返回类型
    2. 处理 JSON 和文本两种格式的表达式
    3. 支持字段引用、字面量、函数调用等表达式类型
    4. 提供函数返回类型字典查询
    5. 解析和构建算子的输入输出 schema

设计理念:
    - 双格式支持：同时支持 JSON 格式和文本格式的表达式解析
    - 递归解析：通过深度控制的递归处理嵌套表达式
    - 类型标准化：与 TypeNormalizer 配合确保类型一致性
    - 灵活的字段查找：支持表名限定和嵌套字段路径解析
    - 函数类型推断：基于函数返回类型字典和参数类型推断

表达式类型支持:
    - FIELD_REFERENCE: 字段引用
    - LITERAL: 字面量值
    - FUNCTION: 函数调用
    - BINARY: 二元运算
    - UNARY: 一元运算
    - CASE/SWITCH: 条件表达式
    - COALESCE: 空值合并
    - IS_NOT_NULL, IN, BETWEEN: 条件判断表达式

依赖模块:
    - json: JSON 数据解析
    - os: 文件路径操作
    - re: 正则表达式处理
    - TableSchemaReader: 表结构读取器，用于解析嵌套字段
    - TypeNormalizer: 类型标准化工具
    - logger: 日志记录工具

使用场景:
    - Flink SQL 表达式类型推断
    - 算子输出 schema 构建
    - 字段类型验证和查找
    - 函数返回类型解析
"""

# 导入标准库模块
import json
import os
import re

# 导入项目内部模块
from omnihelper.flink.schema.table_schema_reader import TableSchemaReader
from omnihelper.flink.schema.type_normalizer import TypeNormalizer
from omnihelper.util.log import logger

MAX_DEPTH = 10

# 未知类型标识
UNKNOWN = "unknown"

# 嵌套函数标识
NESTED_FUNCTION = "NESTED_FUNCTION"

# OMNI 类型 ID 到类型名称的映射表
# 该映射用于将 Flink 内部类型 ID 转换为可读的类型字符串
OMNI_TYPE_ID_MAP = {
    1: "INT",  # 整型
    2: "BIGINT",  # 长整型
    3: "DOUBLE",  # 双精度浮点型
    4: "FLOAT",  # 单精度浮点型
    5: "BOOLEAN",  # 布尔型
    6: "TINYINT",  # 微型整型
    7: "SMALLINT",  # 小型整型
    8: "DATE",  # 日期类型
    9: "TIME",  # 时间类型
    10: "TIMESTAMP",  # 时间戳类型
    11: "TIMESTAMP_LTZ",  # 本地时区时间戳
    12: "DECIMAL",  # 高精度小数
    13: "BINARY",  # 二进制类型
    14: "CHAR",  # 定长字符
    15: "VARCHAR",  # 变长字符
    16: "ARRAY",  # 数组类型
    17: "MAP",  # 映射类型
    18: "ROW",  # 行类型（嵌套结构）
    19: "VARBINARY",  # 变长二进制
    20: "MULTISET",  # 多重集合类型
}

# 字面量类型匹配模式列表
# 用于根据字面量字符串推断类型
TYPE_PATTERNS = [
    (re.compile(r"^true$|^false$", re.I), "BOOLEAN"),
    (re.compile(r"^NULL$", re.I), "NULL"),
    (re.compile(r"^-?\d+$"), "BIGINT"),
    (re.compile(r"^-?\d+[Ll]$"), "BIGINT"),
    (re.compile(r"^-?\d+\.\d+$"), "DECIMAL128"),
    (re.compile(r"^\d{4}-\d{2}-\d{2}$"), "DATE"),
    (re.compile(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}"), "TIMESTAMP"),
    (re.compile(r"^INTERVAL\s+", re.I), "INTERVAL"),
    (re.compile(r"^Sarg\[.*\]$", re.I), "VARCHAR")
]


class FlinkTypeResolver:
    """
    Flink 类型解析器

    核心职责:
    1. 根据上下文和表达式推断类型
    2. 加载和维护函数返回类型字典
    3. 解析 JSON 格式的表达式结构
    4. 处理嵌套类型的字段访问
    5. 构建算子的输出 schema

    设计特点:
    - 双格式支持：同时处理 JSON 和文本格式的表达式
    - 递归解析：通过深度控制避免无限递归
    - 类型标准化：确保类型名称的一致性
    - 灵活查找：支持多种字段查找方式

    成员变量说明:
    - table_schema: dict，表结构字典，格式为 {表名: [字段信息列表]}
      每个字段信息包含 field_name, field_type, original_type, nested_fields
    - column_type: dict，字段名→类型 映射（小写），用于快速查找
    - table_column_type: dict，表名.字段名→类型 映射（小写），用于精确查找
    - alias_map: dict，别名→原始表达式 映射，支持别名解析
    - return_type_dict: dict，函数名→返回类型配置 映射，从 JSON 文件加载

    使用示例:
    ```python
    # 创建类型解析器
    resolver = FlinkTypeResolver(table_schema, column_type, table_column_type)

    # 解析表达式类型
    expr_type = resolver.resolve_expression_type(expr, input_schema)

    # 构建算子输出 schema
    output_schema = resolver.build_output_schema(op_type, description_data, input_schema)
    ```
    """

    def __init__(self, table_schema=None, column_type=None, table_column_type=None):
        """
        初始化类型解析器

        参数说明:
        :param table_schema: dict，表结构字典，格式为 {表名: [字段信息列表]}
        :param column_type: dict，字段名→类型 映射（小写）
        :param table_column_type: dict，表名.字段名→类型 映射（小写）

        初始化流程:
        1. 初始化表结构和字段类型映射（为空字典时使用空默认值）
        2. 初始化别名映射为空字典
        3. 初始化函数返回类型字典为空字典
        4. 调用 _load_return_type_dict 加载函数返回类型配置

        成员变量初始化说明:
        - self.table_schema: 存储表结构信息，用于解析嵌套字段
        - self.column_type: 存储字段名到类型的映射，支持快速查找
        - self.table_column_type: 存储表名.字段名到类型的映射，支持精确查找
        - self.alias_map: 存储别名到原始表达式的映射，用于解析别名引用
        - self.return_type_dict: 存储函数名到返回类型配置的映射
        """
        # 表结构字典，用于解析嵌套字段
        self.table_schema = table_schema or {}

        # 字段名→类型 映射（小写），用于快速查找
        self.column_type = column_type or {}

        # 表名.字段名→类型 映射（小写），用于精确查找
        self.table_column_type = table_column_type or {}

        # 别名→原始表达式 映射，支持别名解析
        self.alias_map = {}

        # 函数名→返回类型配置 映射
        self.return_type_dict = {}

        # 加载函数返回类型字典
        self._load_return_type_dict()

    @staticmethod
    def _normalize_return_type(return_type):
        """
        标准化返回类型

        参数说明:
        :param return_type: 返回类型，可以是 int（类型ID）、str（类型名称或类型ID字符串）或 None
        :return: str 或 None，标准化后的类型字符串；无法识别时返回 UNKNOWN；None 输入返回 None

        转换规则:
        1. None 输入直接返回 None
        2. int 类型：查找 OMNI_TYPE_ID_MAP 映射表
        3. 数字字符串：转换为 int 后查找 OMNI_TYPE_ID_MAP
        4. 其他字符串：调用 TypeNormalizer.normalize_type 进行标准化

        设计考量:
        - 支持多种输入格式（类型ID、类型名称），统一转换为标准化类型字符串
        - 与 OMNI_TYPE_ID_MAP 配合，支持 Flink 内部类型ID的解析
        - 委托 TypeNormalizer 处理类型别名（如 STRING→VARCHAR）

        示例:
        - _normalize_return_type(1) → "INT"
        - _normalize_return_type("15") → "VARCHAR"
        - _normalize_return_type("STRING") → "VARCHAR"
        - _normalize_return_type("UNKNOWN_TYPE") → "unknown"
        """
        # None 输入直接返回 None
        if return_type is None:
            return None

        # int 类型：查找 OMNI_TYPE_ID_MAP 映射表
        if isinstance(return_type, int):
            return OMNI_TYPE_ID_MAP.get(return_type, UNKNOWN)

        # 转换为字符串并去除首尾空白
        type_str = str(return_type).strip()

        # 数字字符串：转换为 int 后查找映射表
        if type_str.isdigit():
            return OMNI_TYPE_ID_MAP.get(int(type_str), UNKNOWN)

        # 其他字符串：调用 TypeNormalizer 进行标准化
        return TypeNormalizer.normalize_type(type_str)

    def _load_return_type_dict(self):
        """
        加载函数返回类型字典

        功能说明:
        从项目资源文件中加载 Flink 函数的返回类型配置，用于函数调用表达式的类型推断

        文件路径:
        resources/flink_function_return_type.json（相对于模块所在目录的上级目录）

        文件格式:
        [
            {"func_name": "upper", "return_type": "VARCHAR", "need_param_type": false},
            {"func_name": "cast", "return_type": "RESULT_TYPE", "need_param_type": true},
            ...
        ]

        返回类型配置字段说明:
        - func_name: 函数名称（小写存储）
        - return_type: 返回类型，可能的值：
          - 具体类型如 "VARCHAR", "INT" 等
          - "ARGUMENT_TYPE": 返回第一个参数的类型
          - "RESULT_TYPE": 需要根据参数计算（如 cast, if 函数）
        - need_param_type: 是否需要参数类型来确定返回类型

        异常处理:
        - 文件不存在或解析失败时记录警告日志，但不抛出异常
        - 继续使用空的 return_type_dict，后续函数解析将基于参数推断

        加载流程:
        1. 构建资源文件路径（相对于当前模块的上级目录）
        2. 打开并读取 JSON 文件
        3. 转换为字典格式，键为函数名（小写），值为完整配置项
        4. 记录加载成功的条目数量
        """
        try:
            # 构建资源文件路径：向上三级目录找到 resources 文件夹
            dict_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "resources",
                "flink_function_return_type.json",
            )

            # 读取并解析 JSON 文件
            with open(dict_path, "r", encoding="utf-8") as f:
                # 转换为字典格式，函数名统一转为小写作为键
                self.return_type_dict = {
                    item["func_name"].lower(): item for item in json.load(f)
                }

            # 记录加载成功日志
            logger.info(f"Loaded {len(self.return_type_dict)} function return type entries")

        except Exception as e:
            # 文件不存在或解析失败时记录警告，继续使用空字典
            logger.warning(f"Failed to load flink_function_return_type.json: {e}")

    def update_column_type(self, column_type, table_column_type=None):
        """
        更新字段类型映射

        参数说明:
        :param column_type: dict，新增的字段名→类型映射（键为小写）
        :param table_column_type: dict，新增的表名.字段名→类型映射（键为小写）

        功能说明:
        将新的字段类型映射合并到现有的映射中，支持增量更新表结构信息

        合并规则:
        - column_type: 使用 dict.update() 方法合并，重复键会被覆盖
        - table_column_type: 同上

        使用场景:
        - 在运行时动态添加新的表结构信息
        - 合并多个表的字段类型映射
        """
        # 合并字段名→类型映射
        if column_type:
            self.column_type.update(column_type)

        # 合并表名.字段名→类型映射
        if table_column_type:
            self.table_column_type.update(table_column_type)

    def update_alias_map(self, alias_map):
        """
        更新别名映射

        参数说明:
        :param alias_map: dict，新增的别名→原始表达式映射

        功能说明:
        将新的别名映射合并到现有的映射中，支持增量更新别名信息

        使用场景:
        - 从算子描述中提取别名后更新
        - 合并多个算子的别名信息

        注意事项:
        - 重复的别名会被新值覆盖
        - 别名查找时会自动处理数组索引后缀（如 alias[0]）
        """
        if alias_map:
            self.alias_map.update(alias_map)

    def resolve_field_type(self, field_name, table_name=None):
        """
        解析字段类型

        参数说明:
        :param field_name: str，字段名，支持简单字段名或点分隔的嵌套路径（如 "address.city"）
        :param table_name: str，表名（可选），用于限定字段查找范围
        :return: str，字段类型；无法解析时返回 UNKNOWN

        查找优先级:
        1. 如果提供了表名，优先查找 table_name.field_name
        2. 查找简单字段名（小写）
        3. 如果字段名包含点，尝试解析为嵌套字段路径
        4. 返回 UNKNOWN

        使用场景:
        - 在表达式解析时查找字段类型
        - 支持带表名限定的字段引用（如 table.column）
        - 支持嵌套字段访问（如 struct.field）

        示例:
        - resolve_field_type("name") → "VARCHAR"
        - resolve_field_type("user.name") → "VARCHAR"
        - resolve_field_type("address.city", "users") → "VARCHAR"
        """
        # 优先级1：表名限定查找
        if table_name:
            key = f"{table_name}.{field_name}".lower()
            if key in self.table_column_type:
                return self.table_column_type[key]

        # 优先级2：简单字段名查找（小写）
        name_lower = field_name.lower()
        if name_lower in self.column_type:
            return self.column_type[name_lower]

        # 优先级3：嵌套字段路径解析
        if "." in field_name:
            nested_type = self._resolve_nested_field_path(field_name, table_name)
            if nested_type and nested_type != UNKNOWN:
                return nested_type

        # 无法解析，返回 UNKNOWN
        return UNKNOWN

    def _resolve_nested_field_path(self, dotted_path, table_name=None):
        """
        解析嵌套字段路径

        参数说明:
        :param dotted_path: str，点分隔的字段路径，如 "address.city"、"user.info.name"
        :param table_name: str，表名（可选），用于限定查找范围
        :return: str，嵌套字段的类型；无法解析时返回 UNKNOWN

        解析流程:
        1. 将路径按点分割为多个部分
        2. 第一部分作为顶层字段名，剩余部分作为嵌套路径
        3. 如果指定了表名，在该表中查找顶层字段
        4. 如果未指定表名或在指定表中未找到，遍历所有表查找
        5. 调用 TableSchemaReader.resolve_nested_field_type 递归解析嵌套路径

        设计考量:
        - 支持多层嵌套字段访问（如 a.b.c.d）
        - 优先按表名查找，提高解析效率
        - 支持跨表查找（未指定表名时）

        示例:
        假设有表 users，包含字段 address (ROW<city VARCHAR, street VARCHAR>)
        - _resolve_nested_field_path("address.city", "users") → "VARCHAR"
        - _resolve_nested_field_path("address.street") → "VARCHAR"
        """
        # 按点分割路径
        parts = dotted_path.split(".")

        # 至少需要两个部分（顶层字段 + 嵌套字段）
        if len(parts) < 2:
            return UNKNOWN

        # 提取顶层字段名和嵌套路径
        top_level_name = parts[0].lower()
        nested_path = parts[1:]

        # 优先级1：在指定表中查找
        if table_name and table_name in self.table_schema:
            for col_info in self.table_schema[table_name]:
                if col_info["field_name"].lower() == top_level_name:
                    return TableSchemaReader.resolve_nested_field_type(col_info, nested_path)

        # 优先级2：遍历所有表查找
        for tbl_name, columns in self.table_schema.items():
            for col_info in columns:
                if col_info["field_name"].lower() == top_level_name:
                    return TableSchemaReader.resolve_nested_field_type(col_info, nested_path)

        # 未找到
        return UNKNOWN

    def _resolve_field_path_from_schema(self, col_info, field_path):
        """
        从 schema 解析嵌套字段类型

        参数说明:
        :param col_info: dict，字段信息字典，包含 field_type 和 nested_fields
        :param field_path: list，字段路径列表，如 ["address", "city"]
        :return: str，嵌套字段的类型；无法解析时返回 UNKNOWN

        解析流程:
        1. 参数校验：路径为空或不是列表时返回 UNKNOWN
        2. 初始化当前层级的嵌套字段列表
        3. 按路径逐级遍历：
           a. 在当前层级查找匹配的字段名
           b. 找到后更新当前层级为该字段的嵌套字段
           c. 更新结果类型为该字段的类型
           d. 未找到则返回 UNKNOWN
        4. 遍历完成后返回最终类型

        使用场景:
        - 在已知字段信息的情况下，递归查找嵌套字段类型

        示例:
        col_info = {
            "field_type": "ROW",
            "nested_fields": [{"field_name": "address", "field_type": "ROW",
                               "nested_fields": [{"field_name": "city", "field_type": "VARCHAR"}]}]
        }
        _resolve_field_path_from_schema(col_info, ["address", "city"]) → "VARCHAR"
        """
        # 参数校验：路径为空或不是列表
        if not field_path or not isinstance(field_path, list):
            return UNKNOWN

        # 初始化当前层级的嵌套字段列表
        nested = col_info.get("nested_fields", [])
        result_type = UNKNOWN

        # 按路径逐级查找
        for part in field_path:
            found = False
            for field in nested:
                if field.get("field_name") == part:
                    # 进入下一层嵌套
                    nested = field.get("nested_fields", [])
                    result_type = field.get("field_type", UNKNOWN)
                    found = True
                    break
            # 某一级未找到，返回 UNKNOWN
            if not found:
                return UNKNOWN

        return result_type

    def resolve_indexed_field_type(self, index, input_schema):
        """
        按索引解析字段类型

        参数说明:
        :param index: int，字段在输入 schema 中的索引位置（从0开始）
        :param input_schema: list，输入 schema，每个元素包含 field_type 字段
        :return: str，字段类型；索引无效时返回 UNKNOWN

        边界检查:
        - input_schema 为 None 或空列表
        - index < 0（负数索引）
        - index >= len(input_schema)（索引越界）

        使用场景:
        - 在 JSON 表达式中，字段引用可能使用索引而非字段名
        - 从输入 schema 中按位置获取字段类型

        示例:
        input_schema = [{"field_name": "id", "field_type": "INT"}, {"field_name": "name", "field_type": "VARCHAR"}]
        resolve_indexed_field_type(0, input_schema) → "INT"
        resolve_indexed_field_type(1, input_schema) → "VARCHAR"
        resolve_indexed_field_type(2, input_schema) → "unknown"
        """
        # 边界检查：schema 为空或索引无效
        if not input_schema or index < 0 or index >= len(input_schema):
            return UNKNOWN

        # 返回指定索引位置的字段类型
        return input_schema[index].get("field_type", UNKNOWN)

    def resolve_literal_type(self, value):
        """
        解析字面量类型

        参数说明:
        :param value: 任意类型，字面量值
        :return: str，推断的类型字符串；无法推断时返回 UNKNOWN

        类型推断规则:
        1. None → "NULL"
        2. bool → "BOOLEAN"
        3. int → "INT"
        4. float → "DOUBLE"
        5. str:
           - 空字符串 → UNKNOWN
           - 引号包裹的字符串 → "VARCHAR"
           - 匹配 TYPE_PATTERNS 中的正则 → 对应类型
           - 其他 → UNKNOWN
        6. 其他类型 → UNKNOWN

        设计考量:
        - 支持 Python 原生类型的直接推断
        - 支持字符串形式的字面量解析（如 "123", "2024-01-01"）
        - 使用正则模式匹配日期、时间戳等特殊格式

        示例:
        - resolve_literal_type(None) → "NULL"
        - resolve_literal_type(True) → "BOOLEAN"
        - resolve_literal_type(42) → "INT"
        - resolve_literal_type(3.14) → "DOUBLE"
        - resolve_literal_type("'hello'") → "VARCHAR"
        - resolve_literal_type("2024-01-01") → "DATE"
        - resolve_literal_type("2024-01-01 12:00:00") → "TIMESTAMP"
        """
        # None 值
        if value is None:
            return "NULL"

        # 布尔值
        if isinstance(value, bool):
            return "BOOLEAN"

        # 整数
        if isinstance(value, int):
            return "BIGINT"

        # 浮点数
        if isinstance(value, float):
            return "DECIMAL128"

        # 字符串类型的字面量
        if isinstance(value, str):
            value_str = value.strip()

            # 空字符串无法推断类型
            if not value_str:
                return UNKNOWN

            # 引号包裹的字符串 → VARCHAR
            if (value_str.startswith("'") and value_str.endswith("'")) or \
                    (value_str.startswith('"') and value_str.endswith('"')):
                return "VARCHAR"

            # 特殊处理 Sarg[...] 类型 → VARCHAR
            if value_str.startswith('Sarg[') or value_str.startswith('SARG['):
                return "VARCHAR"

            # 使用正则模式匹配特殊格式
            for pattern, match_type in TYPE_PATTERNS:
                if pattern.match(value_str):
                    return match_type

            # 无法匹配的字符串
            return UNKNOWN

        # 其他类型无法推断
        return UNKNOWN

    def resolve_expression_type(self, expr, input_schema=None, depth=0):
        """
        解析表达式类型

        参数说明:
        :param expr: dict 或 str，表达式，可以是 JSON 格式的字典或文本格式的字符串
        :param input_schema: list，输入 schema，用于解析字段引用（可选）
        :param depth: int，当前递归深度，用于防止无限递归（默认0）
        :return: str，表达式的返回类型；无法解析或递归过深时返回 UNKNOWN

        核心设计:
        - 递归解析：支持嵌套表达式的递归处理
        - 深度控制：通过 depth 参数限制递归深度，防止栈溢出
        - 双格式支持：自动识别 JSON 和文本格式的表达式

        处理流程:
        1. 深度检查：超过 MAX_DEPTH 时返回 UNKNOWN
        2. 空值检查：表达式为空时返回 UNKNOWN
        3. 类型分发：根据表达式类型调用不同的解析方法
           - dict → _resolve_json_expr_type
           - str → _resolve_text_expr_type
        4. 返回解析结果

        使用场景:
        - 解析 Flink SQL 表达式的返回类型
        - 支持 JSON 格式（来自 Flink REST API）和文本格式（来自日志）

        示例:
        - resolve_expression_type({"exprType": "LITERAL", "value": 42}) → "INT"
        - resolve_expression_type("name") → "VARCHAR"（假设 name 字段存在）
        - resolve_expression_type("upper(name)") → "VARCHAR"
        """
        # 递归深度检查：防止无限递归
        if depth > MAX_DEPTH:
            return UNKNOWN

        # 空值检查
        if not expr:
            return UNKNOWN

        # 根据表达式类型分发解析
        if isinstance(expr, dict):
            return self._resolve_json_expr_type(expr, input_schema, depth)

        if isinstance(expr, str):
            return self.resolve_text_expr_type(expr, input_schema, depth)

        # 未知类型
        return UNKNOWN

    def _resolve_json_expr_type(self, expr, input_schema, depth):
        """
        解析 JSON 格式表达式的类型

        参数说明:
        :param expr: dict，JSON 格式的表达式对象
        :param input_schema: list，输入 schema
        :param depth: int，当前递归深度
        :return: str，表达式的返回类型

        支持的表达式类型:
        - FIELD_REFERENCE: 字段引用
        - LITERAL: 字面量值
        - FUNCTION: 函数调用
        - BINARY: 二元运算（如 +, -, *, /, =, > 等）
        - UNARY: 一元运算（如 NOT, - 等）
        - CASE/SWITCH: 条件表达式
        - COALESCE: 空值合并
        - IS_NOT_NULL: 非空判断
        - MULTIPLE_AND_OR: 逻辑组合
        - IN/BETWEEN: 范围判断
        - REGEX_EXTRACT/SPLIT_INDEX: 字符串操作
        - PROCTIME: 处理时间

        解析策略:
        1. 优先使用表达式中直接提供的 returnType 或 dataType
        2. 无法直接获取时，根据表达式类型进行推断
        3. 对于嵌套表达式，递归调用 resolve_expression_type

        设计考量:
        - JSON 格式来自 Flink REST API，结构较为规范
        - 优先信任 API 返回的类型信息
        - 对于缺失类型信息的情况，提供合理的推断逻辑
        """
        # 获取表达式类型
        expr_type = expr.get("exprType", "")

        # ===== FIELD_REFERENCE: 字段引用 =====
        if expr_type == "FIELD_REFERENCE":
            # 获取字段索引和类型信息
            col_val = expr.get("colVal", -1)
            data_type = expr.get("dataType")

            # 优先使用直接提供的类型
            if data_type:
                return self._normalize_return_type(data_type)

            # 获取嵌套字段路径
            field_path = expr.get("fieldPath")

            # 如果有嵌套路径且索引有效，尝试从 schema 解析
            if field_path and input_schema and 0 <= col_val < len(input_schema):
                col_info = input_schema[col_val]
                resolved = self._resolve_field_path_from_schema(col_info, field_path)
                if resolved and resolved != UNKNOWN:
                    return resolved

            # 如果索引有效，尝试按索引获取类型
            if input_schema and 0 <= col_val < len(input_schema):
                base_type = self.resolve_indexed_field_type(col_val, input_schema)
                # 如果不是 ROW 类型，直接返回
                if base_type != "ROW":
                    return base_type
                # ROW 类型需要进一步解析嵌套路径
                col_info = input_schema[col_val]
                col_name = col_info.get("field_name", "")
                if col_name:
                    nested_path = field_path if field_path else []
                    if nested_path:
                        resolved = self._resolve_nested_field_path(
                            ".".join([col_name] + nested_path)
                        )
                        if resolved and resolved != UNKNOWN:
                            return resolved

            return UNKNOWN

        # ===== LITERAL: 字面量 =====
        if expr_type == "LITERAL":
            data_type = expr.get("dataType")
            if data_type:
                return self._normalize_return_type(data_type)
            if expr.get("isNull", False):
                return "NULL"
            return self.resolve_literal_type(expr.get("value"))

        # ===== BINARY: 二元运算 =====
        if expr_type == "BINARY":
            return_type = expr.get("returnType")
            if return_type is not None:
                return self._normalize_return_type(return_type)
            return UNKNOWN

        # ===== UNARY: 一元运算 =====
        if expr_type == "UNARY":
            return_type = expr.get("returnType")
            if return_type is not None:
                return self._normalize_return_type(return_type)
            # 递归解析内部表达式
            inner = expr.get("expr")
            if inner:
                return self.resolve_expression_type(inner, input_schema, depth + 1)
            return UNKNOWN

        # ===== FUNCTION: 函数调用 =====
        if expr_type == "FUNCTION":
            return self._resolve_function_expr_type(expr, input_schema, depth)

        # ===== CASE/SWITCH: 条件表达式 =====
        if expr_type in ("SWITCH", "CASE"):
            return self._resolve_case_expr_type(expr, input_schema, depth)

        # ===== COALESCE: 空值合并 =====
        if expr_type == "COALESCE":
            return self._resolve_coalesce_expr_type(expr, input_schema, depth)

        # ===== IS_NOT_NULL: 非空判断 =====
        if expr_type == "IS_NOT_NULL":
            return "BOOLEAN"

        # ===== MULTIPLE_AND_OR: 逻辑组合 =====
        if expr_type == "MULTIPLE_AND_OR":
            return "BOOLEAN"

        # ===== IN/BETWEEN: 范围判断 =====
        if expr_type in ("IN", "BETWEEN"):
            return "BOOLEAN"

        # ===== REGEX_EXTRACT/SPLIT_INDEX: 字符串操作 =====
        if expr_type in ("REGEX_EXTRACT", "SPLIT_INDEX"):
            return_type = expr.get("returnType")
            if return_type is not None:
                return self._normalize_return_type(return_type)
            return "VARCHAR"

        # ===== PROCTIME: 处理时间 =====
        if expr_type == "PROCTIME":
            return "TIMESTAMP"

        # 兜底：尝试从 returnType 字段获取类型
        return_type = expr.get("returnType")
        if return_type is not None:
            return self._normalize_return_type(return_type)

        return UNKNOWN

    def _resolve_function_expr_type(self, expr, input_schema, depth):
        """
        解析函数表达式的返回类型

        参数说明:
        :param expr: dict，JSON 格式的函数表达式对象
        :param input_schema: list，输入 schema
        :param depth: int，当前递归深度
        :return: str，函数的返回类型；无法解析时返回 UNKNOWN

        支持的返回类型规则:
        - 具体类型（如 VARCHAR, INT）：直接返回配置的类型
        - ARGUMENT_TYPE：返回参数类型（取第一个非 UNKNOWN 参数的类型，或找公共类型）
        - RESULT_TYPE：需要根据具体参数计算（如 cast, if 函数）

        解析流程:
        1. 优先使用表达式中直接提供的 returnType
        2. 在函数返回类型字典中查找函数定义
        3. 根据函数定义的规则进行类型推断
           - 不需要参数类型：直接返回配置的类型
           - ARGUMENT_TYPE：解析参数类型并返回
           - RESULT_TYPE：调用 _resolve_result_type 处理

        设计考量:
        - 优先信任表达式中直接提供的类型信息
        - 使用函数返回类型字典作为 fallback
        - 支持灵活的类型推断规则

        示例:
        - upper(name) → VARCHAR（配置中指定）
        - cast(age AS STRING) → VARCHAR（根据第二个参数推断）
        - if(cond, val1, val2) → val1 和 val2 的公共类型
        """
        # 获取函数名称（小写）和直接提供的返回类型
        func_name = expr.get("function_name", "").lower()
        return_type = expr.get("returnType")

        # 优先使用直接提供的返回类型
        if return_type is not None:
            return self._normalize_return_type(return_type)

        # 在函数返回类型字典中查找
        dict_entry = self.return_type_dict.get(func_name)
        if not dict_entry:
            # 字典中没有定义，尝试推断第一个参数的类型
            arguments = expr.get("arguments", [])
            if arguments:
                first_arg_type = self.resolve_expression_type(
                    arguments[0], input_schema, depth + 1
                )
                if first_arg_type != UNKNOWN:
                    return first_arg_type
            return UNKNOWN

        # 不需要参数类型的函数，直接返回配置的类型
        if not dict_entry.get("need_param_type", False):
            ret = dict_entry.get("return_type", UNKNOWN)
            return ret if ret != UNKNOWN else UNKNOWN

        # 需要根据参数类型推断
        rule = dict_entry.get("return_type", "")
        arguments = expr.get("arguments", [])

        # ===== ARGUMENT_TYPE: 返回参数类型 =====
        if rule == "ARGUMENT_TYPE":
            # 获取所有参数的类型
            arg_types = [
                self.resolve_expression_type(arg, input_schema, depth + 1)
                for arg in arguments
            ]
            # 过滤 UNKNOWN 类型
            non_unknown = [t for t in arg_types if t != UNKNOWN]

            # 只有一个有效类型，直接返回
            if len(non_unknown) == 1:
                return non_unknown[0]

            # 多个有效类型，找公共类型
            if len(non_unknown) > 1:
                result = TypeNormalizer.find_common_type_multi(non_unknown)
                return result if result else UNKNOWN

            return UNKNOWN

        # ===== RESULT_TYPE: 需要根据参数计算 =====
        if rule == "RESULT_TYPE":
            return self._resolve_result_type(func_name, arguments, input_schema, depth)

        return UNKNOWN

    def _resolve_result_type(self, func_name, arguments, input_schema, depth):
        """
        解析 RESULT_TYPE 规则的函数返回类型

        参数说明:
        :param func_name: str，函数名称（小写）
        :param arguments: list，函数参数列表
        :param input_schema: list，输入 schema
        :param depth: int，当前递归深度
        :return: str，函数的返回类型；无法解析时返回 UNKNOWN

        支持的特殊函数:
        - cast/try_cast: 返回第二个参数指定的类型（目标类型）
        - if: 返回第二和第三个参数的公共类型

        实现逻辑:
        - cast/try_cast: 解析第二个参数作为目标类型
        - if: 解析第二和第三个参数（跳过第一个条件参数），找到它们的公共类型

        示例:
        - cast(age AS STRING) → VARCHAR
        - if(active, name, '-') → VARCHAR（name 和 '-' 的公共类型）
        """
        # ===== cast/try_cast: 类型转换函数 =====
        if func_name in ("cast", "try_cast") and len(arguments) >= 2:
            # 第二个参数指定目标类型
            target_type = self.resolve_expression_type(
                arguments[1], input_schema, depth + 1
            )
            return target_type if target_type != UNKNOWN else UNKNOWN

        # ===== if: 条件表达式函数 =====
        if func_name == "if" and len(arguments) >= 3:
            # 解析第二和第三个参数的类型（跳过第一个条件参数）
            arg_types = [
                self.resolve_expression_type(arguments[i], input_schema, depth + 1)
                for i in range(1, min(3, len(arguments)))
            ]
            # 过滤掉 UNKNOWN、BOOLEAN（条件）和 NULL 类型
            non_unknown = [t for t in arg_types if t not in (UNKNOWN, "BOOLEAN", "NULL")]
            if non_unknown:
                # 找到公共类型
                result = TypeNormalizer.find_common_type_multi(non_unknown)
                return result if result else UNKNOWN
            return UNKNOWN

        return UNKNOWN

    def _resolve_case_expr_type(self, expr, input_schema, depth):
        """
        解析 CASE 表达式的返回类型

        参数说明:
        :param expr: dict，JSON 格式的 CASE 表达式对象
        :param input_schema: list，输入 schema
        :param depth: int，当前递归深度
        :return: str，CASE 表达式的返回类型；无法解析时返回 UNKNOWN

        CASE 表达式格式:
        {
            "Case1": result_expr1,    # 第一个分支的结果表达式
            "Case2": result_expr2,    # 第二个分支的结果表达式
            ...
            "else": else_value,       # else 分支的表达式
            "returnType": "VARCHAR"   # 可选的直接类型指定
        }

        解析逻辑:
        1. 优先使用直接提供的 returnType
        2. 收集所有 Case 分支和 else 分支的类型
        3. 过滤掉 UNKNOWN、BOOLEAN（条件）和 NULL 类型
        4. 使用 TypeNormalizer.find_common_type_multi 找到公共类型
        5. 无法找到公共类型时返回 UNKNOWN

        设计考量:
        - CASE 表达式的返回类型是所有分支的公共类型
        - 布尔类型用于条件判断，不应参与类型推断
        - NULL 类型不影响最终类型推断

        示例:
        CASE WHEN active THEN name ELSE '-' END → VARCHAR
        """
        # 优先使用直接提供的类型
        data_type = expr.get("returnType")
        if data_type:
            return self._normalize_return_type(data_type)

        # 收集所有分支的类型
        branch_types = []

        # 遍历所有 Case 分支（按数字排序）
        for key in sorted(expr.keys()):
            if key.startswith("Case") and key[4:].isdigit():
                case_expr = expr[key]
                if case_expr:
                    t = self.resolve_expression_type(case_expr, input_schema, depth + 1)
                    # 过滤掉 UNKNOWN、BOOLEAN（条件）和 NULL
                    if t not in (UNKNOWN, "BOOLEAN", "NULL"):
                        branch_types.append(t)

        # 处理 else 分支
        else_expr = expr.get("else")
        if else_expr:
            t = self.resolve_expression_type(else_expr, input_schema, depth + 1)
            if t not in (UNKNOWN, "NULL"):
                branch_types.append(t)

        # 如果有有效类型，找到公共类型
        if branch_types:
            common = TypeNormalizer.find_common_type_multi(branch_types)
            if common and common != UNKNOWN:
                return common

        # 兜底：再次尝试 returnType
        return_type = expr.get("returnType")
        if return_type is not None:
            return self._normalize_return_type(return_type)

        return UNKNOWN

    def _resolve_coalesce_expr_type(self, expr, input_schema, depth):
        """
        解析 COALESCE 表达式的返回类型

        参数说明:
        :param expr: dict，JSON 格式的 COALESCE 表达式对象
        :param input_schema: list，输入 schema
        :param depth: int，当前递归深度
        :return: str，COALESCE 表达式的返回类型；无法解析时返回 UNKNOWN

        COALESCE 表达式格式:
        {
            "value0": expr0,   # 第一个值表达式
            "value1": expr1,   # 第二个值表达式
            "value2": expr2,   # 第三个值表达式
            ...
        }

        解析逻辑:
        1. 遍历所有 valueN 字段（按数字排序）
        2. 解析每个值表达式的类型
        3. 过滤掉 UNKNOWN 和 NULL 类型
        4. 使用 TypeNormalizer.find_common_type_multi 找到公共类型

        设计考量:
        - COALESCE 返回第一个非 NULL 值，因此所有参数必须是兼容类型
        - 返回所有参数类型的公共类型

        示例:
        COALESCE(name, '-') → VARCHAR
        COALESCE(age, 0) → INT
        """
        # 收集所有值表达式的类型
        types = []

        # 遍历所有 valueN 字段（按数字排序）
        for key in sorted(expr.keys()):
            if key.startswith("value") and key[5:].isdigit():
                val_expr = expr[key]
                if val_expr:
                    t = self.resolve_expression_type(val_expr, input_schema, depth + 1)
                    # 过滤掉 UNKNOWN 和 NULL
                    if t not in (UNKNOWN, "NULL"):
                        types.append(t)

        # 如果有有效类型，找到公共类型
        if types:
            result = TypeNormalizer.find_common_type_multi(types)
            return result if result else UNKNOWN

        return UNKNOWN

    def resolve_text_expr_type(self, expr_str, input_schema, depth, visited=None):
        if not expr_str or not isinstance(expr_str, str):
            return UNKNOWN

        if depth > 20:
            return UNKNOWN

        expr_str = expr_str.strip()
        if not expr_str:
            return UNKNOWN

        # 优先级1：字符串字面量（引号包裹）
        if (expr_str.startswith("'") and expr_str.endswith("'")) or \
                (expr_str.startswith('"') and expr_str.endswith('"')):
            return "VARCHAR"

        # 优先级2：Sarg[...] 特殊处理 → VARCHAR
        if expr_str.upper().startswith('SARG[') and expr_str.endswith(']'):
            return "VARCHAR"

        # 优先级3：类型模式匹配
        for pattern, match_type in TYPE_PATTERNS:
            if pattern.match(expr_str):
                return match_type

        # 优先级4：NULL 关键字
        if expr_str.upper() == "NULL":
            return "NULL"

        # 优先级5：布尔关键字
        if expr_str.upper() in ("TRUE", "FALSE"):
            return "BOOLEAN"

        # 优先级6：DISTINCT 关键字语法糖（如 "DISTINCT bidder" 等同于 "DISTINCT(bidder)"）
        distinct_match = re.match(r'^(DISTINCT)\s+(.+)', expr_str.strip(), re.I)
        if distinct_match:
            inner_expr = distinct_match.group(2).strip()
            return self.resolve_text_expr_type(inner_expr, input_schema, depth + 1, visited=visited)

        # 优先级7：带括号的表达式（如 (a + b), (event_type = 0)）
        # 注意：只有当表达式真正以 '(' 开头时才处理
        # 如果是 func_name(...) 格式，应该由函数调用处理
        if expr_str.startswith("(") and expr_str.endswith(")"):
            # 检查是否为最外层括号（处理嵌套括号）
            depth_count = 0
            is_outer_paren = True
            for i, c in enumerate(expr_str):
                if c == "(":
                    depth_count += 1
                elif c == ")":
                    depth_count -= 1
                # 如果在中间位置深度回到0，说明不是最外层括号
                if i > 0 and i < len(expr_str) - 1 and depth_count == 0:
                    is_outer_paren = False
                    break
            if is_outer_paren:
                inner_expr = expr_str[1:-1].strip()
                if inner_expr:
                    # 递归解析内部表达式
                    # 如果内部是函数调用，也继续递归解析
                    # 函数调用会在递归中被正确处理
                    return self.resolve_text_expr_type(inner_expr, input_schema, depth + 1, visited=visited)

        # 优先级8：字段名查找（小写）
        name_lower = expr_str.lower()
        if name_lower in self.column_type:
            return self.column_type[name_lower]

        # 优先级8.1：嵌套字段路径解析
        if "." in expr_str:
            nested_type = self._resolve_nested_field_path(expr_str)
            if nested_type and nested_type != UNKNOWN:
                return nested_type

        # 优先级9：输入 schema 查找
        if input_schema:
            for field in input_schema:
                if field.get("field_name", "").lower() == name_lower:
                    # 优先使用原始类型，保留精度信息（如 TIMESTAMP(3)）
                    return field.get("original_type") or field.get("field_type", UNKNOWN)

        alias_resolved = self._resolve_alias(expr_str, visited=visited)
        if alias_resolved and alias_resolved != UNKNOWN:
            return alias_resolved

        # 优先级10：比较表达式
        comparison_type = self._resolve_comparison_type(expr_str, input_schema, depth)
        if comparison_type:
            return comparison_type

        # 优先级11：算术表达式（+、-、*、/）
        arithmetic_type = self._resolve_arithmetic_type(expr_str, input_schema, depth)
        if arithmetic_type and arithmetic_type != UNKNOWN:
            return arithmetic_type

        # 优先级12：函数调用
        func_type = self._resolve_text_function_type(expr_str, input_schema, depth)
        if func_type and func_type != UNKNOWN:
            return func_type

        return UNKNOWN

    def _resolve_comparison_type(self, expr_str, input_schema, depth):
        """
        解析比较表达式的类型

        参数说明:
        :param expr_str: str，文本格式的比较表达式
        :param input_schema: list，输入 schema
        :param depth: int，当前递归深度
        :return: str 或 None，比较表达式返回 BOOLEAN，无法解析返回 None

        支持的比较运算符:
        - =: 等于
        - <>: 不等于
        - !=: 不等于
        - >=: 大于等于
        - <=: 小于等于
        - >: 大于
        - <: 小于

        解析逻辑:
        1. 使用正则表达式匹配比较表达式模式
        2. 提取左操作数、运算符和右操作数
        3. 递归解析左右操作数的类型
        4. 只要有一个操作数类型可解析，返回 BOOLEAN
        5. 无法匹配或解析时返回 None

        示例:
        - "age > 18" → BOOLEAN
        - "name = 'test'" → BOOLEAN
        """
        # 使用正则表达式匹配比较表达式
        op_match = re.match(r'^(.+?)\s*(=|<>|!=|>=|<=|>|<)\s*(.+)$', expr_str.strip())
        if not op_match:
            return None

        # 提取左右操作数
        left = op_match.group(1).strip()
        op = op_match.group(2)
        right = op_match.group(3).strip()

        # 空操作数检查
        if not left or not right:
            return None
        left_type = self.resolve_text_expr_type(left, input_schema, depth + 1)
        right_type = self.resolve_text_expr_type(right, input_schema, depth + 1)
        if left_type != UNKNOWN or right_type != UNKNOWN:
            return "BOOLEAN"

        return None

    def _resolve_arithmetic_type(self, expr_str, input_schema, depth):
        """
        解析算术表达式的类型

        参数说明:
        :param expr_str: str，文本格式的算术表达式
        :param input_schema: list，输入 schema
        :param depth: int，当前递归深度
        :return: str 或 None，算术表达式返回 BIGINT，无法解析返回 None

        支持的算术运算符:
        - +: 加法
        - -: 减法
        - *: 乘法
        - /: 除法

        解析逻辑:
        1. 使用正则表达式匹配算术表达式模式
        2. 提取左操作数、运算符和右操作数
        3. 递归解析左右操作数的类型
        4. 只要有一个操作数类型可解析，返回 BIGINT
        5. 无法匹配或解析时返回 None

        示例:
        - "0.985 * bid" → BIGINT
        - "price + 2" → BIGINT
        - "quantity - 1" → BIGINT
        """
        # 使用正则表达式匹配算术表达式（避免匹配函数调用）
        if re.match(r'^[a-zA-Z_]\w*\s*\(', expr_str.strip()):
            return None

        op_match = re.match(r'^(.+?)\s*([+\-*/])\s*(.+)$', expr_str.strip())
        if not op_match:
            return None

        # 提取左右操作数和运算符
        left = op_match.group(1).strip()
        op = op_match.group(2)
        right = op_match.group(3).strip()

        # 空操作数检查
        if not left or not right:
            return None

        # 递归解析左右操作数的类型
        left_type = self.resolve_text_expr_type(left, input_schema, depth + 1)
        right_type = self.resolve_text_expr_type(right, input_schema, depth + 1)
        if left_type != UNKNOWN or right_type != UNKNOWN:
            return "DECIMAL128"

        return None

    def _resolve_alias(self, param, visited=None):
        if visited is None:
            visited = set()
        if param in visited:
            return None
        visited.add(param)
        alias_param = re.sub(r"\[\d+\]$", "", param)

        # 在别名映射中查找
        if alias_param in self.alias_map:
            real_param = self.alias_map[alias_param]
            return self.resolve_text_expr_type(real_param, None, 0, visited=visited)
        return None

    def _resolve_text_function_type(self, expr_str, input_schema, depth):
        """
        解析文本格式函数的类型

        参数说明:
        :param expr_str: str，文本格式的函数调用表达式
        :param input_schema: list，输入 schema
        :param depth: int，当前递归深度
        :return: str 或 None，函数返回类型，无法解析返回 None

        解析流程:
        1. 使用正则表达式提取函数名
        2. 在函数返回类型字典中查找函数定义
        3. 根据函数定义进行类型推断：
           - case 函数：调用专门的解析方法
           - 不需要参数类型：直接返回配置的类型
           - ARGUMENT_TYPE：提取第一个参数的类型
           - RESULT_TYPE：处理特殊函数（如 cast）

        支持的特殊函数:
        - case: 条件表达式
        - cast/try_cast: 类型转换

        示例:
        - "upper(name)" → VARCHAR
        - "cast(age AS STRING)" → VARCHAR
        """
        # 使用正则表达式提取函数名（必须以字母或下划线开头）
        func_match = re.match(r'^([a-zA-Z_]\w*)\s*\(', expr_str)
        if not func_match:
            return None

        # 获取函数名（小写）
        func_name = func_match.group(1).lower()

        # 在函数返回类型字典中查找
        dict_entry = self.return_type_dict.get(func_name)
        if not dict_entry:
            return None

        # ===== case 函数：特殊处理 =====
        if func_name == "case":
            return self._resolve_case_return_type_from_text(expr_str, input_schema, depth)

        # ===== 不需要参数类型的函数 =====
        if not dict_entry.get("need_param_type", False):
            ret = dict_entry.get("return_type", UNKNOWN)
            return ret if ret != UNKNOWN else None

        # ===== 需要根据参数类型推断 =====
        rule = dict_entry.get("return_type", "")

        # ARGUMENT_TYPE：返回第一个参数的类型
        if rule == "ARGUMENT_TYPE":
            return self._extract_first_arg_type_from_text(expr_str, input_schema, depth)

        # RESULT_TYPE：处理特殊函数
        if rule == "RESULT_TYPE":
            if func_name in ("cast", "try_cast"):
                # 提取 AS 后面的目标类型
                cast_match = re.search(r'\bAS\s+(\w+)', expr_str, re.I)
                if cast_match:
                    return TypeNormalizer.normalize_type(cast_match.group(1))
            return None

        return None

    def _resolve_case_return_type_from_text(self, expr_str, input_schema, depth):
        """
        解析文本格式 CASE 表达式的返回类型

        参数说明:
        :param expr_str: str，文本格式的 CASE 表达式（如 "case a when 1 then 'x' else 'y' end"）
        :param input_schema: list，输入 schema
        :param depth: int，当前递归深度
        :return: str，CASE 表达式的返回类型；无法解析时返回 UNKNOWN

        CASE 表达式格式:
        case <cond1> when <val1> [when <val2> ...] [else <default>] end

        解析逻辑:
        1. 提取函数参数部分
        2. 按逗号分割参数
        3. 识别值表达式（跳过条件表达式）
           - 偶数索引（0, 2, 4...）通常是条件
           - 如果有 else 分支，最后一个参数是值
        4. 收集所有值表达式的类型
        5. 使用 TypeNormalizer.find_common_type_multi 找到公共类型

        设计考量:
        - CASE 表达式的参数是成对出现的（条件, 值, 条件, 值, ... [else值]）
        - 只关注值表达式的类型，条件表达式类型不影响结果

        示例:
        case status when 1 then 'active' when 0 then 'inactive' else 'unknown' end → VARCHAR
        """
        # 提取函数参数部分
        args_str = self._extract_function_args_text(expr_str)
        if not args_str:
            return UNKNOWN

        # 按逗号分割参数（考虑嵌套括号）
        args = self._split_function_args(args_str)

        # 收集值表达式的类型
        value_types = []

        # 判断是否有 else 分支（参数数量为奇数时有 else）
        has_else = len(args) % 2 == 1

        # 遍历参数，跳过条件表达式（偶数索引，除了可能的最后一个）
        for i, arg in enumerate(args):
            arg = arg.strip()
            # 跳过条件表达式（偶数索引且不是 else 分支）
            if i % 2 == 0 and not (has_else and i == len(args) - 1):
                continue
            t = self.resolve_text_expr_type(arg, input_schema, depth + 1)
            if t and t != UNKNOWN:
                value_types.append(t)

        # 没有有效类型，返回 UNKNOWN
        if not value_types:
            return UNKNOWN

        # 找到公共类型
        common = TypeNormalizer.find_common_type_multi(value_types)
        if common and common != UNKNOWN:
            return common

        return UNKNOWN

    def _extract_first_arg_type_from_text(self, expr_str, input_schema, depth):
        """
        提取函数第一个参数的类型

        参数说明:
        :param expr_str: str，文本格式的函数调用表达式
        :param input_schema: list，输入 schema
        :param depth: int，当前递归深度
        :return: str 或 None，第一个参数的类型，无法解析返回 None

        解析流程:
        1. 提取函数参数部分
        2. 使用 _split_function_args 分割参数（处理嵌套括号）
        3. 取第一个参数并去除首尾空白
        4. 递归解析第一个参数的类型

        设计考量:
        - 用于处理 ARGUMENT_TYPE 规则的函数
        - 使用 _split_function_args 处理嵌套括号场景

        示例:
        upper(name) → 提取 "name" → VARCHAR
        """
        # 提取函数参数部分
        inner = self._extract_function_args_text(expr_str)
        if not inner:
            return None

        # 使用 _split_function_args 分割参数（处理嵌套括号）
        args = self._split_function_args(inner)
        if not args:
            return None

        first_arg = args[0].strip()
        if not first_arg:
            return None

        arg_type = self.resolve_text_expr_type(first_arg, input_schema, depth + 1)
        return arg_type if arg_type != UNKNOWN else None

    @staticmethod
    def _extract_function_args_text(expr_str):
        """
        提取函数参数文本

        参数说明:
        :param expr_str: str，函数调用表达式（如 "upper(name)"）
        :return: str 或 None，括号内的参数文本，无法提取返回 None

        解析逻辑:
        1. 找到第一个左括号的位置
        2. 使用深度计数器处理嵌套括号
        3. 找到匹配的右括号
        4. 返回括号内的文本

        设计考量:
        - 支持嵌套函数调用（如 "upper(trim(name))"）
        - 正确处理括号匹配

        示例:
        "upper(name)" → "name"
        "concat(upper(a), lower(b))" → "upper(a), lower(b)"
        """
        # 找到第一个左括号
        start = expr_str.find("(")
        if start == -1:
            return None

        # 使用深度计数器处理嵌套括号
        depth = 0
        for i in range(start, len(expr_str)):
            if expr_str[i] == "(":
                depth += 1
            elif expr_str[i] == ")":
                depth -= 1
                # 找到匹配的右括号
                if depth == 0:
                    return expr_str[start + 1: i]

        return expr_str[start + 1:]

    @staticmethod
    def _split_function_args(args_str):
        """
        按逗号分割函数参数（考虑嵌套括号）

        参数说明:
        :param args_str: str，函数参数文本（不含括号）
        :return: list，参数列表

        解析逻辑:
        1. 遍历字符串，使用深度计数器跟踪括号嵌套
        2. 遇到逗号且深度为 0 时分割
        3. 遇到左括号增加深度
        4. 遇到右括号减少深度
        5. 最后一个参数添加到列表

        设计考量:
        - 正确处理嵌套函数调用中的逗号
        - 保持参数的完整性

        示例:
        "a, upper(b), c" → ["a", " upper(b)", " c"]
        "concat(a, b), trim(c)" → ["concat(a, b)", " trim(c)"]
        """
        if not args_str:
            return []

        parts = []
        depth = 0
        current = []

        for ch in args_str:
            if ch == "(":
                depth += 1
                current.append(ch)
            elif ch == ")":
                depth -= 1
                current.append(ch)
            elif ch == "," and depth == 0:
                # 只有在最外层（深度为0）才分割
                parts.append("".join(current))
                current = []
            else:
                current.append(ch)

        # 添加最后一个参数
        if current:
            parts.append("".join(current))

        return parts

    def find_json_descriptions(self, description_data):
        """
        查找 JSON 格式的描述对象

        参数说明:
        :param description_data: list，描述数据列表，包含字符串或字典
        :return: list，符合条件的 JSON 描述对象列表

        识别条件:
        - 必须是字典类型
        - 包含 inputTypes、outputTypes 或 originDescription 字段之一

        设计考量:
        - Flink 算子描述可能包含多种格式（字符串和 JSON）
        - 该方法用于筛选出结构化的 JSON 描述
        - 这些 JSON 对象包含算子的输入输出类型信息

        使用场景:
        - 从算子描述中提取类型信息
        - 构建算子的输出 schema
        """
        results = []
        if not description_data:
            return results

        # 遍历描述数据，筛选符合条件的 JSON 对象
        for item in description_data:
            if isinstance(item, dict):
                if "inputTypes" in item or "outputTypes" in item or "originDescription" in item:
                    results.append(item)

        return results

    def find_json_desc_for_op(self, op_type, description_data):
        """
        查找特定算子的 JSON 描述

        参数说明:
        :param op_type: str，算子类型（如 "Calc", "GroupAggregate"）
        :param description_data: list，描述数据列表
        :return: dict 或 None，匹配的 JSON 描述对象，无法找到返回 None

        匹配策略:
        1. 提取所有 JSON 格式的描述对象
        2. 如果只有一个，直接返回
        3. 否则根据算子类型在 originDescription 中查找匹配
        4. 找不到匹配时返回第一个

        设计考量:
        - 一个算子可能有多个描述对象
        - 需要根据算子类型选择最合适的描述
        - originDescription 通常包含算子类型信息

        示例:
        op_type = "Calc"
        description_data = [{"originDescription": "Calc(select=[...])", ...}]
        → 返回匹配的 JSON 对象
        """
        # 提取所有 JSON 格式的描述
        all_json = self.find_json_descriptions(description_data)
        if not all_json:
            return None

        # 如果只有一个描述对象，直接返回
        if len(all_json) == 1:
            return all_json[0]

        # 根据算子类型查找匹配的描述
        op_type_lower = op_type.lower()
        for desc in all_json:
            origin = desc.get("originDescription") or ""
            if op_type_lower in origin.lower():
                return desc

        # 返回第一个作为兜底
        return all_json[0] if all_json else None

    def _parse_text_output_types(self, matched_text, input_schema):
        """
        解析文本格式的输出类型

        参数说明:
        :param matched_text: str，逗号分隔的输出类型或字段索引列表
        :param input_schema: list，输入 schema
        :return: list，解析后的类型列表

        解析策略:
        1. 按逗号分割字符串
        2. 判断是否全部为整数（字段索引）
        3. 如果是索引，从 input_schema 中获取对应类型
        4. 如果不是索引，调用 _parse_select_types 解析表达式

        支持的格式:
        - 索引格式: "0, 1, 2" → 从 input_schema 获取类型
        - 表达式格式: "name, upper(age)" → 解析表达式类型

        示例:
        _parse_text_output_types("0, 1", [{"field_name": "id", "field_type": "INT"}, ...])
        → ["INT", "VARCHAR"]

        _parse_text_output_types("name, upper(age)", input_schema)
        → ["VARCHAR", "VARCHAR"]
        """
        # 按逗号分割并去除首尾空白
        items = [item.strip() for item in matched_text.split(",") if item.strip()]
        if not items:
            return []

        # 判断是否全部为整数（字段索引）
        all_int = True
        indices = []
        for item in items:
            try:
                indices.append(int(item))
            except ValueError:
                all_int = False
                break

        # 如果是索引格式，从 input_schema 获取类型
        if all_int and indices and input_schema:
            types = []
            for idx in indices:
                if 0 <= idx < len(input_schema):
                    types.append(input_schema[idx].get("field_type", UNKNOWN))
                else:
                    types.append(UNKNOWN)
            return types

        # 否则解析为 SELECT 表达式
        return self._parse_select_types(matched_text, input_schema)

    def _parse_select_types(self, select_str, input_schema):
        """
        解析 SELECT 表达式的类型

        参数说明:
        :param select_str: str，逗号分隔的 SELECT 项目列表
        :param input_schema: list，输入 schema
        :return: list，每个项目的类型列表

        解析流程:
        1. 使用 _split_select_items 分割项目（考虑嵌套括号）
        2. 对每个项目分离原始表达式和别名
        3. 解析原始表达式的类型
        4. 返回类型列表

        设计考量:
        - 支持带别名的表达式（如 "name AS n"）
        - 正确处理嵌套函数调用

        示例:
        _parse_select_types("id, name AS n, upper(name)", input_schema)
        → ["INT", "VARCHAR", "VARCHAR"]
        """
        types = []
        # 分割 SELECT 项目（考虑嵌套括号）
        items = self._split_select_items(select_str)

        for item in items:
            item = item.strip()
            if not item:
                continue

            # 分离原始表达式和别名
            original_expr, _ = self._split_alias_from_expr(item)
            t = self.resolve_text_expr_type(original_expr, input_schema, 0)
            types.append(t)

        return types

    @staticmethod
    def _split_select_items(text):
        """
        分割 SELECT 项目列表（考虑嵌套括号）

        参数说明:
        :param text: str，逗号分隔的项目列表
        :return: list，分割后的项目列表

        解析逻辑:
        1. 遍历字符串，使用深度计数器跟踪括号嵌套
        2. 遇到逗号且深度为 0 时分割
        3. 遇到左括号增加深度
        4. 遇到右括号减少深度
        5. 最后一个项目添加到列表

        设计考量:
        - 正确处理嵌套函数调用中的逗号
        - 保持项目的完整性

        示例:
        "id, upper(name), concat(a, b)" → ["id", " upper(name)", " concat(a, b)"]
        """
        items = []
        current = []
        depth = 0
        in_string = False
        string_char = None

        for char in text:
            if not in_string and char in ("'", '"'):
                in_string = True
                string_char = char
                current.append(char)
            elif in_string and char == string_char:
                in_string = False
                string_char = None
                current.append(char)
            elif not in_string and char == "(":
                depth += 1
                current.append(char)
            elif not in_string and char == ")":
                depth -= 1
                current.append(char)
            elif not in_string and char == "," and depth == 0:
                items.append("".join(current))
                current = []
            else:
                current.append(char)

        if current:
            items.append("".join(current))

        return items

    def _find_all_select_clauses(self, text):
        """
        查找所有 select=[...] 子句（处理嵌套括号）
        
        参数说明:
        :param text: str，包含 select=[] 的文本
        :return: list，所有 select 子句的内容列表（不含 select=[] 括号）
        
        解析逻辑:
        1. 遍历字符串，查找 "select=[" 模式
        2. 找到后使用深度计数器跟踪括号嵌套
        3. 遇到左括号增加深度，遇到右括号减少深度
        4. 深度为 0 且遇到 "]" 时，表示一个 select 子句结束
        5. 提取 select=[...] 内部的内容
        
        示例:
        "Calc(select=[id, name]) Calc(select=[a, b])" → ["id, name", "a, b"]
        """
        clauses = []
        i = 0
        text_len = len(text)
        
        while i < text_len:
            # 查找 "select=[" 模式（不区分大小写）
            if text[i:i+8].lower() == "select=[" and i + 8 < text_len:
                start = i + 8  # 跳过 "select=["
                depth = 0
                current_clause = []
                j = start
                
                while j < text_len:
                    char = text[j]
                    if char == "[":
                        depth += 1
                        current_clause.append(char)
                    elif char == "]":
                        if depth == 0:
                            # 找到匹配的结束括号
                            clauses.append("".join(current_clause))
                            break
                        else:
                            depth -= 1
                            current_clause.append(char)
                    else:
                        current_clause.append(char)
                    j += 1
                i = j + 1  # 跳过当前的 "]"
            else:
                i += 1
        
        return clauses

    def extract_alias_map_from_description(self, description_data):
        """
        从描述中提取别名映射

        参数说明:
        :param description_data: list，描述数据列表
        :return: dict，别名→原始表达式 映射

        支持的格式:
        - select=[field1 AS alias1, field2 AS alias2]
        - field AS alias（直接在文本中）

        解析流程:
        1. 遍历描述数据
        2. 对于字符串描述：
           a. 查找 select=[...] 格式
           b. 查找 field AS alias 格式
        3. 使用 _split_alias_from_expr 分离原始表达式和别名
        4. 过滤掉类型关键字作为别名的情况

        过滤规则:
        - 别名为 SQL 类型关键字（如 INT, VARCHAR）时跳过
        - 原始表达式和别名相同时跳过

        使用场景:
        - 从算子描述中提取字段别名信息
        - 支持后续的别名解析

        示例:
        desc = "Calc(select=[id AS user_id, name AS user_name])"
        → {"user_id": "id", "user_name": "name"}
        """
        alias_map = {}

        for desc in description_data:
            if isinstance(desc, str):
                # 查找 select=[...] 格式
                select_match = re.search(r'select=\[(.*?)\]', desc, re.I)
                if select_match:
                    items = self._split_select_items(select_match.group(1))
                    for item in items:
                        item = item.strip()
                        if not item:
                            continue
                        original, alias = self._split_alias_from_expr(item)
                        if original != alias and alias:
                            alias_map[alias] = original
                else:
                    # 查找 field AS alias 格式
                    as_matches = re.finditer(r'(\S+)\s+AS\s+(\w+)', desc, re.I)
                    for m in as_matches:
                        original = m.group(1)
                        alias = m.group(2)
                        # 过滤类型关键字作为别名的情况
                        if alias.upper() in ("INT", "BIGINT", "VARCHAR", "DOUBLE", "FLOAT",
                                             "BOOLEAN", "DATE", "TIMESTAMP", "DECIMAL",
                                             "STRING", "LONG", "SHORT", "BYTE", "CHAR"):
                            continue
                        if original != alias:
                            alias_map[alias] = original

        # 更新内部别名映射
        self.update_alias_map(alias_map)
        return alias_map

    @staticmethod
    def _extract_table_name_from_origin(origin_desc):
        """
        从 originDescription 提取表名

        参数说明:
        :param origin_desc: str，originDescription 字段内容
        :return: str 或 None，提取的表名，无法提取返回 None

        支持的模式（按优先级）:
        1. TableSourceScan(table=[[db, schema, table]]) → db.schema.table
        2. Source: [tableName]-tableName → tableName
        3. TableSourceScan(table=[schema.table], table=[table]) → schema.table
        4. Source: xxx - schema.table → schema.table
        5. table=[schema.table] → schema.table

        正则表达式设计:
        - 使用 re.I 标志忽略大小写
        - 支持不同格式的表名提取
        - 优先匹配更精确的模式

        示例:
        "TableSourceScan(table=[[mydb, myschema, mytable]])" → "mydb.myschema.mytable"
        "Source: [user_table]-user_table" → "user_table"
        """
        if not origin_desc:
            return None

        # 定义表名提取模式（按优先级排序）
        patterns = [
            # 模式1: TableSourceScan(table=[[db, schema, table]])
            r'TableSourceScan\(table=\[\[([\w-]+),\s*([\w-]+),\s*([\w-]+)\]\]',
            # 模式2: Source: [tableName]-tableName
            r'Source:\s+\[?\w*[\w-]*\]?\s*-\s*\w*[\w-]*\s*(\w+[\w.-]*\w+)',
            # 模式3: TableSourceScan(table=[schema.table], table=[...])
            r'TableSourceScan\(table=\[\w+\.\w+\],\s*table=\[*(\w+\.\w+)',
            # 模式4: Source: xxx - schema.table
            r'Source:\s+\S+\s*-\s*(\w+\.\w+)',
            # 模式5: table=[schema.table]
            r'table=\[*(\w+\.\w+)',
        ]

        # 按顺序尝试匹配
        for pattern in patterns:
            match = re.search(pattern, origin_desc, re.I)
            if match:
                groups = match.groups()
                # 如果匹配到3个组（db, schema, table），拼接成全限定表名
                if len(groups) == 3:
                    return f"{groups[0]}.{groups[1]}.{groups[2]}"
                # 否则返回第一个捕获组
                return match.group(1)

        return None

    def _extract_table_name_from_text(self, desc):
        """
        从文本描述提取表名

        参数说明:
        :param desc: str，文本描述
        :return: str 或 None，提取的表名，无法提取返回 None

        支持的模式（按优先级）:
        1. TableSourceScan(table=[[db, schema, table]]) → db.schema.table
        2. TableSourceScan(table=[schema.table]) → schema.table
        3. Source: xxx - schema.table → schema.table
        4. Scan xxx schema.table → schema.table

        设计考量:
        - 与 _extract_table_name_from_origin 类似，但针对文本描述优化
        - 支持多种 Flink 日志中常见的表名格式
        """
        patterns = [
            # 模式1: TableSourceScan(table=[[db, schema, table]])
            r'TableSourceScan\(table=\[\[([\w-]+),\s*([\w-]+),\s*([\w-]+)\]\]',
            # 模式2: TableSourceScan(table=[schema.table])
            r'TableSourceScan\(table=\[*(\w+\.\w+)',
            # 模式3: Source: xxx - schema.table
            r'Source:\s+\S+\s*-\s*(\w+\.\w+)',
            # 模式4: Scan xxx schema.table
            r'Scan\s+\w+\s+(\w+\.\w+)',
        ]

        # 按顺序尝试匹配
        for pattern in patterns:
            match = re.search(pattern, desc, re.I)
            if match:
                groups = match.groups()
                # 如果匹配到3个组，拼接成全限定表名
                if len(groups) == 3:
                    return f"{groups[0]}.{groups[1]}.{groups[2]}"
                return match.group(1)

        return None

    def _extract_fields_from_text(self, desc):
        """
        从文本描述提取字段信息

        参数说明:
        :param desc: str，文本描述
        :return: list，字段信息列表，每个元素包含 field_name 和 field_type

        支持的格式:
        - fields=[field1, field2, field3, ...]

        解析流程:
        1. 查找所有 fields=[...] 子句（处理嵌套括号）
        2. 按逗号分割字段名
        3. 为每个字段名解析类型（使用 _resolve_field_type_by_name）
        4. 返回字段信息列表

        示例:
        desc = "Calc(fields=[id, name, age])"
        → [{"field_name": "id", "field_type": "INT"}, ...]
        """
        if not desc:
            return []

        # 查找所有 fields=[...] 子句（处理嵌套括号）
        fields_clauses = self._find_all_fields_clauses(desc)
        if not fields_clauses:
            return []

        # 为每个字段名解析类型
        result = []
        for fields_str in fields_clauses:
            field_names = [f.strip() for f in fields_str.split(',') if f.strip()]
            for name in field_names:
                field_type = self._resolve_field_type_by_name(name)
                result.append({
                    "field_name": name,
                    "field_type": field_type,
                })
        return result

    def _find_all_fields_clauses(self, text):
        """
        查找所有 fields=[...] 子句（处理嵌套括号）
        
        参数说明:
        :param text: str，包含 fields=[] 的文本
        :return: list，所有 fields 子句的内容列表（不含 fields=[] 括号）
        
        解析逻辑:
        1. 遍历字符串，查找 "fields=[" 模式
        2. 找到后使用深度计数器跟踪括号嵌套
        3. 遇到左括号增加深度，遇到右括号减少深度
        4. 深度为 0 且遇到 "]" 时，表示一个 fields 子句结束
        5. 提取 fields=[...] 内部的内容
        
        示例:
        "Scan(fields=[id, name]) Filter(fields=[a, b])" → ["id, name", "a, b"]
        """
        clauses = []
        i = 0
        text_len = len(text)
        
        while i < text_len:
            # 查找 "fields=[" 模式（不区分大小写）
            if text[i:i+8].lower() == "fields=[" and i + 8 < text_len:
                start = i + 8  # 跳过 "fields=["
                depth = 0
                current_clause = []
                j = start
                
                while j < text_len:
                    char = text[j]
                    if char == "[":
                        depth += 1
                        current_clause.append(char)
                    elif char == "]":
                        if depth == 0:
                            # 找到匹配的结束括号
                            clauses.append("".join(current_clause))
                            break
                        else:
                            depth -= 1
                            current_clause.append(char)
                    else:
                        current_clause.append(char)
                    j += 1
                i = j + 1  # 跳过当前的 "]"
            else:
                i += 1
        
        return clauses

    def _resolve_field_type_by_name(self, field_name):
        """
        根据字段名解析类型

        参数说明:
        :param field_name: str，字段名
        :return: str，字段类型，无法解析返回 "unknown"

        查找策略:
        1. 首先在 column_type 映射中查找（精确匹配）
        2. 如果未找到，在 table_column_type 映射中查找（模糊匹配，以.field_name 结尾）
        3. 都未找到返回 "unknown"

        设计考量:
        - 优先精确匹配，提高效率
        - 支持表名限定的字段查找作为 fallback

        示例:
        column_type = {"id": "INT"}
        table_column_type = {"users.name": "VARCHAR"}
        _resolve_field_type_by_name("id") → "INT"
        _resolve_field_type_by_name("name") → "VARCHAR"
        """
        # 优先在 column_type 中精确查找
        if self.column_type and field_name in self.column_type:
            return TypeNormalizer.normalize_type(self.column_type[field_name])

        # 在 table_column_type 中模糊查找（以.field_name 结尾）
        if self.table_column_type:
            for key, type_val in self.table_column_type.items():
                if key.endswith(f".{field_name}"):
                    return TypeNormalizer.normalize_type(type_val)

        return "unknown"

    def _resolve_field_name_from_expr(self, expr, input_schema, default_index):
        """
        从表达式解析字段名

        参数说明:
        :param expr: dict，JSON 格式的表达式对象
        :param input_schema: list，输入 schema
        :param default_index: int，默认索引（用于生成默认字段名）
        :return: str，解析得到的字段名

        解析规则:
        1. FIELD_REFERENCE: 从输入 schema 中获取字段名，或使用 field_{index}
        2. FUNCTION: 使用函数名，或使用 expr_{default_index}
        3. 其他: 使用 expr_{default_index}

        设计考量:
        - 优先使用有意义的字段名
        - 无法确定时使用默认命名规则

        示例:
        expr = {"exprType": "FIELD_REFERENCE", "colVal": 0}
        input_schema = [{"field_name": "id", "field_type": "INT"}]
        → "id"

        expr = {"exprType": "FUNCTION", "function_name": "upper"}
        → "upper"
        """
        expr_type = expr.get("exprType", "")

        # FIELD_REFERENCE: 字段引用
        if expr_type == "FIELD_REFERENCE":
            col_val = expr.get("colVal", -1)
            if input_schema and 0 <= col_val < len(input_schema):
                return input_schema[col_val].get("field_name", f"field_{col_val}")

        # FUNCTION: 函数调用
        if expr_type == "FUNCTION":
            return expr.get("function_name", f"expr_{default_index}")

        # 默认返回
        return f"expr_{default_index}"

    def build_output_schema(self, op_type, description_data, input_schema=None):
        """
        构建算子的输出 schema

        参数说明:
        :param op_type: str，算子类型（如 "Calc", "GroupAggregate", "Join"）
        :param description_data: list，描述数据列表，包含字符串或 JSON 对象
        :param input_schema: list，输入 schema（可选）
        :return: list，输出 schema，每个元素包含 field_name 和 field_type

        构建策略:
        1. 如果没有描述数据，返回输入 schema（如果有）
        2. 优先查找 JSON 格式的描述对象
        3. 使用 JSON 描述构建输出 schema
        4. 如果没有 JSON 描述，使用文本描述构建

        设计考量:
        - JSON 格式的描述更结构化，优先使用
        - 文本格式作为 fallback
        - 不同算子类型有不同的构建逻辑

        支持的算子类型:
        - Calc: 计算表达式
        - GroupAggregate: 分组聚合
        - Join/WindowJoin: 连接
        - LookupJoin: 维表关联
        - Deduplicate/Expand/WatermarkAssigner: 透传输入 schema
        """
        # 没有描述数据，返回输入 schema
        if not description_data:
            return input_schema or []

        # 优先查找 JSON 格式的描述对象
        json_desc = self.find_json_desc_for_op(op_type, description_data)
        if json_desc:
            return self._build_output_schema_from_json(op_type, json_desc, input_schema)

        # 使用文本描述构建
        return self._build_output_schema_from_text(op_type, description_data, input_schema)

    def _build_output_schema_from_json(self, op_type, json_desc, input_schema):
        """
        从 JSON 描述构建输出 schema

        参数说明:
        :param op_type: str，算子类型
        :param json_desc: dict，JSON 格式的描述对象
        :param input_schema: list，输入 schema
        :return: list，输出 schema

        支持的算子类型及处理逻辑:
        1. 通用情况：使用 outputNames + outputTypes 或仅 outputTypes
        2. Calc: 从 indices 字段构建
        3. GroupAggregate: 从 grouping 和 aggInfoList 构建
        4. Join/WindowJoin: 合并 leftInputTypes 和 rightInputTypes
        5. LookupJoin: 合并 inputTypes 和 lookupInputTypes
        6. 其他: 透传输入 schema

        构建优先级:
        1. outputNames + outputTypes（最准确）
        2. 仅 outputTypes（生成默认字段名）
        3. 算子特定逻辑
        4. 透传输入 schema

        设计考量:
        - 优先使用直接提供的类型信息
        - 无法直接获取时根据表达式推断
        - 保持字段名的可读性
        """
        output_schema = []

        # 获取输出名称和类型
        output_names = json_desc.get("outputNames", [])
        output_types = json_desc.get("outputTypes", [])
        if output_names and output_types:
            for name, type_str in zip(output_names, output_types):
                schema_entry = {
                    "field_name": name,
                    "field_type": TypeNormalizer.normalize_type(type_str),
                    "original_type": type_str,
                }
                # 对于 ROW 类型，从 table_schema 中查找原始类型（可能包含更详细的信息）
                original_type = self._find_original_type_from_table_schema(name)
                if original_type:
                    schema_entry["original_type"] = original_type
                output_schema.append(schema_entry)
            return output_schema
        if output_types and not output_names:
            for i, type_str in enumerate(output_types):
                output_schema.append({
                    "field_name": f"field_{i}",
                    "field_type": TypeNormalizer.normalize_type(type_str),
                })
            return output_schema
        if op_type == "Calc" and json_desc.get("indices"):
            indices = json_desc.get("indices", [])
            for i, idx_expr in enumerate(indices):
                if isinstance(idx_expr, dict):
                    field_name = self._resolve_field_name_from_expr(idx_expr, input_schema, i)
                    field_type = self.resolve_expression_type(idx_expr, input_schema)
                    schema_entry = {
                        "field_name": field_name,
                        "field_type": field_type,
                    }
                    # ★ 修改点：从 table_schema 中查找原始类型
                    original_type = self._find_original_type_from_table_schema(field_name)
                    if original_type:
                        schema_entry["original_type"] = original_type
                    output_schema.append(schema_entry)
        elif op_type == "GroupAggregate":
            # 添加分组字段
            grouping = json_desc.get("grouping", [])
            if grouping and input_schema:
                for idx in grouping:
                    if 0 <= idx < len(input_schema):
                        field_name = input_schema[idx].get("field_name", f"group_{idx}")
                        schema_entry = {
                            "field_name": field_name,
                            "field_type": input_schema[idx].get("field_type", UNKNOWN),
                        }
                        # ★ 修改点：从 table_schema 中查找原始类型
                        original_type = self._find_original_type_from_table_schema(field_name)
                        if original_type:
                            schema_entry["original_type"] = original_type
                        output_schema.append(schema_entry)
            agg_info = json_desc.get("aggInfoList", {})
            agg_calls = agg_info.get("aggregateCalls", [])
            agg_value_types = agg_info.get("aggValueTypes", [])
            for i, call in enumerate(agg_calls):
                agg_name = call.get("name", f"agg_{i}")
                if i < len(agg_value_types):
                    field_type = TypeNormalizer.normalize_type(agg_value_types[i])
                else:
                    agg_func = call.get("aggregationFunction", "")
                    field_type = self._resolve_agg_func_return_type(agg_func, input_schema, call)
                schema_entry = {
                    "field_name": agg_name,
                    "field_type": field_type,
                }
                # ★ 修改点：从 table_schema 中查找原始类型
                original_type = self._find_original_type_from_table_schema(agg_name)
                if original_type:
                    schema_entry["original_type"] = original_type
                output_schema.append(schema_entry)
        elif op_type in ("Join", "WindowJoin"):
            left_types = json_desc.get("leftInputTypes", [])
            right_types = json_desc.get("rightInputTypes", [])
            all_types = left_types + right_types
            for i, t in enumerate(all_types):
                field_name = f"field_{i}"
                schema_entry = {
                    "field_name": field_name,
                    "field_type": TypeNormalizer.normalize_type(t),
                }
                # ★ 修改点：从 table_schema 中查找原始类型
                original_type = self._find_original_type_from_table_schema(field_name)
                if original_type:
                    schema_entry["original_type"] = original_type
                output_schema.append(schema_entry)
        elif op_type == "LookupJoin":
            input_types = json_desc.get("inputTypes", [])
            lookup_types = json_desc.get("lookupInputTypes", [])
            all_types = input_types + lookup_types
            for i, t in enumerate(all_types):
                field_name = f"field_{i}"
                schema_entry = {
                    "field_name": field_name,
                    "field_type": TypeNormalizer.normalize_type(t),
                }
                # ★ 修改点：从 table_schema 中查找原始类型
                original_type = self._find_original_type_from_table_schema(field_name)
                if original_type:
                    schema_entry["original_type"] = original_type
                output_schema.append(schema_entry)
        else:
            if input_schema:
                output_schema = list(input_schema)
        return output_schema

    def _build_output_schema_from_text(self, op_type, description_data, input_schema):
        """
        从文本描述构建输出 schema

        参数说明:
        :param op_type: str，算子类型
        :param description_data: list，描述数据列表
        :param input_schema: list，输入 schema
        :return: list，输出 schema

        支持的算子类型及处理逻辑:
        1. Deduplicate/Expand/WatermarkAssigner/StreamRecordTimestampInserter/ConstraintEnforcer/Sink:
           - 透传输入 schema（这些算子不改变字段结构）
        2. Calc:
           - 从 select=[...] 格式提取字段信息
        3. GroupAggregate:
           - 从 groupBy=[...] 格式提取分组字段索引
        4. 其他:
           - 透传输入 schema

        设计考量:
        - 文本描述信息有限，尽可能提取有用信息
        - 无法提取时透传输入 schema 作为 fallback
        """
        # ===== 透传算子：不改变字段结构 =====
        if op_type in ("Deduplicate", "Expand", "WatermarkAssigner",
                       "StreamRecordTimestampInserter", "ConstraintEnforcer", "Sink"):
            return list(input_schema) if input_schema else []

        # ===== Calc 算子：从 select=[...] 提取 =====
        if op_type == "Calc":
            output_schema = []
            for desc in description_data:
                if isinstance(desc, str):
                    select_matches = self._find_all_select_clauses(desc)
                    for select_str in select_matches:
                        output_schema.extend(self._build_calc_output_from_text(select_str, input_schema))
            return output_schema if output_schema else list(input_schema or [])

        # ===== GroupAggregate 算子：从 groupBy=[...] 提取 =====
        if op_type == "GroupAggregate":
            output_schema = []
            for desc in description_data:
                if isinstance(desc, str):
                    groupby_match = re.search(r'groupBy=\[(.*?)\]', desc, re.I)
                    if groupby_match and input_schema:
                        for idx_str in groupby_match.group(1).split(","):
                            try:
                                idx = int(idx_str.strip())
                                if 0 <= idx < len(input_schema):
                                    output_schema.append(input_schema[idx])
                            except ValueError:
                                pass
            return output_schema if output_schema else list(input_schema or [])

        # ===== 其他算子：透传输入 schema =====
        return list(input_schema) if input_schema else []

    def _build_calc_output_from_text(self, select_str, input_schema):
        """
        构建 Calc 算子的输出 schema

        参数说明:
        :param select_str: str，SELECT 表达式字符串（不含 select=[] 括号）
        :param input_schema: list，输入 schema
        :return: list，输出 schema

        解析流程:
        1. 使用 _split_select_items 分割 SELECT 项目
        2. 对每个项目分离原始表达式和别名
        3. 解析原始表达式的类型
        4. 构建输出 schema（使用别名作为字段名）

        示例:
        select_str = "id, name AS user_name, upper(name)"
        → [{"field_name": "id", "field_type": "INT"},
           {"field_name": "user_name", "field_type": "VARCHAR"},
           {"field_name": "upper(name)", "field_type": "VARCHAR"}]
        """
        output_schema = []

        # 分割 SELECT 项目（考虑嵌套括号）
        items = self._split_select_items(select_str)

        for i, item in enumerate(items):
            item = item.strip()
            if not item:
                continue
            original_expr, alias_name = self._split_alias_from_expr(item)
            field_type = self.resolve_text_expr_type(original_expr, input_schema, 0)
            schema_entry = {
                "field_name": alias_name,
                "field_type": field_type,
            }
            # ★ 修改点：从 table_schema 中查找原始类型
            original_type = self._find_original_type_from_table_schema(alias_name)
            if original_type:
                schema_entry["original_type"] = original_type
            output_schema.append(schema_entry)
        return output_schema

    def _resolve_agg_func_return_type(self, agg_func, input_schema, call):
        """
        解析聚合函数的返回类型

        参数说明:
        :param agg_func: str，聚合函数名称
        :param input_schema: list，输入 schema
        :param call: dict，聚合调用信息
        :return: str，聚合函数的返回类型，目前返回 UNKNOWN

        注意:
        - 当前实现返回 UNKNOWN
        - 可扩展支持常见聚合函数（如 SUM, COUNT, AVG 等）的返回类型推断
        - 需要根据聚合函数类型和参数类型推断返回类型

        扩展建议:
        - COUNT → BIGINT
        - SUM/AVG → 与输入类型相关（SUM(INT) → BIGINT, SUM(DOUBLE) → DOUBLE）
        - MIN/MAX → 与输入类型相同
        """
        return UNKNOWN

    @staticmethod
    def _split_alias_from_expr(expr_str):
        """
        从表达式中分离别名

        参数说明:
        :param expr_str: str，表达式字符串（如 "name AS user_name"）
        :return: tuple，(original_expr, alias)，如果没有别名则返回 (expr_str, expr_str)

        支持的格式:
        - expression AS alias
        - expression AS "alias"
        - expression AS 'alias'

        解析逻辑:
        1. 遍历字符串，使用深度计数器跟踪括号嵌套
        2. 在最外层（depth=0）查找 " AS " 模式
        3. 记录最后一个匹配的位置（支持多个 AS 的情况）
        4. 分离原始表达式和别名
        5. 过滤掉类型关键字作为别名的情况

        过滤规则:
        - 别名为 SQL 类型关键字时，不视为别名

        示例:
        "name AS user_name" → ("name", "user_name")
        "upper(name)" → ("upper(name)", "upper(name)")
        "cast(age AS INT)" → ("cast(age AS INT)", "cast(age AS INT)") （INT 是类型关键字）
        """
        depth = 0
        last_as_pos = -1
        upper = expr_str.upper()
        i = 0

        # 遍历字符串查找 AS 关键字（只在最外层查找）
        while i < len(expr_str):
            c = expr_str[i]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif depth == 0 and upper[i:i + 4] == " AS " and (i + 4 <= len(expr_str)):
                last_as_pos = i
            i += 1

        # 如果找到有效的 AS 关键字
        if last_as_pos >= 0:
            original = expr_str[:last_as_pos].strip()
            alias = expr_str[last_as_pos + 4:].strip()

            # 检查别名是否为类型关键字
            if alias and not alias.upper() in (
                    "INT", "BIGINT", "VARCHAR", "DOUBLE", "FLOAT",
                    "BOOLEAN", "DATE", "TIMESTAMP", "DECIMAL",
                    "STRING", "LONG", "SHORT", "BYTE", "CHAR",
                    "TINYINT", "SMALLINT", "BINARY", "VARBINARY",
                    "ARRAY", "MAP", "ROW", "MULTISET",
            ):
                return original, alias

        # 没有找到有效的别名，返回原始表达式
        return expr_str, expr_str

    def expand_row_type(self, field_info, parent_name=None):
        expanded = []

        if isinstance(field_info, str):
            field_info = {
                "field_name": field_info,
                "field_type": "ROW",
                "nested_fields": []
            }

        field_name = field_info.get("field_name", "")
        field_type = field_info.get("field_type", "")
        nested_fields = field_info.get("nested_fields", [])

        if parent_name:
            full_name = f"{parent_name}.{field_name}"
        else:
            full_name = field_name

        if field_type == "ROW" and nested_fields:
            for nested_field in nested_fields:
                nested_name = nested_field.get("field_name", "")
                nested_type = nested_field.get("field_type", "")

                if nested_type == "ROW" and nested_field.get("nested_fields"):
                    expanded.extend(
                        self.expand_row_type(nested_field, full_name)
                    )
                else:
                    expanded.append({
                        "field_name": f"{full_name}.{nested_name}",
                        "field_type": nested_type
                    })
        else:
            expanded.append({
                "field_name": full_name,
                "field_type": field_type
            })

        return expanded

    @staticmethod
    def expand_schema_if_needed(schema):
        if not schema:
            return []

        expanded = []
        for field in schema:
            field_type = field.get("field_type", "")
            nested_fields = field.get("nested_fields", [])

            if field_type == "ROW" and nested_fields:
                expanded.extend(
                    FlinkTypeResolver._expand_field_recursive(field, "")
                )
            else:
                expanded.append({
                    "field_name": field.get("field_name", ""),
                    "field_type": field_type
                })

        return expanded

    @staticmethod
    def _expand_field_recursive(field, parent_name):
        expanded = []
        field_name = field.get("field_name", "")
        field_type = field.get("field_type", "")
        nested_fields = field.get("nested_fields", [])

        if parent_name:
            full_name = f"{parent_name}.{field_name}"
        else:
            full_name = field_name

        if field_type == "ROW" and nested_fields:
            for nested_field in nested_fields:
                nested_type = nested_field.get("field_type", "")

                if nested_type == "ROW" and nested_field.get("nested_fields"):
                    expanded.extend(
                        FlinkTypeResolver._expand_field_recursive(nested_field, full_name)
                    )
                else:
                    expanded.append({
                        "field_name": f"{full_name}.{nested_field.get('field_name', '')}",
                        "field_type": nested_type
                    })
        else:
            expanded.append({
                "field_name": full_name,
                "field_type": field_type
            })

        return expanded

    def extract_table_source_info(self, description_data):
        tables = []
        output_schema = []
        for desc in description_data:
            if isinstance(desc, dict):
                origin = desc.get("originDescription", "")
                output_names = desc.get("outputNames", [])
                output_types = desc.get("outputTypes", [])
                if output_names and output_types:
                    for name, type_str in zip(output_names, output_types):
                        schema_entry = {
                            "field_name": name,
                            "field_type": TypeNormalizer.normalize_type(type_str),
                        }
                        # ★ 修改点：从 table_schema 中查找原始类型（保留 ROW 等复杂类型的完整定义）
                        original_type = self._find_original_type_from_table_schema(name)
                        if original_type:
                            schema_entry["original_type"] = original_type
                        output_schema.append(schema_entry)
                table_name = self._extract_table_name_from_origin(origin)
                if table_name:
                    tables.append(table_name)
                if not output_schema:
                    input_types = desc.get("inputTypes", [])
                    out_types = desc.get("outputTypes", [])
                    if out_types and not output_names:
                        for i, type_str in enumerate(out_types):
                            output_schema.append({
                                "field_name": f"field_{i}",
                                "field_type": TypeNormalizer.normalize_type(type_str),
                            })
            elif isinstance(desc, str):
                table_name = self._extract_table_name_from_text(desc)
                if table_name:
                    tables.append(table_name)
                if not output_schema:
                    fields = self._extract_fields_from_text(desc)
                    if fields:
                        output_schema = fields
        return tables, output_schema

    def _find_original_type_from_table_schema(self, field_name):
        """从 table_schema 中查找字段的原始类型"""
        if not field_name or not self.table_schema:
            return None
        name_lower = field_name.lower()
        for table_name, columns in self.table_schema.items():
            for col_info in columns:
                if col_info["field_name"].lower() == name_lower:
                    original = col_info.get("original_type", "")
                    if original and original.upper().startswith("ROW"):
                        return original
        return None