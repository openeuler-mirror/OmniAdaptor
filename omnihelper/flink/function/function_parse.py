"""
   Flink 函数解析模块

   Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
   You can use this software according to the terms and conditions of the Mulan PSL v2.
   You may obtain a copy of Mulan PSL v2 at:
              http://license.coscl.org.cn/MulanPSL2
   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
   See the Mulan PSL v2 for more details.

模块功能说明:
    本模块负责从 Flink 物理计划描述中解析函数调用信息，支持三种类型的函数识别:
    1. 函数调用形式: func_name(...) - 如 UPPER(col), CAST(... AS INT)
    2. 关键字形式: 关键字如 AND, OR, IS NULL, LIKE 等
    3. 操作符形式: 符号如 =, <>, >=, <=, *, + 等

    核心功能包括:
    - 加载和维护 Flink 函数字典 (flink_function_dictionary.json)
    - 构建正则表达式模式用于函数匹配
    - 解析算子描述中的函数调用
    - 判断函数是否支持以及参数类型是否兼容

设计理念:
    采用策略模式将函数匹配分为三个独立的正则模式，便于扩展和维护。
    通过类型归一化和函数支持映射，实现对函数兼容性的快速判断。
"""

import re
import os
import json

from omnihelper.flink.schema.type_normalizer import TypeNormalizer
from omnihelper.util.common_util import CommonUtil
from omnihelper.util.log import logger


class FlinkFunctionParser:
    """
    Flink 函数解析器 - 核心类

    设计职责:
    1. 从配置文件加载 Flink 函数字典，建立函数元数据映射
    2. 构建三类正则表达式模式 (函数调用、关键字、操作符)
    3. 解析描述文本中的函数并识别不支持的函数
    4. 基于参数类型判断函数是否在给定场景下可用

    成员变量详解:
    - function_list: list，从字典文件加载的原始函数列表，每个元素是包含函数元数据的字典
    - omni_functions: list，函数名小写形式列表，用于构建匹配模式
    - func_pattern: re.Pattern 或 None，编译后的函数调用正则表达式，匹配 func_name(
    - keywords_pattern: re.Pattern 或 None，编译后的关键字正则表达式，使用单词边界
    - operator_pattern: re.Pattern 或 None，编译后的操作符正则表达式，无边界限制
    - func_support_map: dict，函数名(小写) -> 是否支持(bool)的映射
    - func_is_supported_types: dict，函数名(小写) -> 支持的类型列表的映射

    架构设计:
        本类采用初始化时一次性加载配置的策略，避免运行时重复读取文件。
        正则表达式模式在初始化阶段编译完成，提高匹配性能。
    """

    def __init__(self):
        """
        初始化函数解析器

        初始化流程:
        1. 初始化所有实例变量为空容器
        2. 调用 load_func_list() 加载函数字典并构建匹配模式

        变量初始化说明:
        - function_list: 存储原始函数配置数据
        - omni_functions: 存储小写函数名，用于模式构建
        - *_pattern: 正则模式初始化为 None，由 build_patterns() 填充
        - func_support_map/func_is_supported_types: 用于快速查找函数支持信息
        """
        # 存储原始函数配置列表
        self.function_list = []
        # 存储小写函数名，用于构建正则模式
        self.omni_functions = []
        # 函数调用匹配模式
        self.func_pattern = None
        # 关键字匹配模式
        self.keywords_pattern = None
        # 操作符匹配模式
        self.operator_pattern = None
        # 函数支持状态映射
        self.func_support_map = {}
        # 函数支持类型列表映射
        self.func_is_supported_types = {}
        # 加载配置并构建模式
        self.load_func_list()

    def load_func_list(self):
        """
        加载函数字典配置并构建匹配模式

        实现逻辑:
        1. 确定配置文件路径: resources/flink_function_dictionary.json
        2. 检查文件是否存在，不存在则记录警告并返回
        3. 读取并解析 JSON 配置文件
        4. 遍历函数列表，提取关键元数据:
           - 函数名小写形式 (用于匹配)
           - is_support_func: 是否支持的标志
           - is_supported_type: 支持的参数类型列表
        5. 调用 build_patterns() 构建正则表达式匹配模式

        错误处理策略:
        - 文件不存在: 记录警告，所有模式设为 None，后续匹配将被跳过
        - JSON 解析失败: 记录警告，清空所有数据，避免使用损坏的配置

        配置文件格式要求:
            [
                {
                    "func_name": "UPPER",
                    "is_support_func": true,
                    "is_supported_type": ["VARCHAR", "CHAR"]
                },
                ...
            ]
        """
        # 获取资源文件目录路径
        base_path = os.path.join(CommonUtil.get_execute_path(), "resources")
        dictionary_path = os.path.join(base_path, "flink_function_dictionary.json")

        # 文件不存在的处理
        if not os.path.exists(dictionary_path):
            logger.warning(f"Flink function dictionary not found: {dictionary_path}")
            # 重置所有状态
            self.function_list = []
            self.omni_functions = []
            self.func_support_map = {}
            self.func_is_supported_types = {}
            self.func_pattern = None
            self.keywords_pattern = None
            self.operator_pattern = None
            return

        try:
            # 读取并解析 JSON 文件
            with open(dictionary_path, "r", encoding="utf-8") as f:
                self.function_list = json.load(f)
            logger.info(f"Loaded {len(self.function_list)} functions from {dictionary_path}")
        except Exception as e:
            logger.warning(f"Failed to load {dictionary_path}: {e}")
            # 重置所有状态，防止使用损坏的数据
            self.function_list = []
            self.omni_functions = []
            self.func_support_map = {}
            self.func_is_supported_types = {}
            self.func_pattern = None
            self.keywords_pattern = None
            self.operator_pattern = None
            return

        # 初始化映射容器
        self.omni_functions = []
        self.func_support_map = {}
        self.func_is_supported_types = {}

        # 遍历函数列表，构建映射关系
        for func in self.function_list:
            func_name = func.get("func_name")
            if not func_name:
                continue  # 跳过无效条目
            # 统一转为小写，确保匹配时不区分大小写
            func_name_lower = func_name.lower()
            self.omni_functions.append(func_name_lower)
            # 构建支持状态映射
            self.func_support_map[func_name_lower] = func.get("is_support_func", False)
            # 构建支持类型列表映射
            self.func_is_supported_types[func_name_lower] = func.get("is_supported_type", [])

        # 构建正则匹配模式
        self.build_patterns()

    def build_patterns(self):
        """
        构建三种正则表达式匹配模式

        分类规则(优先级从高到低):
        1. 包含空格的函数名归类为关键字 (如 "IS NULL", "NOT BETWEEN")
           - 原因: 多词组成的操作符，需要完整匹配
        2. 长度<=2且不含字母的归类为操作符 (如 "=", "<", ">")
           - 原因: 符号操作符不需要单词边界保护
        3. 在预定义关键字列表中的归类为关键字
           - 原因: 这些词在 SQL 中有特殊语义
        4. 其余归类为函数调用
           - 需要匹配 func_name( 形式

        预定义关键字列表包含:
        - 逻辑关键字: or, and
        - SQL 关键字: like, between, not, row, date, time, interval
        - 时间函数: localtime, localtimestamp, current_time, current_date, current_timestamp
        - 集合类型: array, map

        匹配模式说明:
        - func_pattern: 使用 \b 单词边界 + 函数名 + \s*\( 匹配函数调用
          例如: r"\b(upper|lower)\s*\(" 匹配 upper( 或 lower(
        - keywords_pattern: 使用 \b 单词边界确保完整单词匹配
          例如: r"\b(or|and)\b" 避免匹配到 order 中的 or
        - operator_pattern: 不使用边界，允许在任何位置匹配操作符
          例如: r"(=|<|>)" 匹配任何位置的等号

        算法优化:
        - 操作符按长度降序排序，确保长操作符优先匹配 (如 >= 优先于 >)
        - 使用 re.escape() 转义特殊字符，避免正则语法冲突
        - 使用 re.I 标志实现不区分大小写匹配
        """
        if not self.omni_functions:
            logger.warning("No functions loaded, patterns are None")
            self.func_pattern = None
            self.keywords_pattern = None
            self.operator_pattern = None
            return

        # 预定义关键字集合 - 这些词需要用单词边界保护
        keyword_keywords = {
             'or', 'and', 'like', 'between', 'not', 'row', 'date', 'time', 'interval', 'localtime',
              'localtimestamp', 'current_time', 'current_date', 'current_timestamp', 'array', 'map'
        }

        # 三类模式的候选列表
        func_call_patterns = []      # 函数调用模式候选
        keyword_patterns = []        # 关键字模式候选
        operator_patterns = []       # 操作符模式候选

        # 遍历所有函数，进行分类
        for func in self.omni_functions:
            is_keyword = False

            # 规则1: 包含空格的函数名归类为关键字
            if ' ' in func:
                is_keyword = True
            # 规则2: 长度<=2且不含字母的归类为操作符
            elif len(func) <= 2 and not any(c.isalpha() for c in func):
                operator_patterns.append(func)
                continue  # 跳过后续判断
            # 规则3: 在预定义关键字列表中
            elif func.lower() in keyword_keywords:
                is_keyword = True

            # 根据分类添加到对应列表
            if is_keyword:
                keyword_patterns.append(func)
            else:
                func_call_patterns.append(func)

        # 构建函数调用模式
        if func_call_patterns:
            # 转义特殊字符并构建正则表达式
            escaped_funcs = [re.escape(f) for f in func_call_patterns]
            self.func_pattern = re.compile(
                r"\b({})\s*\(".format("|".join(escaped_funcs)),
                re.I  # 不区分大小写
            )
            logger.info(f"Created function call pattern for {len(func_call_patterns)} functions")
        else:
            self.func_pattern = None

        # 构建关键字模式
        if keyword_patterns:
            escaped_keywords = [re.escape(f) for f in keyword_patterns]
            self.keywords_pattern = re.compile(
                r"\b({})\b".format("|".join(escaped_keywords)),
                re.I
            )
            logger.info(f"Created keywords pattern for {len(keyword_patterns)} keywords")
        else:
            self.keywords_pattern = None

        # 构建操作符模式
        if operator_patterns:
            # 关键优化: 按长度降序排序，确保长操作符优先匹配
            # 例如: >= 必须在 > 之前匹配，否则会被拆分为 > 和 =
            sorted_ops = sorted(operator_patterns, key=len, reverse=True)
            escaped_ops = [re.escape(f) for f in sorted_ops]
            self.operator_pattern = re.compile(
                r"({})".format("|".join(escaped_ops))
            )
            logger.info(f"Created operator pattern for {len(operator_patterns)} operators: {operator_patterns}")
        else:
            self.operator_pattern = None

    def is_func_type_supported(self, func_name, param_types):
        """
        判断函数在给定参数类型下是否支持

        参数说明:
        :param func_name: str，函数名 (不区分大小写)
        :param param_types: list，参数类型列表，如 ["INT", "VARCHAR"]

        返回值说明:
        :return: tuple，(is_supported, unsupported_types)
            - is_supported: bool，表示函数是否支持
            - unsupported_types: list，不支持的类型列表，为空表示全部支持

        判断逻辑(顺序执行):
        1. 函数名不在字典中: 返回 (False, [])
           - 原因: 未知函数无法判断支持性
        2. is_support_func=False: 返回 (False, [])
           - 原因: 函数本身不被支持
        3. is_supported_type 为空列表: 返回 (True, [])
           - 原因: 表示无类型限制，任意类型都支持
        4. 遍历参数类型，标准化后检查是否在支持列表中
           - 使用 TypeNormalizer.normalize_type() 统一类型格式
        5. 收集不在支持列表中的类型作为 unsupported_types

        设计原理:
            通过类型归一化和集合查找，实现 O(n) 的时间复杂度判断。
            支持类型列表为空表示函数对参数类型没有限制。
        """
        # 统一转为小写，确保匹配时不区分大小写
        func_name_lower = func_name.lower() if func_name else ""

        # 规则1: 函数名不在字典中
        if func_name_lower not in self.func_support_map:
            return False, []

        # 规则2: 函数本身不被支持
        if not self.func_support_map[func_name_lower]:
            return False, []

        # 获取支持的类型列表
        is_supported_list = self.func_is_supported_types.get(func_name_lower, [])

        # 规则3: 无类型限制
        if not is_supported_list:
            return True, []

        # 规则4: 检查每个参数类型
        unsupported_found = []
        for param_type in (param_types or []):
            # 类型归一化，消除格式差异
            normalized = TypeNormalizer.normalize_type(param_type)
            if normalized not in is_supported_list:
                unsupported_found.append(normalized)

        # 规则5: 返回结果
        if unsupported_found:
            return False, unsupported_found

        return True, []

    @staticmethod
    def _strip_html_tags(description):
        """
        去除 HTML 标签及其周围的空白和分隔符

        参数说明:
        :param description: str，包含 HTML 标签的描述文本
        :return: str，清理后的文本

        实现原理:
        使用正则表达式 r'<[^>]+>[\s:*\+\-]*' 匹配:
        - <[^>]+>: HTML 标签内容，从 < 开始到 > 结束
        - [\s:*\+\-]*: 标签后的可选空白字符和分隔符 (:, *, +, -)
        替换为空格，保留文本内容结构

        应用场景:
        Flink 算子描述中可能包含 HTML 标签，需要清理后再进行函数匹配。
        例如: "<br/>" 或 "<b>CAST</b>" 需要转换为空格或纯文本。
        """
        if not description:
            return description
        # 正则替换: HTML标签及后续分隔符 → 空格
        cleaned = re.sub(r'<[^>]+>[\s:*\+\-]*', ' ', description)
        return cleaned

    @staticmethod
    def _is_operator_false_positive(op, match, description):
        """
        判断操作符匹配是否为误报

        参数说明:
        :param op: str，操作符字符，如 "=", "<", ">"
        :param match: re.Match，正则匹配对象，包含匹配位置信息
        :param description: str，完整描述文本
        :return: bool，True 表示误报，False 表示有效匹配

        误报场景分析:
        1. "=" 后紧跟 "[" - 这是数组索引操作而非比较操作符
           - 例如: array[0]=value 中的 [ 前的 = 不是比较
        2. "=" 前是字母 - 可能是赋值语句或属性设置
           - 例如: name=value 中的 = 是赋值而非比较
        3. "=", "<", ">" 与相邻操作符组合 (如 "<=", ">=", "<>")
           - 例如: >= 中的 > 不应该单独匹配
        4. "*" 被括号包围 "(*)" - 可能是通配符而非乘法
           - 例如: COUNT(*) 中的 * 是通配符而非乘法操作符

        边界处理:
        - 使用 match.start() 和 match.end() 获取匹配位置
        - 检查边界字符时需要确保不越界 (pos > 0, op_end < len(description))
        """
        pos = match.start()      # 匹配开始位置
        op_end = match.end()     # 匹配结束位置

        # 场景1: "=" 后紧跟 "[" - 数组索引操作
        if op == '=' and op_end < len(description) and description[op_end] == '[':
            return True

        # 场景2: "=" 前是字母 - 赋值语句
        if op == '=' and pos > 0 and description[pos - 1].isalpha():
            return True

        # 场景3: 操作符与相邻符号组合
        if op in ('=', '<', '>') and pos > 0 and description[pos - 1] in ('<', '>', '!'):
            return True
        if op in ('=', '<', '>') and op_end < len(description) and description[op_end] == '=':
            return True

        # 场景4: "*" 被括号包围
        if op == '*' and pos > 0 and description[pos - 1] == '(' and op_end < len(description) and description[op_end] == ')':
            return True

        # 非误报
        return False

    def _is_operator_form(self, description, match_start):
        """
        判断函数名位置是否在操作符描述行中

        参数说明:
        :param description: str，描述文本
        :param match_start: int，函数名匹配的起始位置
        :return: bool，True 表示是操作符描述行，False 表示普通函数调用

        实现原理:
        Flink 物理计划中，每个算子有一个标识行，格式为 "[数字]:算子类型"
        例如: "[1]:Calc", "[2]:GroupAggregate"

        检查匹配位置前 10 个字符内是否存在 "[数字]:字母" 模式，
        如果存在，则说明匹配位置在算子标识行中，不是真正的函数调用。

        设计原因:
        算子标识行中的类型名 (如 Calc, GroupAggregate) 可能与函数字典中的函数名冲突，
        需要过滤掉这些误匹配。
        """
        # 边界处理: 匹配位置太靠前，不可能在标识行中
        if match_start <= 3:
            return False

        # 取匹配位置前 10 个字符进行检查
        prev_part = description[max(0, match_start - 10):match_start]

        # 匹配 "[数字]:字母" 模式
        op_pattern = re.compile(r'\[\d+\]:[A-Za-z]*$')
        return bool(op_pattern.search(prev_part))

    def parse_plan_description(self, description):
        """
        解析计划描述中的所有函数

        参数说明:
        :param description: str，Flink 算子描述文本
        :return: list，函数信息列表，每项包含:
            - func: str，函数/关键字/操作符名称 (小写)
            - params: list，参数列表 (空列表，由后续处理填充)
            - type: str，类型标识 ("func", "keyword", "operator")

        解析流程:
        1. 使用 func_pattern 匹配函数调用形式
           - 过滤掉算子标识行中的误匹配
        2. 使用 keywords_pattern 匹配关键字形式
           - 直接匹配，不需要额外过滤
        3. 使用 operator_pattern 匹配操作符形式
           - 需要先去除 HTML 标签
           - 需要过滤误报场景

        设计考虑:
        - 三类模式独立匹配，结果合并为一个列表
        - 每个匹配项标记类型，便于后续处理
        """
        if not description:
            return []

        # 存储解析结果
        funcs = []

        # 步骤1: 匹配函数调用形式 func(...)
        if self.func_pattern:
            for match in self.func_pattern.finditer(description):
                func_name = match.group(1).lower()
                match_start = match.start()
                # 过滤算子标识行中的误匹配
                if self._is_operator_form(description, match_start):
                    continue
                funcs.append({"func": func_name, "params": [], "type": "func"})

        # 步骤2: 匹配关键字形式
        if self.keywords_pattern:
            for match in self.keywords_pattern.finditer(description):
                keyword = match.group(1).lower()
                funcs.append({"func": keyword, "params": [], "type": "keyword"})

        # 步骤3: 匹配操作符形式
        if self.operator_pattern:
            # 先清理 HTML 标签
            clean_desc = self._strip_html_tags(description)
            for match in self.operator_pattern.finditer(clean_desc):
                op = match.group(1).lower()
                # 过滤误报场景
                if self._is_operator_false_positive(op, match, clean_desc):
                    continue
                funcs.append({"func": op, "params": [], "type": "operator"})

        return funcs

    def analyze_unsupported_functions(self, description, param_types_map=None):
        """
        分析描述中的不支持函数

        参数说明:
        :param description: str，算子描述字符串
        :param param_types_map: dict 或 None，函数名→参数类型列表的映射
                               如 {"upper": ["VARCHAR"], "cast": ["INT", "VARCHAR"]}
        :return: list，不支持的函数列表，每项包含:
            - func_name: str，函数名称 (小写)
            - times: int，出现次数
            - unsupported_types: list，(可选) 不支持的类型列表

        分析规则(优先级):
        1. 函数不在字典中: 跳过，不计入结果
           - 原因: 无法判断支持性的函数不报告
        2. is_support_func=False: 计入不支持列表
           - 原因: 函数本身不被支持
        3. is_support_func=True 但有类型限制:
           - 提供参数类型: 检查是否全部支持，不支持则计入
           - 未提供参数类型: 视为支持，不计入

        计数策略:
        - 使用 (函数名, 参数类型元组) 作为计数器的键
        - 相同函数不同参数类型视为不同条目
        - 例如: upper(VARCHAR) 和 upper(INT) 分别计数

        调试支持:
        - 收集所有匹配结果用于日志输出
        - 便于排查匹配问题
        """
        if not description:
            return []

        # 计数器: {(func_name, types_tuple): count}
        func_counter = {}
        # 记录不支持的类型: {(func_name, types_tuple): [unsupported_types]}
        func_unsupported_types = {}

        # 调试用: 收集所有匹配结果
        all_func_matches = []
        all_keyword_matches = []
        all_operator_matches = []

        # 步骤1: 匹配函数调用
        if self.func_pattern:
            for match in self.func_pattern.finditer(description):
                func_name = match.group(1).lower()
                match_start = match.start()
                if self._is_operator_form(description, match_start):
                    continue
                all_func_matches.append(match)
                # 检查支持性并更新计数器
                self._check_func_support(
                    func_name, func_counter, func_unsupported_types, param_types_map
                )

        # 步骤2: 匹配关键字
        if self.keywords_pattern:
            for match in self.keywords_pattern.finditer(description):
                all_keyword_matches.append(match)
                keyword = match.group(1).lower()
                self._check_func_support(
                    keyword, func_counter, func_unsupported_types, param_types_map
                )

        # 步骤3: 匹配操作符
        clean_desc = self._strip_html_tags(description) if self.operator_pattern else description
        if self.operator_pattern:
            for match in self.operator_pattern.finditer(clean_desc):
                op = match.group(1).lower()
                if self._is_operator_false_positive(op, match, clean_desc):
                    continue
                all_operator_matches.append(match)
                self._check_func_support(
                    op, func_counter, func_unsupported_types, param_types_map
                )

        # 调试日志: 输出所有匹配的函数
        all_matches = all_func_matches + all_keyword_matches + all_operator_matches
        if all_matches:
            matched_funcs = [m.group(1).lower() for m in all_matches]
            logger.info(f"从description解析到的所有函数: {matched_funcs}")

        # 转换为标准输出格式
        results = []
        for key, times in func_counter.items():
            # 提取函数名
            func_name = key[0] if isinstance(key, tuple) else key
            result = {
                "func_name": func_name,
                "times": times
            }
            # 添加不支持的类型列表
            unsupported = func_unsupported_types.get(key)
            if unsupported:
                result["unsupported_types"] = unsupported
            results.append(result)

        return results

    def _check_func_support(self, func_name, func_counter, func_unsupported_types, param_types_map):
        """
        检查单个函数是否支持，并更新计数器

        参数说明:
        :param func_name: str，函数名 (小写)
        :param func_counter: dict，函数出现次数计数器，键格式为 (函数名, 参数类型元组)
        :param func_unsupported_types: dict，函数不支持类型记录
        :param param_types_map: dict，函数名→参数类型列表的映射

        内部逻辑(顺序执行):
        1. 函数不在支持映射中: 直接返回
           - 原因: 无法判断支持性，跳过
        2. 获取函数参数类型，构建计数器键
           - 如果提供了 param_types_map，获取对应参数类型
           - 否则使用空元组作为类型标识
        3. is_support_func=False: 计数器+1，返回
           - 原因: 函数本身不被支持
        4. 获取支持类型列表，为空则视为支持，返回
           - 原因: 无类型限制的函数默认支持
        5. 调用 is_func_type_supported 检查类型支持情况
           - 如果不支持，更新计数器和不支持类型记录

        计数器键设计:
        - 使用 (func_name, types_tuple) 作为键
        - types_tuple = tuple(param_types) 或 ()
        - 这样可以区分同一函数不同参数类型的情况
        """
        # 规则1: 函数不在支持映射中
        if func_name not in self.func_support_map:
            return

        # 获取参数类型
        param_types = None
        if param_types_map:
            param_types = param_types_map.get(func_name)

        # 构建计数器键: (函数名, 参数类型元组)
        types_key = tuple(param_types) if param_types else ()
        counter_key = (func_name, types_key)

        # 规则2: 函数本身不被支持
        if not self.func_support_map[func_name]:
            func_counter[counter_key] = func_counter.get(counter_key, 0) + 1
            return

        # 获取支持类型列表
        is_supported_list = self.func_is_supported_types.get(func_name, [])

        # 规则3: 无类型限制
        if not is_supported_list:
            logger.debug(f"函数 '{func_name}' 被过滤（is_support_func=True，无类型限制）")
            return

        # 规则4: 有类型限制，检查参数类型
        if param_types:
            is_supported, unsupported = self.is_func_type_supported(func_name, param_types)
            if is_supported:
                logger.debug(f"函数 '{func_name}' 被过滤（is_support_func=True，参数类型均支持）")
            else:
                func_counter[counter_key] = func_counter.get(counter_key, 0) + 1
                func_unsupported_types[counter_key] = unsupported
        else:
            # 有类型限制但未提供参数类型，视为支持
            logger.debug(f"函数 '{func_name}' is_support_func=True，有类型限制但未提供参数类型，视为支持")