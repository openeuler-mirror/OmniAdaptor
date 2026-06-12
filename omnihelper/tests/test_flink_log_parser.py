"""
   Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
   You can use this software according to the terms and conditions of the Mulan PSL v2.
   You may obtain a copy of Mulan PSL v2 at:
            http://license.coscl.org.cn/MulanPSL2
   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
   See the Mulan PSL v2 for more details.
"""

import os
import argparse
import shutil
import unittest

from omnihelper.flink_log_parser import FlinkLogParser


class TestFlinkLogParser(unittest.TestCase):
    """测试 FlinkLogParser 类的测试用例"""

    def setUp(self):
        """初始化测试环境"""
        self.test_data = [
            {
                'jobid': 'job-123',
                'taskid': 'task-456',
                '状态': 'RUNNING',
                '算子名称': 'Map',
                'Input': 'test_input_1',
                'Output': 'test_output_1',
                '出现频次': 5,
                '运行时间(s)': 1.23,
                '输入数据量': '100MB',
                '输出数据量': '50MB',
                '表达式/内置函数名称': 'SUBSTRING',
                '表达式Input': 'col1, 1, 5',
                '嵌套内容': 'SUBSTRING(col1, 1, 5)',
                '表达式出现频次': 3
            },
            {
                'jobid': 'job-123',
                'taskid': 'task-456',
                '状态': 'RUNNING',
                '算子名称': 'Filter',
                'Input': 'test_input_1',
                'Output': 'test_output_2',
                '出现频次': 3,
                '运行时间(s)': 0.89,
                '输入数据量': '50MB',
                '输出数据量': '30MB',
                '表达式/内置函数名称': 'WHERE',
                '表达式Input': 'col2 > 10',
                '嵌套内容': 'WHERE col2 > 10',
                '表达式出现频次': 2
            },
            {
                'jobid': 'job-123',
                'taskid': 'task-789',
                '状态': 'FINISHED',
                '算子名称': 'Reduce',
                'Input': 'test_output_2',
                'Output': 'test_output_3',
                '出现频次': 2,
                '运行时间(s)': 2.45,
                '输入数据量': '30MB',
                '输出数据量': '10MB',
                '表达式/内置函数名称': 'SUM',
                '表达式Input': 'col3',
                '嵌套内容': 'SUM(col3)',
                '表达式出现频次': 1
            },
            {
                'jobid': 'job-456',
                'taskid': 'task-123',
                '状态': 'RUNNING',
                '算子名称': 'Join',
                'Input': 'test_input_2, test_input_3',
                'Output': 'test_output_4',
                '出现频次': 1,
                '运行时间(s)': 3.67,
                '输入数据量': '200MB',
                '输出数据量': '150MB',
                '表达式/内置函数名称': '',
                '表达式Input': '',
                '嵌套内容': '',
                '表达式出现频次': ''
            },
            {
                'jobid': 'job-456',
                'taskid': 'task-123',
                '状态': 'RUNNING',
                '算子名称': '',
                'Input': '',
                'Output': '',
                '出现频次': '',
                '运行时间(s)': '',
                '输入数据量': '',
                '输出数据量': '',
                '表达式/内置函数名称': 'CONCAT',
                '表达式Input': 'col1, col2',
                '嵌套内容': 'CONCAT(col1, col2)',
                '表达式出现频次': 4
            }
        ]

        self.args = argparse.Namespace(
            url='http://127.0.0.1:8081',
            interval=1000,
            timeout=30,
            no_ssl_verify=False,
            ssl_verify=True,
            output_dir=None,
            show_op_details=True,
            jobid=None,
            header=None,
            kerberos=False,
            kerberos_mutual_auth='OPTIONAL',
            input_data=None,
        )
        self.output_dir = "./tmp_output"
        self.args.output_dir = self.output_dir

    def tearDown(self):
        """清理测试环境"""
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

    def test_flink_report_generation(self):
        """测试 Flink 报告生成功能"""
        flink_parser = FlinkLogParser(self.args)
        flink_parser.analysis_result = self.test_data
        flink_parser.generate_report()

        output_files = os.listdir(self.output_dir)
        self.assertGreater(len(output_files), 0, "No report file generated")

    def test_window_join_exec_plan_parsing(self):
        """测试 WindowJoin 执行计划解析：验证算子提取、类型解析、递归死循环修复"""
        from omnihelper.flink.operator.op_parse import FlinkParser

        column_type = {
            'event_type': 'INT', 'person': 'ROW', 'auction': 'ROW', 'bid': 'ROW',
            'dateTime': 'TIMESTAMP',
            'order_id': 'BIGINT', 'user_id': 'BIGINT', 'amount': 'DECIMAL',
            'pay_amount': 'DECIMAL', 'order_id0': 'BIGINT',
            'o_window_start': 'TIMESTAMP', 'o_window_end': 'TIMESTAMP',
            'p_window_start': 'TIMESTAMP', 'p_window_end': 'TIMESTAMP',
        }
        parser = FlinkParser(None, column_type, None)

        description = (
            "[1]:TableSourceScan(table=[[default_catalog, default_database, datagen]], "
            "fields=[event_type, person, auction, bid])"
            "<br/>+- "
            "[2]:Calc(select=[event_type, bid, CASE((event_type = 0), person.dateTime, "
            "(event_type = 1), auction.dateTime, bid.dateTime) AS dateTime])"
            "<br/>   +- "
            "[3]:WatermarkAssigner(rowtime=[dateTime], watermark=[(dateTime - 4000:INTERVAL SECOND)])"
            "<br/>      +- "
            "[4]:Calc(select=[dateTime, bid.auction AS $1, bid.bidder AS $2, bid.price AS $3, "
            "bid.channel AS $4, bid.url AS $5, bid.extra AS $6], where=[(event_type = 2)])"
            "<br/> \n"
            "[6]:Rank(strategy=[AppendFastStrategy], rankType=[ROW_NUMBER], "
            "rankRange=[rankStart=1, rankEnd=10], partitionBy=[$1], orderBy=[$3 DESC], "
            "select=[dateTime, $1, $2, $3, $4, $5, $6, rank_number])"
            "<br/>+- "
            "[7]:Calc(select=[$1 AS auction, $2 AS bidder, $3 AS price, $4 AS channel, "
            "$5 AS url, dateTime, $6 AS extra, rank_number])"
            "<br/>   +- "
            "[8]:Sink(table=[default_catalog.default_database.nexmark_q19], "
            "fields=[auction, bidder, price, channel, url, dateTime, extra, rank_number])"
            "<br/>"
        )

        import re
        raw_parts = re.split(r"<br/>[\s:*\+\-]*|\n", description)
        description_data = [
            parsed_line for line in raw_parts
            if (parsed_line := FlinkParser.parse_single_description_line(line)) is not None
        ]

        schema_chain = parser._build_schema_chain_for_vertex(description_data)
        self.assertGreater(len(schema_chain), 0, "schema_chain should not be empty")

        op_types = [entry.get("op_type") for entry in schema_chain]
        self.assertIn("TableSourceScan", op_types, "TableSourceScan should be in schema_chain")
        self.assertIn("Calc", op_types, "Calc should be in schema_chain")

        source_entry = next(e for e in schema_chain if e.get("op_type") == "TableSourceScan")
        self.assertGreater(len(source_entry.get("output_schema", [])), 0,
                           "TableSourceScan should have output_schema")

        ops = parser._build_ops_from_schema_chain(schema_chain, {})
        self.assertGreater(len(ops), 0, "ops should not be empty")

        source_op = next((op for op in ops if op["op_type"] == "TableSourceScan"), None)
        self.assertIsNotNone(source_op, "TableSourceScan should be in ops")
        self.assertNotEqual(source_op["input_types_str"], "",
                            "TableSourceScan input should not be empty")
        self.assertEqual(source_op["output_types_str"], "",
                         "TableSourceScan output should be empty")

    def test_window_join_recursion_fix(self):
        """测试 WindowJoin 执行计划不触发递归死循环"""
        from omnihelper.flink.operator.op_parse import FlinkParser

        column_type = {
            'order_id': 'BIGINT', 'user_id': 'BIGINT', 'amount': 'DECIMAL',
            'pay_amount': 'DECIMAL', 'order_id0': 'BIGINT',
            'o_window_start': 'TIMESTAMP', 'o_window_end': 'TIMESTAMP',
            'p_window_start': 'TIMESTAMP', 'p_window_end': 'TIMESTAMP',
        }
        parser = FlinkParser(None, column_type, None)

        description = (
            "[165]:WindowJoin(leftWindow=[TUMBLE(win_start=[o_window_start], "
            "win_end=[o_window_end], size=[1 min])], "
            "rightWindow=[TUMBLE(win_start=[p_window_start], win_end=[p_window_end], "
            "size=[1 min])], joinType=[InnerJoin], where=[(order_id = order_id0)], "
            "select=[o_window_start, o_window_end, order_id, user_id, amount, "
            "p_window_start, p_window_end, order_id0, pay_amount])"
            "<br/>+- "
            "[166]:Calc(select=[o_window_start AS window_start, o_window_end AS window_end, "
            "order_id, user_id, amount, pay_amount])"
            "<br/>   +- "
            "[167]:Sink(table=[default_catalog.default_database.window_join_print], "
            "fields=[window_start, window_end, order_id, user_id, amount, pay_amount])"
            "<br/>"
        )

        import re
        raw_parts = re.split(r"<br/>[\s:*\+\-]*|\n", description)
        description_data = [
            parsed_line for line in raw_parts
            if (parsed_line := FlinkParser.parse_single_description_line(line)) is not None
        ]

        try:
            schema_chain = parser._build_schema_chain_for_vertex(description_data)
            self.assertIsInstance(schema_chain, list, "schema_chain should be a list")
        except RecursionError:
            self.fail("RecursionError should not occur when parsing WindowJoin description")


if __name__ == "__main__":
    unittest.main()
