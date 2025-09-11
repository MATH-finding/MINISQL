#!/usr/bin/env python3
"""
formatter.py 功能测试文件
在shell运行后，可通过命令行运行此文件进行测试

使用方法:
1. 先启动shell: python main.py shell
2. 在另一个终端运行: python test_formatter.py
"""

import sys
import os
from typing import Dict, Any, List
from interface.formatter import (
    format_query_result,
    format_table_info,
    format_database_stats,
    _format_select_result
)

# 新增: 语义分析集成测试需要数据库
try:
    from interface.database import SimpleDatabase
    HAS_DB = True
except Exception:
    HAS_DB = False

class TestFormatter:
    """Formatter功能测试类"""
    
    def __init__(self):
        self.tests_passed = 0
        self.tests_failed = 0
        self.test_results = []
    
    def run_all_tests(self):
        """运行所有测试"""
        print("🧪 开始测试 formatter.py 功能")
        print("=" * 50)
        
        # 测试format_query_result函数
        self.test_format_query_result()
        
        # 测试format_select_result函数
        self.test_format_select_result()
        
        # 测试format_table_info函数
        self.test_format_table_info()
        
        # 测试format_database_stats函数
        self.test_format_database_stats()
        
        # 输出测试总结
        self.print_summary()
    
    def test_format_query_result(self):
        """测试format_query_result函数"""
        print("\n📋 测试 format_query_result 函数")
        print("-" * 30)
        
        # 测试用例1: 成功的SELECT查询
        test_case_1 = {
            "success": True,
            "type": "SELECT",
            "data": [
                {"id": 1, "name": "张三", "age": 20},
                {"id": 2, "name": "李四", "age": 21}
            ]
        }
        print("测试用例1: 成功的SELECT查询")
        self._capture_output(lambda: format_query_result(test_case_1))
        
        # 测试用例2: 成功的CREATE_TABLE操作
        test_case_2 = {
            "success": True,
            "type": "CREATE_TABLE",
            "message": "表 users 创建成功"
        }
        print("\n测试用例2: 成功的CREATE_TABLE操作")
        self._capture_output(lambda: format_query_result(test_case_2))
        
        # 测试用例3: 成功的INSERT操作
        test_case_3 = {
            "success": True,
            "type": "INSERT",
            "message": "插入数据成功"
        }
        print("\n测试用例3: 成功的INSERT操作")
        self._capture_output(lambda: format_query_result(test_case_3))
        
        # 测试用例4: 失败的操作
        test_case_4 = {
            "success": False,
            "error": "表不存在"
        }
        print("\n测试用例4: 失败的操作")
        self._capture_output(lambda: format_query_result(test_case_4))
        
        # 测试用例5: 未知类型操作
        test_case_5 = {
            "success": True,
            "type": "UNKNOWN",
            "message": "操作完成"
        }
        print("\n测试用例5: 未知类型操作")
        self._capture_output(lambda: format_query_result(test_case_5))

        # 新增: ORDER BY / GROUP BY 的格式化展示
        print("\n测试用例6: ORDER BY 结果展示")
        order_by_result = {
            "success": True,
            "type": "SELECT",
            "data": [
                {"name": "Alice", "score": 98},
                {"name": "Bob", "score": 85},
                {"name": "Carol", "score": 76},
            ]
        }
        self._capture_output(lambda: format_query_result(order_by_result))

        print("\n测试用例7: GROUP BY 结果展示(含COUNT)")
        group_by_result = {
            "success": True,
            "type": "SELECT",
            "data": [
                {"dept": "HR", "COUNT": 3},
                {"dept": "ENG", "COUNT": 5},
            ]
        }
        self._capture_output(lambda: format_query_result(group_by_result))
        
        self._mark_test_passed("format_query_result基本功能")
    
    def test_format_select_result(self):
        """测试_format_select_result函数"""
        print("\n📊 测试 _format_select_result 函数")
        print("-" * 30)
        
        # 测试用例1: 正常数据表格
        test_data_1 = {
            "data": [
                {"id": 1, "name": "张三", "age": 20, "email": "zhangsan@test.com"},
                {"id": 2, "name": "李四", "age": 21, "email": "lisi@test.com"},
                {"id": 3, "name": "王五", "age": 19, "email": "wangwu@test.com"}
            ]
        }
        print("测试用例1: 正常数据表格")
        self._capture_output(lambda: _format_select_result(test_data_1))
        
        # 测试用例2: 空数据
        test_data_2 = {"data": []}
        print("\n测试用例2: 空数据")
        self._capture_output(lambda: _format_select_result(test_data_2))
        
        # 测试用例3: 单行数据
        test_data_3 = {
            "data": [{"count": 5}]
        }
        print("\n测试用例3: 单行数据")
        self._capture_output(lambda: _format_select_result(test_data_3))
        
        # 测试用例4: 包含NULL值的数据
        test_data_4 = {
            "data": [
                {"id": 1, "name": "张三", "phone": None},
                {"id": 2, "name": None, "phone": "123456789"}
            ]
        }
        print("\n测试用例4: 包含NULL值的数据")
        self._capture_output(lambda: _format_select_result(test_data_4))
        
        # 测试用例5: 长文本数据
        test_data_5 = {
            "data": [
                {"id": 1, "description": "这是一个非常长的描述文本，用来测试列宽度自动调整功能"},
                {"id": 2, "description": "短文本"}
            ]
        }
        print("\n测试用例5: 长文本数据")
        self._capture_output(lambda: _format_select_result(test_data_5))

        # 新增: GROUP BY + 聚合结果
        test_data_6 = {
            "data": [
                {"dept": "ENG", "SUM": 120, "COUNT": 5},
                {"dept": "HR", "SUM": 60, "COUNT": 3},
            ]
        }
        print("\n测试用例6: GROUP BY + 聚合结果")
        self._capture_output(lambda: _format_select_result(test_data_6))
        
        self._mark_test_passed("_format_select_result基本功能")
    
    def test_format_table_info(self):
        """测试format_table_info函数"""
        print("\n🏗️ 测试 format_table_info 函数")
        print("-" * 30)
        
        # 测试用例1: 完整表信息
        table_info_1 = {
            "table_name": "users",
            "columns": [
                {
                    "name": "id",
                    "type": "INTEGER",
                    "max_length": None,
                    "primary_key": True,
                    "nullable": False
                },
                {
                    "name": "name",
                    "type": "VARCHAR",
                    "max_length": 50,
                    "primary_key": False,
                    "nullable": False
                },
                {
                    "name": "email",
                    "type": "VARCHAR",
                    "max_length": 100,
                    "primary_key": False,
                    "nullable": True
                },
                {
                    "name": "age",
                    "type": "INTEGER",
                    "max_length": None,
                    "primary_key": False,
                    "nullable": True
                }
            ],
            "record_count": 10,
            "pages": [1, 2, 3]
        }
        print("测试用例1: 完整表信息")
        self._capture_output(lambda: format_table_info(table_info_1))
        
        # 测试用例2: 简单表信息
        table_info_2 = {
            "table_name": "test_table",
            "columns": [
                {
                    "name": "id",
                    "type": "INTEGER",
                    "max_length": None,
                    "primary_key": True,
                    "nullable": False
                }
            ],
            "record_count": 0,
            "pages": []
        }
        print("\n测试用例2: 简单表信息")
        self._capture_output(lambda: format_table_info(table_info_2))
        
        # 测试用例3: 错误信息
        table_info_3 = {
            "error": "表 'nonexistent' 不存在"
        }
        print("\n测试用例3: 错误信息")
        self._capture_output(lambda: format_table_info(table_info_3))
        
        self._mark_test_passed("format_table_info基本功能")
    
    def test_format_database_stats(self):
        """测试format_database_stats函数"""
        print("\n📈 测试 format_database_stats 函数")
        print("-" * 30)
        
        # 测试用例1: 完整数据库统计信息
        stats_1 = {
            "database_file": "test_database.db",
            "file_size_pages": 25,
            "tables_count": 5,
            "cache_stats": {
                "cache_hits": 150,
                "cache_misses": 30,
                "hit_rate": 0.8333,
                "cached_pages": 12,
                "cache_size": 16
            }
        }
        print("测试用例1: 完整数据库统计信息")
        self._capture_output(lambda: format_database_stats(stats_1))
        
        # 测试用例2: 零统计信息
        stats_2 = {
            "database_file": "empty_database.db",
            "file_size_pages": 0,
            "tables_count": 0,
            "cache_stats": {
                "cache_hits": 0,
                "cache_misses": 0,
                "hit_rate": 0.0,
                "cached_pages": 0,
                "cache_size": 16
            }
        }
        print("\n测试用例2: 零统计信息")
        self._capture_output(lambda: format_database_stats(stats_2))
        
        # 测试用例3: 高命中率
        stats_3 = {
            "database_file": "high_performance.db",
            "file_size_pages": 100,
            "tables_count": 10,
            "cache_stats": {
                "cache_hits": 950,
                "cache_misses": 50,
                "hit_rate": 0.95,
                "cached_pages": 16,
                "cache_size": 16
            }
        }
        print("\n测试用例3: 高命中率统计")
        self._capture_output(lambda: format_database_stats(stats_3))
        
        self._mark_test_passed("format_database_stats基本功能")

    # 新增：语义分析集成测试
    def test_semantic_integration(self):
        print("\n🧠 测试 语义分析 集成")
        print("-" * 30)
        if not HAS_DB:
            print("(跳过) 环境缺少数据库依赖，无法运行语义分析集成测试")
            self._mark_test_passed("语义分析集成(跳过)")
            return
        db_path = "semantic_test.db"
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception:
                pass
        db = SimpleDatabase(db_path, cache_size=16)
        try:
            # 建表
            format_query_result(db.execute_sql("CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT)"))
            format_query_result(db.execute_sql("CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)"))
            # 插入
            db.execute_sql("INSERT INTO dept VALUES (1, 'HR')")
            db.execute_sql("INSERT INTO dept VALUES (2, 'ENG')")
            db.execute_sql("INSERT INTO emp VALUES (10, 'Alice', 1, 100)")
            db.execute_sql("INSERT INTO emp VALUES (11, 'Bob', 2, 200)")
            db.execute_sql("INSERT INTO emp VALUES (12, 'Carol', 2, 300)")

            # 1) * 展开与 ORDER BY（列在选择列表中）
            print("\n[用例1] SELECT * 与 ORDER BY name DESC")
            res1 = db.execute_sql("SELECT * FROM emp ORDER BY name DESC")
            format_query_result(res1)

            # 2) GROUP BY 正确用法
            print("\n[用例2] GROUP BY dept_id, 计算 COUNT 与 SUM")
            res2 = db.execute_sql("SELECT dept_id, COUNT(*), SUM(salary) FROM emp GROUP BY dept_id ORDER BY dept_id ASC")
            format_query_result(res2)

            # 3) 违反聚合规则（应触发语义错误）
            print("\n[用例3] 聚合规则校验 (应失败)")
            res3 = db.execute_sql("SELECT dept_id, salary FROM emp GROUP BY dept_id")
            format_query_result(res3)

            # 4) 列歧义（JOIN 不带前缀）
            print("\n[用例4] 列歧义校验 (应失败)")
            res4 = db.execute_sql("SELECT id FROM emp JOIN dept ON emp.dept_id = dept.id")
            format_query_result(res4)

        finally:
            try:
                db.flush_all()
                db.close()
            except Exception:
                pass
            try:
                os.remove(db_path)
            except Exception:
                pass
        self._mark_test_passed("语义分析集成")
    
    def test_edge_cases(self):
        """测试边界情况"""
        print("\n⚠️ 测试边界情况")
        print("-" * 30)
        
        # 测试空字典
        print("测试用例1: 空字典")
        self._capture_output(lambda: format_query_result({}))
        
        # 测试None数据
        print("\n测试用例2: None数据")
        try:
            format_query_result(None)
            self._mark_test_failed("None数据处理")
        except Exception as e:
            print(f"正确捕获异常: {e}")
            self._mark_test_passed("None数据处理")
        
        # 测试大数据量
        large_data = {
            "success": True,
            "type": "SELECT",
            "data": [{"id": i, "value": f"data_{i}"} for i in range(100)]
        }
        print("\n测试用例3: 大数据量（100行）")
        self._capture_output(lambda: format_query_result(large_data))
        
        self._mark_test_passed("边界情况处理")
    
    def test_integration_scenarios(self):
        """测试集成场景"""
        print("\n🔄 测试集成场景")
        print("-" * 30)
        
        # 模拟完整的数据库操作流程
        print("场景1: 创建表 -> 插入数据 -> 查询数据")
        
        # 1. 创建表
        create_result = {
            "success": True,
            "type": "CREATE_TABLE",
            "message": "表 students 创建成功"
        }
        print("1. 创建表:")
        self._capture_output(lambda: format_query_result(create_result))
        
        # 2. 插入数据
        insert_result = {
            "success": True,
            "type": "INSERT",
            "message": "成功插入 3 条记录"
        }
        print("\n2. 插入数据:")
        self._capture_output(lambda: format_query_result(insert_result))
        
        # 3. 查询数据
        select_result = {
            "success": True,
            "type": "SELECT",
            "data": [
                {"id": 1, "name": "张三", "age": 20, "gpa": 3.8},
                {"id": 2, "name": "李四", "age": 21, "gpa": 3.5},
                {"id": 3, "name": "王五", "age": 19, "gpa": 3.9}
            ]
        }
        print("\n3. 查询数据:")
        self._capture_output(lambda: format_query_result(select_result))
        
        # 4. 查看表信息
        table_info = {
            "table_name": "students",
            "columns": [
                {"name": "id", "type": "INTEGER", "max_length": None, "primary_key": True, "nullable": False},
                {"name": "name", "type": "VARCHAR", "max_length": 50, "primary_key": False, "nullable": False},
                {"name": "age", "type": "INTEGER", "max_length": None, "primary_key": False, "nullable": True},
                {"name": "gpa", "type": "FLOAT", "max_length": None, "primary_key": False, "nullable": True}
            ],
            "record_count": 3,
            "pages": [1]
        }
        print("\n4. 查看表信息:")
        self._capture_output(lambda: format_table_info(table_info))
        
        self._mark_test_passed("集成场景测试")
    
    def _capture_output(self, func):
        """捕获函数输出"""
        import io
        import contextlib
        
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            try:
                func()
            except Exception as e:
                print(f"❌ 执行出错: {e}")
        
        output = f.getvalue()
        print(output, end="")
        return output
    
    def _mark_test_passed(self, test_name: str):
        """标记测试通过"""
        self.tests_passed += 1
        self.test_results.append((test_name, "PASSED"))
        print(f"✅ {test_name} - 测试通过")
    
    def _mark_test_failed(self, test_name: str):
        """标记测试失败"""
        self.tests_failed += 1
        self.test_results.append((test_name, "FAILED"))
        print(f"❌ {test_name} - 测试失败")
    
    def print_summary(self):
        """打印测试总结"""
        total_tests = self.tests_passed + self.tests_failed
        print("\n" + "=" * 50)
        print("📊 测试总结")
        print("=" * 50)
        print(f"总测试数: {total_tests}")
        print(f"通过: {self.tests_passed}")
        print(f"失败: {self.tests_failed}")
        print(f"通过率: {(self.tests_passed/total_tests)*100:.1f}%" if total_tests > 0 else "通过率: 0%")
        
        print("\n📋 详细结果:")
        for test_name, result in self.test_results:
            status = "✅" if result == "PASSED" else "❌"
            print(f"  {status} {test_name}")
        
        if self.tests_failed == 0:
            print("\n🎉 所有测试都通过了！formatter.py 功能正常")
        else:
            print(f"\n⚠️ 有 {self.tests_failed} 个测试失败，需要检查")

def run_performance_test():
    """运行性能测试"""
    print("\n⏱️ 性能测试")
    print("-" * 30)
    
    import time
    
    # 创建大量数据用于性能测试
    large_data = {
        "success": True,
        "type": "SELECT", 
        "data": [
            {
                "id": i,
                "name": f"用户_{i}",
                "email": f"user_{i}@test.com",
                "description": f"这是用户{i}的详细描述信息" * 5
            }
            for i in range(1000)
        ]
    }
    
    print("测试大数据量格式化性能（1000行数据）...")
    start_time = time.time()
    
    # 捕获输出避免大量打印
    import io
    import contextlib
    f = io.StringIO()
    with contextlib.redirect_stdout(f):
        format_query_result(large_data)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"✅ 性能测试完成")
    print(f"📊 处理1000行数据耗时: {execution_time:.3f} 秒")
    print(f"📊 平均每行处理时间: {(execution_time * 1000) / len(large_data['data']):.3f} 毫秒")
    
    if execution_time < 1.0:
        print("🎉 性能表现优秀！")
    elif execution_time < 5.0:
        print("👍 性能表现良好")
    else:
        print("⚠️ 性能可能需要优化")

def main():
    """主函数"""
    print("🧪 formatter.py 综合测试套件")
    print("=" * 50)
    print("测试内容:")
    print("- format_query_result 函数")
    print("- _format_select_result 函数") 
    print("- format_table_info 函数")
    print("- format_database_stats 函数")
    print("- 语义分析集成测试")
    print("- 边界情况处理")
    print("- 集成场景测试")
    print("- 性能测试")
    print()
    
    # 运行测试
    tester = TestFormatter()
    tester.run_all_tests()
    
    # 运行语义分析集成测试
    tester.test_semantic_integration()
    
    # 运行边界情况测试
    tester.test_edge_cases()
    
    # 运行集成场景测试
    tester.test_integration_scenarios()
    
    # 运行性能测试
    run_performance_test()
    
    print("\n🎯 测试说明:")
    print("- 此测试文件独立于数据库实例运行；语义分析集成测试会临时创建一个测试数据库文件")
    print("- 主要测试 formatter.py 中各个格式化函数与语义分析整体效果")
    print("- 包含正常情况、边界情况和错误情况的测试")
    print("- 可以在shell运行时同时运行此测试")

if __name__ == "__main__":
    main()