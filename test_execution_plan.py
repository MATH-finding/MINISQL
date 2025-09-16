"""
测试执行计划与存储层对接功能 - 修复版
"""

import os
import tempfile
from storage import PageManager, BufferManager, RecordManager
from catalog import SystemCatalog, ColumnDefinition, DataType
from catalog.index_manager import IndexManager
from table import TableManager
from sql.lexer import SQLLexer
from sql.parser import SQLParser
from sql.executor import SQLExecutor
from sql.planner import ExecutionPlanner
from sql.execution_engine import ExecutionEngine

class TestExecutionPlanIntegration:
    def __init__(self):
        # 创建临时数据库文件
        self.temp_dir = tempfile.mkdtemp()
        self.db_file = os.path.join(self.temp_dir, "test_db.db")

        # 初始化存储层
        self.page_manager = PageManager(self.db_file)
        self.buffer_manager = BufferManager(self.page_manager, cache_size=50)
        self.record_manager = RecordManager(self.buffer_manager)

        # 初始化系统目录
        self.catalog = SystemCatalog(self.buffer_manager)

        # 初始化索引管理器
        self.index_manager = IndexManager(self.buffer_manager, self.page_manager, self.catalog)

        # 初始化表管理器
        self.table_manager = TableManager(self.catalog, self.record_manager)

        # 初始化执行器
        self.executor = SQLExecutor(self.table_manager, self.catalog, self.index_manager)

        # 测试状态标记
        self.table_created = False
        self.data_inserted = False

        print(f"[TEST] 测试数据库文件: {self.db_file}")

    def ensure_table_exists(self):
        """确保测试表存在"""
        if not self.table_created:
            self._create_test_table()
            self.table_created = True

    def ensure_data_exists(self):
        """确保测试数据存在"""
        self.ensure_table_exists()
        if not self.data_inserted:
            self._insert_test_data()
            self.data_inserted = True

    def _create_test_table(self):
        """创建测试表"""
        sql = """
        CREATE TABLE students (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            age INTEGER,
            grade FLOAT
        );
        """

        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()

        planner = ExecutionPlanner(self.catalog, self.index_manager)
        plan = planner.generate_plan(ast)

        engine = ExecutionEngine(self.table_manager, self.catalog, self.index_manager)
        result = engine.execute_plan(plan)

        if not result["success"]:
            raise Exception(f"创建表失败: {result.get('message', 'unknown error')}")

    def _insert_test_data(self):
        """插入测试数据"""
        sql = """
        INSERT INTO students (id, name, age, grade) 
        VALUES 
            (1, 'Alice', 20, 85.5),
            (2, 'Bob', 19, 92.0),
            (3, 'Charlie', 21, 78.5);
        """

        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()

        planner = ExecutionPlanner(self.catalog, self.index_manager)
        plan = planner.generate_plan(ast)

        engine = ExecutionEngine(self.table_manager, self.catalog, self.index_manager)
        result = engine.execute_plan(plan)

        if not result["success"]:
            raise Exception(f"插入数据失败: {result.get('message', 'unknown error')}")

    def test_create_table_with_plan(self):
        """测试建表算子"""
        print("\n=== 测试 CREATE TABLE 执行计划 ===")

        # 重新初始化避免冲突
        if self.table_created:
            print("表已存在，跳过创建")
            return

        sql = """
        CREATE TABLE students (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            age INTEGER,
            grade FLOAT
        );
        """

        # 解析SQL
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()

        # 生成执行计划
        planner = ExecutionPlanner(self.catalog, self.index_manager)
        plan = planner.generate_plan(ast)

        print("执行计划:")
        print(plan.to_tree_string())

        # 执行计划
        engine = ExecutionEngine(self.table_manager, self.catalog, self.index_manager)
        result = engine.execute_plan(plan)

        print(f"执行结果: {result}")

        # 验证表是否创建成功
        schema = self.catalog.get_table_schema("students")
        assert schema is not None, "表创建失败"
        assert len(schema.columns) == 4, "表列数不正确"

        self.table_created = True
        print("✅ CREATE TABLE 测试通过")

    def test_insert_with_plan(self):
        """测试插入算子"""
        print("\n=== 测试 INSERT 执行计划 ===")

        # 确保表存在
        self.ensure_table_exists()

        # 检查是否已经插入过数据
        if self.data_inserted:
            print("数据已存在，跳过插入")
            return

        sql = """
        INSERT INTO students (id, name, age, grade) 
        VALUES 
            (1, 'Alice', 20, 85.5),
            (2, 'Bob', 19, 92.0),
            (3, 'Charlie', 21, 78.5);
        """

        # 解析SQL
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()

        # 生成执行计划
        planner = ExecutionPlanner(self.catalog, self.index_manager)
        plan = planner.generate_plan(ast)

        print("执行计划:")
        print(plan.to_tree_string())

        # 执行计划
        engine = ExecutionEngine(self.table_manager, self.catalog, self.index_manager)
        result = engine.execute_plan(plan)

        print(f"执行结果: {result}")

        # 验证数据是否插入成功
        records = self.table_manager.scan_table("students")
        assert len(records) == 3, f"插入数据数量不正确，期望3，实际{len(records)}"

        self.data_inserted = True
        print("✅ INSERT 测试通过")

    def test_seq_scan_with_plan(self):
        """测试顺序扫描算子"""
        print("\n=== 测试 SeqScan 执行计划 ===")

        # 确保数据存在
        self.ensure_data_exists()

        sql = "SELECT * FROM students;"

        # 解析SQL
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()

        # 生成执行计划
        planner = ExecutionPlanner(self.catalog, self.index_manager)
        plan = planner.generate_plan(ast)

        print("执行计划:")
        print(plan.to_tree_string())

        # 执行计划
        engine = ExecutionEngine(self.table_manager, self.catalog, self.index_manager)
        result = engine.execute_plan(plan)

        print(f"执行结果: {result}")
        print(f"返回数据: {result.get('data', [])}")

        # 验证查询结果
        assert result["success"] == True, "查询失败"
        assert result["rows_returned"] == 3, f"返回行数不正确，期望3，实际{result['rows_returned']}"

        # 验证数据内容
        data = result["data"]
        assert data[0]["name"] == "Alice", "数据内容不正确"
        assert data[1]["age"] == 19, "数据内容不正确"

        print("✅ SeqScan 测试通过")

    def test_filter_with_plan(self):
        """测试过滤算子"""
        print("\n=== 测试 Filter 执行计划 ===")

        # 确保数据存在
        self.ensure_data_exists()

        sql = "SELECT * FROM students WHERE age > 19;"

        # 解析SQL
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()

        # 生成执行计划
        planner = ExecutionPlanner(self.catalog, self.index_manager)
        plan = planner.generate_plan(ast)

        print("执行计划:")
        print(plan.to_tree_string())

        # 执行计划
        engine = ExecutionEngine(self.table_manager, self.catalog, self.index_manager)
        result = engine.execute_plan(plan)

        print(f"执行结果: {result}")
        print(f"过滤后数据: {result.get('data', [])}")

        # 验证过滤结果
        assert result["success"] == True, "查询失败"
        assert result["rows_returned"] == 2, f"过滤后行数不正确，期望2，实际{result['rows_returned']}"

        # 验证过滤条件
        for row in result["data"]:
            assert row["age"] > 19, f"过滤条件不正确，age={row['age']}"

        print("✅ Filter 测试通过")

    def test_project_with_plan(self):
        """测试投影算子"""
        print("\n=== 测试 Project 执行计划 ===")

        # 确保数据存在
        self.ensure_data_exists()

        sql = "SELECT name, grade FROM students WHERE age >= 20;"

        # 解析SQL
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()

        # 生成执行计划
        planner = ExecutionPlanner(self.catalog, self.index_manager)
        plan = planner.generate_plan(ast)

        print("执行计划:")
        print(plan.to_tree_string())

        # 执行计划
        engine = ExecutionEngine(self.table_manager, self.catalog, self.index_manager)
        result = engine.execute_plan(plan)

        print(f"执行结果: {result}")
        print(f"投影后数据: {result.get('data', [])}")

        # 验证投影结果
        assert result["success"] == True, "查询失败"
        assert result["rows_returned"] == 2, f"投影后行数不正确，期望2，实际{result['rows_returned']}"

        # 验证投影列
        for row in result["data"]:
            assert "name" in row, "缺少name列"
            assert "grade" in row, "缺少grade列"
            assert "id" not in row, "不应该包含id列"
            assert "age" not in row, "不应该包含age列"

        print("✅ Project 测试通过")

    def test_physical_storage_access(self):
        """测试物理存储访问"""
        print("\n=== 测试物理存储访问 ===")

        # 确保数据存在
        self.ensure_data_exists()

        # 检查页面分配情况
        pages = self.catalog.get_table_pages("students")
        print(f"表 'students' 使用的页面: {pages}")

        # 检查缓冲池状态
        cache_stats = self.buffer_manager.get_cache_stats()
        print(f"缓冲池状态: {cache_stats}")

        # 验证物理页面访问
        for page_id in pages:
            page = self.buffer_manager.get_page(page_id)
            try:
                record_count = page.read_int(0)
                free_space_offset = page.read_int(4)
                print(f"页面 {page_id}: 记录数={record_count}, 空闲空间偏移={free_space_offset}")
                assert record_count > 0, "页面应该包含记录"
            finally:
                self.buffer_manager.unpin_page(page_id, False)

        print("✅ 物理存储访问测试通过")

    def test_compare_execution_methods(self):
        """比较执行计划与直接执行的结果"""
        print("\n=== 比较执行方法 ===")

        # 确保数据存在
        self.ensure_data_exists()

        sql = "SELECT name, age FROM students WHERE age > 19;"

        # 解析SQL
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()

        # 方法1：直接执行
        result1 = self.executor.execute(ast)
        print("直接执行结果:")
        print(f"  成功: {result1['success']}")
        print(f"  行数: {result1.get('rows_returned', 0)}")

        # 方法2：执行计划执行
        result2 = self.executor.execute_with_execution_plan(ast, use_plan=True)
        print("执行计划结果:")
        print(f"  成功: {result2['success']}")
        print(f"  行数: {result2.get('rows_returned', 0)}")
        if 'execution_plan' in result2:
            print(f"  估计成本: {result2['execution_plan'].get('estimated_cost', 0)}")

        # 比较结果
        assert result1["success"] == result2["success"], "执行状态不一致"
        assert result1.get("rows_returned", 0) == result2.get("rows_returned", 0), "返回行数不一致"

        print("✅ 执行方法比较测试通过")

    def run_all_tests(self):
        """运行所有测试"""
        print("开始执行计划与存储层对接测试...")

        try:
            self.test_create_table_with_plan()
            self.test_insert_with_plan()
            self.test_seq_scan_with_plan()
            self.test_filter_with_plan()
            self.test_project_with_plan()
            self.test_physical_storage_access()
            self.test_compare_execution_methods()

            print("\n🎉 所有测试通过！")
            print("✅ 操作系统页面管理与执行计划对接成功")
            print("✅ 支持物理数据访问")
            print("✅ 实现了 CreateTable、Insert、SeqScan、Filter、Project 算子")

        except Exception as e:
            print(f"\n❌ 测试失败: {e}")
            import traceback
            traceback.print_exc()

        finally:
            # 清理资源
            self.cleanup()

    def cleanup(self):
        """清理测试资源"""
        try:
            if hasattr(self, 'buffer_manager'):
                self.buffer_manager.flush_all()
            if os.path.exists(self.db_file):
                os.remove(self.db_file)
            if os.path.exists(self.temp_dir):
                os.rmdir(self.temp_dir)
            print("测试资源清理完成")
        except Exception as e:
            print(f"清理资源时出错: {e}")

if __name__ == "__main__":
    # 运行测试
    test = TestExecutionPlanIntegration()
    test.run_all_tests()