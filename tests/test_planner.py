"""
/tests/test_planner.py

执行计划生成器单元测试
"""
import sys
import os
import json
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sql.lexer import SQLLexer
from sql.parser import SQLParser
from sql.planner_interface import PlanGeneratorInterface
from sql.plan_nodes import *
from catalog import SystemCatalog, TableSchema, ColumnDefinition, DataType
from storage.page_manager import PageManager
from storage.buffer_manager import BufferManager

passed = 0
failed = 0

def assert_test(test_name, condition, message=""):
    global passed, failed
    if condition:
        print(f"✅ PASS: {test_name}")
        passed += 1
    else:
        print(f"❌ FAIL: {test_name} - {message}")
        failed += 1

def print_test_summary():
    total = passed + failed
    print("\n" + "=" * 60)
    print(f"📊 测试结果统计: 通过: {passed}  失败: {failed}")
    if total > 0:
        print(f"📈 通过率: {passed / total * 100:.1f}%")
    if failed == 0:
        print("🎉 所有测试通过！")
    else:
        print("⚠️  部分测试失败，请检查相关功能")

def test_plan_create_table():
    """测试CREATE TABLE的执行计划"""
    sql = "CREATE TABLE new_table (col1 INTEGER);"
    lexer = SQLLexer(sql)
    tokens = lexer.tokenize()
    parser = SQLParser(tokens)
    ast = parser.parse()

    # 确保每次测试前都清理掉旧的数据库文件
    db_file = "test_planner.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        page_manager = PageManager(tmpfile.name)
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        # 清理users表
        try:
            catalog.drop_table("users")
        except Exception:
            pass
        # 创建 users 表
        users_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("name", DataType.VARCHAR, max_length=100)
        ]
        catalog.create_table("users", users_columns)
        planner_interface = PlanGeneratorInterface(catalog, index_manager=None)

        plan = planner_interface.planner.generate_plan(ast)
        assert_test("测试CREATE TABLE的执行计划", isinstance(plan, CreateTableNode))
        assert_test("测试CREATE TABLE的执行计划", plan.table_name == "new_table")
    finally:
        tmpfile.close()
        os.remove(tmpfile.name)

def test_plan_insert():
    """测试INSERT的执行计划"""
    sql = "INSERT INTO users (id, name) VALUES (1, 'Alice');"
    lexer = SQLLexer(sql)
    tokens = lexer.tokenize()
    parser = SQLParser(tokens)
    ast = parser.parse()

    # 确保每次测试前都清理掉旧的数据库文件
    db_file = "test_planner.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        page_manager = PageManager(tmpfile.name)
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        # 清理users表
        try:
            catalog.drop_table("users")
        except Exception:
            pass
        # 创建 users 表
        users_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("name", DataType.VARCHAR, max_length=100)
        ]
        catalog.create_table("users", users_columns)
        planner_interface = PlanGeneratorInterface(catalog, index_manager=None)

        plan = planner_interface.planner.generate_plan(ast)
        assert_test("测试INSERT的执行计划", isinstance(plan, InsertNode))
        assert_test("测试INSERT的执行计划", plan.table_name == 'users')
        assert_test("测试INSERT的执行计划", plan.values[0] == [1, 'Alice'])
    finally:
        tmpfile.close()
        os.remove(tmpfile.name)

def test_plan_seq_scan():
    """测试简单SELECT生成的SeqScan计划"""
    sql = "SELECT * FROM users;"
    lexer = SQLLexer(sql)
    tokens = lexer.tokenize()
    parser = SQLParser(tokens)
    ast = parser.parse()

    # 确保每次测试前都清理掉旧的数据库文件
    db_file = "test_planner.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        page_manager = PageManager(tmpfile.name)
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        # 清理users表
        try:
            catalog.drop_table("users")
        except Exception:
            pass
        # 创建 users 表
        users_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("name", DataType.VARCHAR, max_length=100)
        ]
        catalog.create_table("users", users_columns)
        planner_interface = PlanGeneratorInterface(catalog, index_manager=None)

        plan = planner_interface.planner.generate_plan(ast)
        assert_test("测试简单SELECT生成的SeqScan计划", isinstance(plan, SeqScanNode))
        assert_test("测试简单SELECT生成的SeqScan计划", plan.table_name == 'users')
    finally:
        tmpfile.close()
        os.remove(tmpfile.name)

def test_plan_filter():
    """测试带WHERE的SELECT生成的Filter + SeqScan计划"""
    sql = "SELECT * FROM users WHERE id = 10;"
    lexer = SQLLexer(sql)
    tokens = lexer.tokenize()
    parser = SQLParser(tokens)
    ast = parser.parse()

    # 确保每次测试前都清理掉旧的数据库文件
    db_file = "test_planner.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        page_manager = PageManager(tmpfile.name)
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        # 清理users表
        try:
            catalog.drop_table("users")
        except Exception:
            pass
        # 创建 users 表
        users_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("name", DataType.VARCHAR, max_length=100)
        ]
        catalog.create_table("users", users_columns)
        planner_interface = PlanGeneratorInterface(catalog, index_manager=None)

        plan = planner_interface.planner.generate_plan(ast)
        assert_test("测试带WHERE的SELECT生成的Filter + SeqScan计划", isinstance(plan, FilterNode))
        assert_test("测试带WHERE的SELECT生成的Filter + SeqScan计划", isinstance(plan.children[0], SeqScanNode))
        # 假设 plan.condition 暴露了 BinaryOp 或类似的结构
        # assert_test("测试带WHERE的SELECT生成的Filter + SeqScan计划", isinstance(plan.condition, BinaryOp)) # 这行可能需要根据你的实际实现调整
    finally:
        tmpfile.close()
        os.remove(tmpfile.name)

def test_plan_project():
    """测试选择部分列的SELECT生成的Project计划"""
    sql = "SELECT name FROM users;"
    lexer = SQLLexer(sql)
    tokens = lexer.tokenize()
    parser = SQLParser(tokens)
    ast = parser.parse()

    # 确保每次测试前都清理掉旧的数据库文件
    db_file = "test_planner.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        page_manager = PageManager(tmpfile.name)
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        # 清理users表
        try:
            catalog.drop_table("users")
        except Exception:
            pass
        # 创建 users 表
        users_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("name", DataType.VARCHAR, max_length=100)
        ]
        catalog.create_table("users", users_columns)
        planner_interface = PlanGeneratorInterface(catalog, index_manager=None)

        plan = planner_interface.planner.generate_plan(ast)
        assert_test("测试选择部分列的SELECT生成的Project计划", isinstance(plan, ProjectNode))
        assert_test("测试选择部分列的SELECT生成的Project计划", isinstance(plan.children[0], SeqScanNode))
        assert_test("测试选择部分列的SELECT生成的Project计划", len(plan.select_list) == 1)
    finally:
        tmpfile.close()
        os.remove(tmpfile.name)

def test_plan_output_formats():
    """测试不同格式的计划输出"""
    sql = "SELECT name FROM users WHERE id > 5;"
    lexer = SQLLexer(sql)
    tokens = lexer.tokenize()
    parser = SQLParser(tokens)
    ast = parser.parse()

    # 确保每次测试前都清理掉旧的数据库文件
    db_file = "test_planner.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        page_manager = PageManager(tmpfile.name)
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        # 清理users表
        try:
            catalog.drop_table("users")
        except Exception:
            pass
        # 创建 users 表
        users_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("name", DataType.VARCHAR, max_length=100)
        ]
        catalog.create_table("users", users_columns)
        planner_interface = PlanGeneratorInterface(catalog, index_manager=None)

        # 测试树形输出
        result_tree = planner_interface.generate_execution_plan(ast, output_format='tree')
        assert_test("测试不同格式的计划输出 - 树形输出", result_tree['success'], "树形输出失败")
        assert_test("测试不同格式的计划输出 - 树形输出", 'Project' in result_tree['plan'])
        assert_test("测试不同格式的计划输出 - 树形输出", 'Filter' in result_tree['plan'])
        assert_test("测试不同格式的计划输出 - 树形输出", 'SeqScan' in result_tree['plan'])

        # 测试JSON输出
        result_json = planner_interface.generate_execution_plan(ast, output_format='json')
        assert_test("测试不同格式的计划输出 - JSON输出", result_json['success'], "JSON输出失败")
        plan_dict = json.loads(result_json['plan'])
        assert_test("测试不同格式的计划输出 - JSON输出", plan_dict['node_type'] == 'Project')
        assert_test("测试不同格式的计划输出 - JSON输出", plan_dict['children'][0]['node_type'] == 'Filter')

        # 测试S表达式输出
        result_sexp = planner_interface.generate_execution_plan(ast, output_format='sexp')
        assert_test("测试不同格式的计划输出 - S表达式输出", result_sexp['success'], "S表达式输出失败")
        assert_test("测试不同格式的计划输出 - S表达式输出", result_sexp['plan'].startswith('(project'))
        assert_test("测试不同格式的计划输出 - S表达式输出", '(filter' in result_sexp['plan'])
        assert_test("测试不同格式的计划输出 - S表达式输出", '(seqscan' in result_sexp['plan'])
    finally:
        tmpfile.close()
        os.remove(tmpfile.name)

def main():
    test_plan_create_table()
    test_plan_insert()
    test_plan_seq_scan()
    test_plan_filter()
    test_plan_project()
    test_plan_output_formats()
    print_test_summary()

if __name__ == "__main__":
    main()