"""
/tests/test_semantic_analyzer.py

语义分析器单元测试
"""
import sys
import os
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sql.lexer import SQLLexer
from sql.parser import SQLParser
from sql.semantic import SemanticAnalyzer, SemanticError
from sql.ast_nodes import *
from catalog import SystemCatalog, TableSchema, ColumnDefinition, DataType
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

def test_select_valid_columns():
    """测试SELECT有效列"""
    # 先尝试删除users和posts表，忽略不存在的异常
    from storage.page_manager import PageManager
    from storage.buffer_manager import BufferManager
    from catalog import SystemCatalog
    try:
        page_manager = PageManager(":memory:")
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        for table in ["users", "posts"]:
            try:
                catalog.drop_table(table)
            except Exception:
                pass
    except Exception:
        pass
    """初始化一个包含模式的虚拟目录"""
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        page_manager = PageManager(tmpfile.name)
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        # 创建 users 表
        users_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("name", DataType.VARCHAR, max_length=100),
            ColumnDefinition("age", DataType.INTEGER)
        ]
        catalog.create_table("users", users_columns)
        
        # 创建 posts 表
        posts_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("title", DataType.VARCHAR, max_length=255),
            ColumnDefinition("user_id", DataType.INTEGER)
        ]
        catalog.create_table("posts", posts_columns)
        
        analyzer = SemanticAnalyzer(catalog)

        sql = "SELECT id, name FROM users;"
        try:
            lexer = SQLLexer(sql)
            tokens = lexer.tokenize()
            parser = SQLParser(tokens)
            ast = parser.parse()
            result = analyzer.analyze(ast)
            assert_test("测试SELECT有效列", True)
        except (SyntaxError, SemanticError) as e:
            # 尝试获取行列信息
            line = getattr(e, 'line', '?')
            column = getattr(e, 'column', '?')
            assert_test("测试SELECT有效列", False, f"位置: {line}, {column}，原因: {e}")
    finally:
        tmpfile.close()
        os.remove(tmpfile.name)

def test_error_select_non_existent_table():
    """测试错误：SELECT不存在的表"""
    # 先尝试删除users和posts表，忽略不存在的异常
    from storage.page_manager import PageManager
    from storage.buffer_manager import BufferManager
    from catalog import SystemCatalog
    try:
        page_manager = PageManager(":memory:")
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        for table in ["users", "posts"]:
            try:
                catalog.drop_table(table)
            except Exception:
                pass
    except Exception:
        pass
    """初始化一个包含模式的虚拟目录"""
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        page_manager = PageManager(tmpfile.name)
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        # 创建 users 表
        users_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("name", DataType.VARCHAR, max_length=100),
            ColumnDefinition("age", DataType.INTEGER)
        ]
        catalog.create_table("users", users_columns)
        
        # 创建 posts 表
        posts_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("title", DataType.VARCHAR, max_length=255),
            ColumnDefinition("user_id", DataType.INTEGER)
        ]
        catalog.create_table("posts", posts_columns)
        
        analyzer = SemanticAnalyzer(catalog)

        sql = "SELECT * FROM non_existent_table;"
        try:
            lexer = SQLLexer(sql)
            tokens = lexer.tokenize()
            parser = SQLParser(tokens)
            ast = parser.parse()
            analyzer.analyze(ast)
            assert_test("测试错误：SELECT不存在的表", False, "预期抛出SemanticError")
        except SemanticError as e:
            assert_test("测试错误：SELECT不存在的表", True)
            assert_test("测试错误：SELECT不存在的表", '表 non_existent_table 不存在' in str(e.error_list), f"错误信息不匹配: {e.error_list}")
    finally:
        tmpfile.close()
        os.remove(tmpfile.name)

def test_error_select_non_existent_column():
    """测试错误：SELECT不存在的列"""
    # 先尝试删除users和posts表，忽略不存在的异常
    from storage.page_manager import PageManager
    from storage.buffer_manager import BufferManager
    from catalog import SystemCatalog
    try:
        page_manager = PageManager(":memory:")
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        for table in ["users", "posts"]:
            try:
                catalog.drop_table(table)
            except Exception:
                pass
    except Exception:
        pass
    """初始化一个包含模式的虚拟目录"""
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        page_manager = PageManager(tmpfile.name)
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        # 创建 users 表
        users_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("name", DataType.VARCHAR, max_length=100),
            ColumnDefinition("age", DataType.INTEGER)
        ]
        catalog.create_table("users", users_columns)
        
        # 创建 posts 表
        posts_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("title", DataType.VARCHAR, max_length=255),
            ColumnDefinition("user_id", DataType.INTEGER)
        ]
        catalog.create_table("posts", posts_columns)
        
        analyzer = SemanticAnalyzer(catalog)

        sql = "SELECT email FROM users;"
        try:
            lexer = SQLLexer(sql)
            tokens = lexer.tokenize()
            parser = SQLParser(tokens)
            ast = parser.parse()
            analyzer.analyze(ast)
            assert_test("测试错误：SELECT不存在的列", False, "预期抛出SemanticError")
        except SemanticError as e:
            assert_test("测试错误：SELECT不存在的列", True)
            assert_test("测试错误：SELECT不存在的列", '列 email 不存在' in str(e.error_list), f"错误信息不匹配: {e.error_list}")
    finally:
        tmpfile.close()
        os.remove(tmpfile.name)

def test_error_ambiguous_column_in_join():
    """测试错误：JOIN中存在不明确的列"""
    # 先尝试删除users和posts表，忽略不存在的异常
    from storage.page_manager import PageManager
    from storage.buffer_manager import BufferManager
    from catalog import SystemCatalog
    try:
        page_manager = PageManager(":memory:")
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        for table in ["users", "posts"]:
            try:
                catalog.drop_table(table)
            except Exception:
                pass
    except Exception:
        pass
    """初始化一个包含模式的虚拟目录"""
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        page_manager = PageManager(tmpfile.name)
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        # 创建 users 表
        users_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("name", DataType.VARCHAR, max_length=100),
            ColumnDefinition("age", DataType.INTEGER)
        ]
        catalog.create_table("users", users_columns)
        
        # 创建 posts 表
        posts_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("title", DataType.VARCHAR, max_length=255),
            ColumnDefinition("user_id", DataType.INTEGER)
        ]
        catalog.create_table("posts", posts_columns)
        
        analyzer = SemanticAnalyzer(catalog)

        sql = "SELECT id FROM users JOIN posts ON users.id = posts.user_id;"
        try:
            lexer = SQLLexer(sql)
            tokens = lexer.tokenize()
            parser = SQLParser(tokens)
            ast = parser.parse()
            analyzer.analyze(ast)
            assert_test("测试错误：JOIN中存在不明确的列", False, "预期抛出SemanticError")
        except SemanticError as e:
            assert_test("测试错误：JOIN中存在不明确的列", True)
            assert_test("测试错误：JOIN中存在不明确的列", '列 id 不明确' in str(e.error_list), f"错误信息不匹配: {e.error_list}")
    finally:
        tmpfile.close()
        os.remove(tmpfile.name)

def test_insert_column_count_mismatch():
    """测试INSERT列数/值数不匹配 (此检查在执行器中，语义分析器只检查列名)"""
    # 先尝试删除users和posts表，忽略不存在的异常
    from storage.page_manager import PageManager
    from storage.buffer_manager import BufferManager
    from catalog import SystemCatalog
    try:
        page_manager = PageManager(":memory:")
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        for table in ["users", "posts"]:
            try:
                catalog.drop_table(table)
            except Exception:
                pass
    except Exception:
        pass
    """初始化一个包含模式的虚拟目录"""
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        page_manager = PageManager(tmpfile.name)
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        # 创建 users 表
        users_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("name", DataType.VARCHAR, max_length=100),
            ColumnDefinition("age", DataType.INTEGER)
        ]
        catalog.create_table("users", users_columns)
        
        # 创建 posts 表
        posts_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("title", DataType.VARCHAR, max_length=255),
            ColumnDefinition("user_id", DataType.INTEGER)
        ]
        catalog.create_table("posts", posts_columns)
        
        analyzer = SemanticAnalyzer(catalog)

        sql = "INSERT INTO users (id, non_existent_col) VALUES (1, 'test');"
        try:
            lexer = SQLLexer(sql)
            tokens = lexer.tokenize()
            parser = SQLParser(tokens)
            ast = parser.parse()
            analyzer.analyze(ast)
            assert_test("测试INSERT列数/值数不匹配", False, "预期抛出SemanticError")
        except SemanticError as e:
            assert_test("测试INSERT列数/值数不匹配", True)
            assert_test("测试INSERT列数/值数不匹配", '列 non_existent_col 不存在于表 users' in str(e.error_list), f"错误信息不匹配: {e.error_list}")
    finally:
        tmpfile.close()
        os.remove(tmpfile.name)

def test_error_non_agg_in_group_by_select():
    """测试错误：非聚合列未包含在GROUP BY中"""
    # 先尝试删除users和posts表，忽略不存在的异常
    from storage.page_manager import PageManager
    from storage.buffer_manager import BufferManager
    from catalog import SystemCatalog
    try:
        page_manager = PageManager(":memory:")
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        for table in ["users", "posts"]:
            try:
                catalog.drop_table(table)
            except Exception:
                pass
    except Exception:
        pass
    """初始化一个包含模式的虚拟目录"""
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        page_manager = PageManager(tmpfile.name)
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        # 创建 users 表
        users_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("name", DataType.VARCHAR, max_length=100),
            ColumnDefinition("age", DataType.INTEGER)
        ]
        catalog.create_table("users", users_columns)
        
        # 创建 posts 表
        posts_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("title", DataType.VARCHAR, max_length=255),
            ColumnDefinition("user_id", DataType.INTEGER)
        ]
        catalog.create_table("posts", posts_columns)
        
        analyzer = SemanticAnalyzer(catalog)

        sql = "SELECT name, COUNT(id) FROM users GROUP BY age;"
        try:
            lexer = SQLLexer(sql)
            tokens = lexer.tokenize()
            parser = SQLParser(tokens)
            ast = parser.parse()
            analyzer.analyze(ast)
            assert_test("测试错误：非聚合列未包含在GROUP BY中", False, "预期抛出SemanticError")
        except SemanticError as e:
            assert_test("测试错误：非聚合列未包含在GROUP BY中", True)
            assert_test("测试错误：非聚合列未包含在GROUP BY中", '非聚合列必须包含在分组键中' in str(e.error_list), f"错误信息不匹配: {e.error_list}")
    finally:
        tmpfile.close()
        os.remove(tmpfile.name)

def test_create_existing_table_error():
    """测试错误：创建已存在的表"""
    # 先尝试删除users和posts表，忽略不存在的异常
    from storage.page_manager import PageManager
    from storage.buffer_manager import BufferManager
    from catalog import SystemCatalog
    try:
        page_manager = PageManager(":memory:")
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        for table in ["users", "posts"]:
            try:
                catalog.drop_table(table)
            except Exception:
                pass
    except Exception:
        pass
    """初始化一个包含模式的虚拟目录"""
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        page_manager = PageManager(tmpfile.name)
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        # 创建 users 表
        users_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("name", DataType.VARCHAR, max_length=100),
            ColumnDefinition("age", DataType.INTEGER)
        ]
        catalog.create_table("users", users_columns)
        
        # 创建 posts 表
        posts_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("title", DataType.VARCHAR, max_length=255),
            ColumnDefinition("user_id", DataType.INTEGER)
        ]
        catalog.create_table("posts", posts_columns)
        
        analyzer = SemanticAnalyzer(catalog)

        sql = "CREATE TABLE users (id INTEGER);"
        try:
            lexer = SQLLexer(sql)
            tokens = lexer.tokenize()
            parser = SQLParser(tokens)
            ast = parser.parse()
            analyzer.analyze(ast)
            assert_test("测试错误：创建已存在的表", False, "预期抛出SemanticError")
        except SemanticError as e:
            assert_test("测试错误：创建已存在的表", True)
            assert_test("测试错误：创建已存在的表", '表 users 已存在' in str(e.error_list), f"错误信息不匹配: {e.error_list}")
    finally:
        tmpfile.close()
        os.remove(tmpfile.name)

def test_drop_non_existing_table_error():
    """测试错误：删除不存在的表"""
    # 先尝试删除users和posts表，忽略不存在的异常
    from storage.page_manager import PageManager
    from storage.buffer_manager import BufferManager
    from catalog import SystemCatalog
    try:
        page_manager = PageManager(":memory:")
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        for table in ["users", "posts"]:
            try:
                catalog.drop_table(table)
            except Exception:
                pass
    except Exception:
        pass
    """初始化一个包含模式的虚拟目录"""
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        page_manager = PageManager(tmpfile.name)
        buffer_manager = BufferManager(page_manager)
        catalog = SystemCatalog(buffer_manager)
        # 创建 users 表
        users_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("name", DataType.VARCHAR, max_length=100),
            ColumnDefinition("age", DataType.INTEGER)
        ]
        catalog.create_table("users", users_columns)
        
        # 创建 posts 表
        posts_columns = [
            ColumnDefinition("id", DataType.INTEGER, primary_key=True),
            ColumnDefinition("title", DataType.VARCHAR, max_length=255),
            ColumnDefinition("user_id", DataType.INTEGER)
        ]
        catalog.create_table("posts", posts_columns)
        
        analyzer = SemanticAnalyzer(catalog)

        sql = "DROP TABLE non_existent_table;"
        try:
            lexer = SQLLexer(sql)
            tokens = lexer.tokenize()
            parser = SQLParser(tokens)
            ast = parser.parse()
            analyzer.analyze(ast)
            assert_test("测试错误：删除不存在的表", False, "预期抛出SemanticError")
        except SemanticError as e:
            assert_test("测试错误：删除不存在的表", True)
            assert_test("测试错误：删除不存在的表", '表 non_existent_table 不存在' in str(e.error_list), f"错误信息不匹配: {e.error_list}")
    finally:
        tmpfile.close()
        os.remove(tmpfile.name)


def main():
    test_select_valid_columns()
    test_error_select_non_existent_table()
    test_error_select_non_existent_column()
    test_error_ambiguous_column_in_join()
    test_insert_column_count_mismatch()
    test_error_non_agg_in_group_by_select()
    test_create_existing_table_error()
    test_drop_non_existing_table_error()
    print_test_summary()

if __name__ == "__main__":
    main()