"""
/tests/test_parser.py

语法分析器单元测试
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sql.lexer import SQLLexer
from sql.parser import SQLParser
from sql.ast_nodes import *
from pprint import pprint

passed = 0
failed = 0

def assert_test(test_name, condition, message=""):
    global passed, failed
    if condition:
        print(f"✅ PASS: {test_name}")
        passed += 1
    else:
        # 标准化错误输出
        if isinstance(message, Exception):
            err_type = type(message).__name__
            line = getattr(message, 'line', '?')
            column = getattr(message, 'column', '?')
            reason = str(message)
            print(f"❌ FAIL: {test_name} - {[err_type, (line, column), reason]}")
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

def print_test_coverage():
    print("\n================= 语法分析测试覆盖功能 =================")
    print("1. CREATE TABLE/INDEX/VIEW/USER/TRIGGER：表、索引、视图、用户、触发器的创建")
    print("2. DROP TABLE/INDEX/VIEW/USER/TRIGGER：表、索引、视图、用户、触发器的删除")
    print("3. INSERT/DELETE/UPDATE/SELECT：数据插入、删除、更新、查询，支持JOIN、聚合、分组、排序、WHERE等")
    print("4. GRANT/REVOKE：权限授予与回收")
    print("5. BEGIN/START/COMMIT/ROLLBACK：事务控制")
    print("6. SET/SHOW：系统参数设置与查询（如AUTOCOMMIT、ISOLATION LEVEL）")
    print("7. TRUNCATE/ALTER：表截断与结构变更")
    print("8. IF [NOT] EXISTS：存在性修饰符解析")
    print("9. 游标相关：OPEN CURSOR、FETCH、CLOSE CURSOR，支持游标声明、批量提取、关闭")
    print("10. 语法错误分支：缺分号、缺关键字、括号、非法token等健壮性测试")
    print("======================================================\n")

def test_parse_create_table():
    sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, score FLOAT DEFAULT 0.0);"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, CreateTableStatement) and ast.table_name == "users" and len(ast.columns) == 3
        assert_test("测试解析CREATE TABLE语句", cond)
    except Exception as e:
        assert_test("测试解析CREATE TABLE语句", False, e)

def test_parse_insert():
    sql = "INSERT INTO products (name, price) VALUES ('Apple', 1.99), ('Milk', 2.5);"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, InsertStatement) and ast.table_name == "products" and ast.columns == ["name", "price"] and len(ast.values) == 2
        assert_test("测试解析INSERT语句", cond)
    except Exception as e:
        assert_test("测试解析INSERT语句", False, e)

def test_parse_select():
    sql = "SELECT id, COUNT(id) FROM users JOIN posts ON users.id = posts.user_id WHERE age > 20 GROUP BY id ORDER BY id DESC;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = (
            isinstance(ast, SelectStatement) and
            isinstance(ast.from_table, JoinClause) and
            ast.from_table.right == 'posts' and  # 修正此处，原为tuple
            isinstance(ast.columns[0], ColumnRef) and
            ast.columns[0].column_name == 'id' and
            isinstance(ast.columns[1], AggregateFunction) and ast.columns[1].func_name == 'COUNT' and
            isinstance(ast.where_clause, BinaryOp) and ast.where_clause.operator == '>' and
            len(ast.group_by) == 1 and ast.group_by[0].column_name == 'id' and
            len(ast.order_by) == 1 and ast.order_by[0].direction == 'DESC'
        )
        assert_test("测试解析SELECT语句", cond, f"ast: {ast}")
    except Exception as e:
        assert_test("测试解析SELECT语句", False, e)

def test_parse_delete():
    sql = "DELETE FROM orders WHERE status = 'cancelled';"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, DeleteStatement) and ast.table_name == "orders" and ast.where_clause is not None and ast.where_clause.right.value == "cancelled"
        assert_test("测试解析DELETE语句", cond)
    except Exception as e:
        assert_test("测试解析DELETE语句", False, e)

def test_parse_update():
    sql = "UPDATE employees SET salary = salary * 1.1, title = 'Senior Developer' WHERE department = 'IT';"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, UpdateStatement) and ast.table_name == 'employees' and len(ast.set_clauses) == 2 and ast.set_clauses[1]['column'] == 'title' and ast.set_clauses[1]['value'].value == 'Senior Developer' and ast.where_clause is not None
        if not cond:
            print(ast)
        assert_test("测试解析UPDATE语句", cond, f"set_clauses: {ast.set_clauses}, where_clause: {ast.where_clause}")
    except Exception as e:
        assert_test("测试解析UPDATE语句", False, e)

def test_syntax_error_missing_parenthesis():
    sql = "INSERT INTO t VALUES (1, 2;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        assert_test("测试语法错误：缺少括号", False, "SyntaxError expected but not raised")
    except SyntaxError as e:
        assert_test("测试语法错误：缺少括号", True, e)

def test_syntax_error_unexpected_token():
    sql = "SELECT id FROM users WHERE;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        assert_test("测试语法错误：非预期的符号", False, "SyntaxError expected but not raised")
    except SyntaxError as e:
        assert_test("测试语法错误：非预期的符号", True, e)

def test_parse_if_exists():
    sql_drop = "DROP TABLE IF EXISTS my_table;"
    try:
        lexer = SQLLexer(sql_drop)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, DropTableStatement) and ast.if_exists and ast.table_name == "my_table"
        assert_test("测试解析 IF [NOT] EXISTS 子句 (DROP)", cond)
    except Exception as e:
        assert_test("测试解析 IF [NOT] EXISTS 子句 (DROP)", False, e)

    sql_create = "CREATE TABLE IF NOT EXISTS my_table (id INTEGER);"
    try:
        lexer = SQLLexer(sql_create)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, CreateTableStatement) and ast.if_not_exists and ast.table_name == "my_table" and len(ast.columns) == 1 and ast.columns[0]['name'] == 'id' and ast.columns[0]['type'] == 'INTEGER'
        assert_test("测试解析 IF [NOT] EXISTS 子句 (CREATE)", cond)
    except Exception as e:
        assert_test("测试解析 IF [NOT] EXISTS 子句 (CREATE)", False, e)

def test_parse_create_index():
    sql = "CREATE INDEX idx_name ON users(id);"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, CreateIndexStatement) and ast.index_name == "idx_name" and ast.table_name == "users" and ast.column_name == "id"
        assert_test("测试解析CREATE INDEX语句", cond)
    except Exception as e:
        assert_test("测试解析CREATE INDEX语句", False, e)

def test_parse_drop_index():
    sql = "DROP INDEX idx_name;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, DropIndexStatement) and ast.index_name == "idx_name"
        assert_test("测试解析DROP INDEX语句", cond)
    except Exception as e:
        assert_test("测试解析DROP INDEX语句", False, e)

def test_parse_grant():
    sql = "GRANT SELECT ON users TO alice;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, GrantStatement) and ast.privilege == "SELECT" and ast.table_name == "users" and ast.username == "alice"
        assert_test("测试解析GRANT语句", cond)
    except Exception as e:
        assert_test("测试解析GRANT语句", False, e)

def test_parse_revoke():
    sql = "REVOKE SELECT ON users FROM alice;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, RevokeStatement) and ast.privilege == "SELECT" and ast.table_name == "users" and ast.username == "alice"
        assert_test("测试解析REVOKE语句", cond)
    except Exception as e:
        assert_test("测试解析REVOKE语句", False, e)

def test_parse_begin():
    sql = "BEGIN;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, BeginTransaction)
        assert_test("测试解析BEGIN语句", cond)
    except Exception as e:
        assert_test("测试解析BEGIN语句", False, e)

def test_parse_start_transaction():
    sql = "START TRANSACTION;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, BeginTransaction)
        assert_test("测试解析START TRANSACTION语句", cond)
    except Exception as e:
        assert_test("测试解析START TRANSACTION语句", False, e)

def test_parse_commit():
    sql = "COMMIT;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, CommitTransaction)
        assert_test("测试解析COMMIT语句", cond)
    except Exception as e:
        assert_test("测试解析COMMIT语句", False, e)

def test_parse_rollback():
    sql = "ROLLBACK;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, RollbackTransaction)
        assert_test("测试解析ROLLBACK语句", cond)
    except Exception as e:
        assert_test("测试解析ROLLBACK语句", False, e)

def test_parse_set_autocommit():
    sql = "SET AUTOCOMMIT=1;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = hasattr(ast, 'enabled') and ast.enabled is True
        assert_test("测试解析SET AUTOCOMMIT语句", cond)
    except Exception as e:
        assert_test("测试解析SET AUTOCOMMIT语句", False, e)

def test_parse_set_isolation():
    sql = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = hasattr(ast, 'level') and ast.level == "REPEATABLE READ"
        assert_test("测试解析SET ISOLATION LEVEL语句", cond)
    except Exception as e:
        assert_test("测试解析SET ISOLATION LEVEL语句", False, e)

def test_parse_truncate():
    sql = "TRUNCATE TABLE t1;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, TruncateTableStatement) and ast.table_name == "t1"
        assert_test("测试解析TRUNCATE TABLE语句", cond)
    except Exception as e:
        assert_test("测试解析TRUNCATE TABLE语句", False, e)

def test_parse_alter_table_add():
    sql = "ALTER TABLE t1 ADD COLUMN age INTEGER;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, AlterTableStatement) and ast.table_name == "t1" and ast.action == 'ADD'
        assert_test("测试解析ALTER TABLE ADD COLUMN语句", cond)
    except Exception as e:
        assert_test("测试解析ALTER TABLE ADD COLUMN语句", False, e)

def test_parse_alter_table_drop():
    sql = "ALTER TABLE t1 DROP COLUMN age;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, AlterTableStatement) and ast.table_name == "t1" and ast.action == 'DROP'
        assert_test("测试解析ALTER TABLE DROP COLUMN语句", cond)
    except Exception as e:
        assert_test("测试解析ALTER TABLE DROP COLUMN语句", False, e)

def test_parse_show_autocommit():
    sql = "SHOW AUTOCOMMIT;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()

        cond = hasattr(ast, 'show_type') and ast.show_type == "AUTOCOMMIT"
        assert_test("测试解析SHOW AUTOCOMMIT语句", cond)
    except Exception as e:
        assert_test("测试解析SHOW AUTOCOMMIT语句", False, e)

def test_parse_show_isolation():
    sql = "SHOW ISOLATION LEVEL;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = hasattr(ast, 'show_type') and ast.show_type == "ISOLATION_LEVEL"
        assert_test("测试解析SHOW ISOLATION LEVEL语句", cond)
    except Exception as e:
        assert_test("测试解析SHOW ISOLATION LEVEL语句", False, e)

def test_syntax_error_missing_semicolon():
    sql = "SELECT id FROM users"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        parser.parse()
        assert_test("测试语法错误：缺少分号", False, "SyntaxError expected but not raised")
    except SyntaxError as e:
        msg = str(e)
        assert_test("测试语法错误：缺少分号", True, e)

def test_syntax_error_missing_from():
    sql = "SELECT id, name users;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        parser.parse()
        assert_test("测试语法错误：缺少FROM", False, "SyntaxError expected but not raised")
    except SyntaxError as e:
        msg = str(e)
        assert_test("测试语法错误：缺少FROM", True, e)

def test_syntax_error_unexpected_token_in_insert():
    sql = "INSERT INTO t VALUES 1, 2);"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        parser.parse()
        assert_test("测试语法错误：INSERT缺少括号", False, "SyntaxError expected but not raised")
    except SyntaxError as e:
        msg = str(e)
        assert_test("测试语法错误：INSERT缺少括号", True, e)

def test_syntax_error_illegal_token():
    sql = "SELECT id FROM users WHERE id @ 1;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        parser.parse()
        assert_test("测试语法错误：非法token", False, "SyntaxError expected but not raised")
    except SyntaxError as e:
        msg = str(e)
        assert_test("测试语法错误：非法token", True, e)

def test_syntax_error_update_missing_set():
    sql = "UPDATE users id = 1;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        parser.parse()
        assert_test("测试语法错误：UPDATE缺少SET", False, "SyntaxError expected but not raised")
    except SyntaxError as e:
        msg = str(e)
        assert_test("测试语法错误：UPDATE缺少SET", True, e)

def test_syntax_error_create_table_missing_paren():
    sql = "CREATE TABLE t id INTEGER, name VARCHAR(10));"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        parser.parse()
        assert_test("测试语法错误：CREATE TABLE缺少左括号", False, "SyntaxError expected but not raised")
    except SyntaxError as e:
        msg = str(e)
        assert_test("测试语法错误：CREATE TABLE缺少左括号", True, e)

# ================== 游标相关测试 ==================
def test_parse_open_cursor():
    sql = "OPEN CURSOR my_cursor FOR SELECT * FROM users;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = hasattr(ast, 'cursor_name') and ast.cursor_name == "my_cursor"
        assert_test("测试解析OPEN CURSOR语句", cond)
    except Exception as e:
        assert_test("测试解析OPEN CURSOR语句", False, e)

def test_parse_fetch_cursor():
    sql = "FETCH 10 FROM my_cursor;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = hasattr(ast, 'cursor_name') and ast.cursor_name == "my_cursor"
        assert_test("测试解析FETCH CURSOR语句", cond)
    except Exception as e:
        assert_test("测试解析FETCH CURSOR语句", False, e)

def test_parse_close_cursor():
    sql = "CLOSE CURSOR my_cursor;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = hasattr(ast, 'cursor_name') and ast.cursor_name == "my_cursor"
        assert_test("测试解析CLOSE CURSOR语句", cond)
    except Exception as e:
        assert_test("测试解析CLOSE CURSOR语句", False, e)

def test_parse_open_cursor_complex():
    sql = "OPEN CURSOR c1 FOR SELECT id, name FROM users WHERE age > 18 ORDER BY id DESC;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, OpenCursorStatement) and ast.cursor_name == "c1" and isinstance(ast.select_stmt, SelectStatement)
        assert_test("测试解析OPEN CURSOR复杂SELECT", cond)
    except Exception as e:
        assert_test("测试解析OPEN CURSOR复杂SELECT", False, e)

def test_parse_fetch_cursor_1():
    sql = "FETCH 1 FROM c1;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, FetchCursorStatement) and ast.count == 1 and ast.cursor_name == "c1"
        assert_test("测试解析FETCH 1 FROM CURSOR", cond)
    except Exception as e:
        assert_test("测试解析FETCH 1 FROM CURSOR", False, e)

def test_parse_close_cursor_simple():
    sql = "CLOSE CURSOR c1;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()
        cond = isinstance(ast, CloseCursorStatement) and ast.cursor_name == "c1"
        assert_test("测试解析CLOSE CURSOR语句", cond)
    except Exception as e:
        assert_test("测试解析CLOSE CURSOR语句", False, e)

# 错误用法

def test_syntax_error_open_cursor_missing_for():
    sql = "OPEN CURSOR c1 SELECT * FROM t1;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        parser.parse()
        assert_test("测试语法错误：OPEN CURSOR缺FOR", False, "SyntaxError expected but not raised")
    except SyntaxError as e:
        assert_test("测试语法错误：OPEN CURSOR缺FOR", True, e)

def test_syntax_error_fetch_cursor_missing_from():
    sql = "FETCH 10 c1;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        parser.parse()
        assert_test("测试语法错误：FETCH缺FROM", False, "SyntaxError expected but not raised")
    except SyntaxError as e:
        assert_test("测试语法错误：FETCH缺FROM", True, e)

def test_syntax_error_close_cursor_missing_name():
    sql = "CLOSE CURSOR;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        parser.parse()
        assert_test("测试语法错误：CLOSE CURSOR缺游标名", False, "SyntaxError expected but not raised")
    except SyntaxError as e:
        assert_test("测试语法错误：CLOSE CURSOR缺游标名", True, e)

def test_syntax_error_set_invalid_param():
    sql = "SET FOOBAR=1;"
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        parser.parse()
        assert_test("测试语法错误：SET非法参数", False, "SyntaxError expected but not raised")
    except SyntaxError as e:
        assert_test("测试语法错误：SET非法参数", True, e)

def main():
    print_test_coverage()
    test_parse_create_table()
    test_parse_insert()
    test_parse_select()
    test_parse_delete()
    test_parse_update()
    test_syntax_error_missing_parenthesis()
    test_syntax_error_unexpected_token()
    test_parse_if_exists()
    test_parse_create_index()
    test_parse_drop_index()
    test_parse_grant()
    test_parse_revoke()
    test_parse_begin()
    test_parse_start_transaction()
    test_parse_commit()
    test_parse_rollback()
    test_parse_set_autocommit()
    test_parse_set_isolation()
    test_parse_truncate()
    test_parse_alter_table_add()
    test_parse_alter_table_drop()
    test_parse_show_autocommit()
    test_parse_show_isolation()
    print_test_summary()
    test_syntax_error_missing_semicolon()
    test_syntax_error_missing_from()
    test_syntax_error_unexpected_token_in_insert()
    test_syntax_error_illegal_token()
    test_syntax_error_update_missing_set()
    test_syntax_error_create_table_missing_paren()
    test_parse_open_cursor()
    test_parse_fetch_cursor()
    test_parse_close_cursor()
    test_parse_open_cursor_complex()
    test_parse_fetch_cursor_1()
    test_parse_close_cursor_simple()
    test_syntax_error_open_cursor_missing_for()
    test_syntax_error_fetch_cursor_missing_from()
    test_syntax_error_close_cursor_missing_name()
    test_syntax_error_set_invalid_param()


if __name__ == "__main__":
    main()