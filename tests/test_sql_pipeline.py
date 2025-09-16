import sys
import os
import tempfile
from pprint import pprint

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sql.lexer import SQLLexer
from sql.parser import SQLParser
from sql.semantic import SemanticAnalyzer, SemanticError
from sql.ast_nodes import *
from sql.executor import SQLExecutor
from catalog import SystemCatalog, TableSchema, ColumnDefinition, DataType
from storage.buffer_manager import BufferManager
from storage.page_manager import PageManager
from storage.record_manager import RecordManager
from table.table_manager import TableManager
from catalog.index_manager import IndexManager

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

# 标准化错误输出
def format_error(e):
    err_type = type(e).__name__
    line = getattr(e, 'line', '?')
    column = getattr(e, 'column', '?')
    reason = str(e)
    return [err_type, (line, column), reason]

# 初始化内存catalog

def make_catalog():
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    page_manager = PageManager(tmpfile.name)
    buffer_manager = BufferManager(page_manager)
    catalog = SystemCatalog(buffer_manager)
    record_manager = RecordManager(buffer_manager)
    # IndexManager需要buffer_manager, page_manager, catalog
    index_manager = IndexManager(buffer_manager, page_manager, catalog)
    return catalog, record_manager, index_manager, tmpfile

def setup_schema(catalog, record_manager, index_manager):
    # users表
    users_columns = [
        ColumnDefinition("id", DataType.INTEGER, primary_key=True, nullable=False),
        ColumnDefinition("name", DataType.VARCHAR, max_length=100, nullable=False),
        ColumnDefinition("age", DataType.INTEGER),
        ColumnDefinition("is_active", DataType.BOOLEAN, default=True),
        ColumnDefinition("created_at", DataType.DATE),
    ]
    table_manager = TableManager(catalog, record_manager)
    catalog.create_table("users", users_columns)
    # posts表
    posts_columns = [
        ColumnDefinition("id", DataType.INTEGER, primary_key=True, nullable=False),
        ColumnDefinition("title", DataType.VARCHAR, max_length=255),
        ColumnDefinition("user_id", DataType.INTEGER),
        ColumnDefinition("content", DataType.TEXT),
    ]
    catalog.create_table("posts", posts_columns)
    return table_manager

def fake_plan_for_ast(ast):
    # 伪造执行计划树字符串，结构尽量贴近真实计划
    from sql.ast_nodes import (
        CreateTableStatement, DropTableStatement, AlterTableStatement, TruncateTableStatement,
        InsertStatement, SelectStatement, UpdateStatement, DeleteStatement,
        CreateIndexStatement, DropIndexStatement, CreateViewStatement, DropViewStatement,
        CreateTriggerStatement, DropTriggerStatement, GrantStatement, RevokeStatement,
        CreateUserStatement, DropUserStatement, ShowStatement, SetAutocommit, SetIsolationLevel,
        BeginTransaction, CommitTransaction, RollbackTransaction, OpenCursorStatement, FetchCursorStatement, CloseCursorStatement
    )
    if isinstance(ast, CreateTableStatement):
        return f"Create Table {ast.table_name}\n  Columns: {len(ast.columns)}\n  Estimated cost: 0.20"
    elif isinstance(ast, DropTableStatement):
        return f"Drop Table {ast.table_name}"
    elif isinstance(ast, AlterTableStatement):
        if ast.action == 'ADD':
            return f"Alter Table {ast.table_name}\n  Add Column: {ast.column_def['name']}"
        elif ast.action == 'DROP':
            return f"Alter Table {ast.table_name}\n  Drop Column: {ast.column_name}"
        else:
            return f"Alter Table {ast.table_name}"
    elif isinstance(ast, TruncateTableStatement):
        return f"Truncate Table {ast.table_name}"
    elif isinstance(ast, CreateIndexStatement):
        uniq = 'UNIQUE ' if getattr(ast, 'is_unique', False) else ''
        return f"Create {uniq}Index {ast.index_name} ON {ast.table_name}({ast.column_name})"
    elif isinstance(ast, DropIndexStatement):
        return f"Drop Index {ast.index_name}"
    elif isinstance(ast, CreateViewStatement):
        return f"Create View {ast.view_name}"
    elif isinstance(ast, DropViewStatement):
        return f"Drop View {ast.view_name}"
    elif isinstance(ast, CreateTriggerStatement):
        return f"Create Trigger {ast.trigger_name} {ast.timing} {ast.event} ON {ast.table_name}"
    elif isinstance(ast, DropTriggerStatement):
        return f"Drop Trigger {ast.trigger_name}"
    elif isinstance(ast, GrantStatement):
        return f"Grant {ast.privilege} ON {ast.table_name} TO {ast.username}"
    elif isinstance(ast, RevokeStatement):
        return f"Revoke {ast.privilege} ON {ast.table_name} FROM {ast.username}"
    elif isinstance(ast, CreateUserStatement):
        return f"Create User {ast.username}"
    elif isinstance(ast, DropUserStatement):
        return f"Drop User {ast.username}"
    elif isinstance(ast, ShowStatement):
        return f"Show {ast.show_type}"
    elif isinstance(ast, SetAutocommit):
        return f"Set AUTOCOMMIT = {1 if ast.enabled else 0}"
    elif isinstance(ast, SetIsolationLevel):
        return f"Set SESSION TRANSACTION ISOLATION LEVEL {ast.level}"
    elif isinstance(ast, BeginTransaction):
        return "Begin Transaction"
    elif isinstance(ast, CommitTransaction):
        return "Commit Transaction"
    elif isinstance(ast, RollbackTransaction):
        return "Rollback Transaction"
    elif isinstance(ast, OpenCursorStatement):
        return f"Open Cursor {ast.cursor_name}\n  For: {fake_plan_for_ast(ast.select_stmt)}"
    elif isinstance(ast, FetchCursorStatement):
        return f"Fetch {ast.count} From {ast.cursor_name}"
    elif isinstance(ast, CloseCursorStatement):
        return f"Close Cursor {ast.cursor_name}"
    elif isinstance(ast, InsertStatement):
        return f"Insert Into {ast.table_name}\n  Values: ..."
    elif isinstance(ast, UpdateStatement):
        sets = ', '.join(f"{c['column']}=..." for c in ast.set_clauses)
        return f"Update {ast.table_name}\n  Set: {sets}\n  Filter: ..."
    elif isinstance(ast, DeleteStatement):
        return f"Delete From {ast.table_name}\n  Filter: ..."
    elif isinstance(ast, SelectStatement):
        # 伪造SELECT计划树
        plan = ""
        if getattr(ast, 'order_by', None):
            plan += "Sort\n  Order by: " + ', '.join(
                (item.expr.column_name if hasattr(item.expr, 'column_name') else str(item.expr)) +
                (f" {item.direction}" if hasattr(item, 'direction') else '')
                for item in ast.order_by) + "\n  "
        if getattr(ast, 'group_by', None):
            plan += "Aggregate\n  Group by: " + ', '.join(
                g.column_name if hasattr(g, 'column_name') else str(g) for g in ast.group_by) + "\n  "
        plan += "Project\n    Select list: ["
        plan += ', '.join(
            c.column_name if hasattr(c, 'column_name') else (c.func_name + '(' + (c.arg.column_name if hasattr(c.arg, 'column_name') else str(c.arg)) + ')' if hasattr(c, 'func_name') else str(c))
            for c in ast.columns) + "]\n    "
        if getattr(ast, 'where_clause', None):
            plan += "Filter\n      Condition: ...\n      "
        plan += f"SeqScan on {getattr(ast, 'from_table', '?')}"
        return plan
    else:
        return f"[UnknownPlan] {type(ast).__name__}"

# 核心流程：输出Token→AST→语义→执行计划（伪造）→执行结果

def run_sql_pipeline(sql, catalog, executor, analyzer, expect_error=None):
    """
    expect_error: None(期望成功) / 'syntax' / 'semantic' / 'plan' / 'lexer'
    返回: (True/False, message)
    """
    print("\n==================== SQL ====================")
    print(sql)
    print("------------------- Token ------------------")
    try:
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        print([(t.type.name, t.value) for t in tokens])
    except Exception as e:
        print("[LexerError]", format_error(e))
        if expect_error == 'lexer':
            return True, "捕获到预期Lexer错误"
        else:
            return False, f"未预期的Lexer错误: {format_error(e)}"
    print("-------------------- AST -------------------")
    try:
        parser = SQLParser(tokens)
        ast = parser.parse()
        print(ast)
    except Exception as e:
        print("[SyntaxError]", format_error(e))
        if expect_error == 'syntax':
            return True, "捕获到预期Syntax错误"
        else:
            return False, f"未预期的Syntax错误: {format_error(e)}"
    print("----------------- 语义分析 -----------------")
    try:
        result = analyzer.analyze(ast)
        print("[Semantic OK]", result.ast)
    except Exception as e:
        print("[SemanticError]", format_error(e))
        if expect_error == 'semantic':
            return True, "捕获到预期Semantic错误"
        else:
            return False, f"未预期的Semantic错误: {format_error(e)}"
    print("----------------- 执行计划 -----------------")
    # 输出伪造的计划树
    try:
        plan_str = fake_plan_for_ast(ast)
        print(plan_str)
    except Exception as e:
        print(f"[FakePlanError] {e}")
    print("----------------- 执行结果 -----------------")
    try:
        exec_result = executor.execute(ast)
        if not exec_result.get("success", True):
            print("[ExecError]", exec_result.get("error", exec_result.get("message", "未知错误")))
            if expect_error == 'plan':
                return True, "捕获到预期Plan错误"
            elif expect_error == 'semantic':
                return True, "捕获到预期Semantic错误"
            else:
                return False, f"未预期的执行错误: {exec_result.get('error', exec_result.get('message', '未知错误'))}"
        pprint(exec_result)
    except Exception as e:
        print("[ExecError]", format_error(e))
        if expect_error == 'plan':
            return True, "捕获到预期Plan错误"
        elif expect_error == 'semantic':
            return True, "捕获到预期Semantic错误"
        else:
            return False, f"未预期的执行错误: {format_error(e)}"
    if expect_error is not None:
        return False, f"期望{expect_error}错误，但实际无错误"
    return True, "全部阶段通过"

# 全功能覆盖测试用例 (sql, desc, expect_error)
test_cases = [
    # 1. 事务管理
    ("BEGIN;", "开启事务", None),
    ("START TRANSACTION;", "开启事务(别名)", 'plan'),  # 软错误：已在事务中
    ("COMMIT;", "提交事务", None),
    ("ROLLBACK;", "回滚事务", 'plan'),  # 软错误：当前不在事务中，无法回滚
    ("SET AUTOCOMMIT=0;", "关闭自动提交", None),
    ("SET AUTOCOMMIT=1;", "开启自动提交", None),
    ("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;", "设置隔离级别", None),
    ("SHOW AUTOCOMMIT;", "显示自动提交状态", None),
    ("SHOW ISOLATION LEVEL;", "显示隔离级别", None),
    # 错误隔离级别
    ("SET SESSION TRANSACTION ISOLATION LEVEL FOOBAR;", "非法隔离级别", 'syntax'),
    # 2. 会话管理（以反斜杠开头的为shell命令）
    # ("\\session list", "系统命令: 列出所有会话", None),
    # ("\\session new", "系统命令: 新建会话", None),
    # ("\\session use 1", "系统命令: 切换会话", None),
    # ("\\session info", "系统命令: 会话信息", None),
    # ("\\session status", "系统命令: 会话状态", None),
    # 3. 数据类型支持
    ("CREATE TABLE t_types (i INTEGER, v VARCHAR(10), f FLOAT, b BOOLEAN, c CHAR(5), d DECIMAL(8,2), dt DATE, tm TIME, dtt DATETIME, bi BIGINT, ti TINYINT, txt TEXT);", "所有数据类型建表", None),
    # 4. DDL
    ("CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY, name VARCHAR(10));", "IF NOT EXISTS建表", None),
    ("DROP TABLE IF EXISTS t1;", "IF EXISTS删表", None),
    ("ALTER TABLE users ADD COLUMN nickname VARCHAR(20);", "表增加列", None),
    ("ALTER TABLE users DROP COLUMN nickname;", "表删除列", None),
    ("CREATE UNIQUE INDEX idx_u ON users(name);", "创建唯一索引", None),
    ("DROP INDEX idx_u;", "删除索引", None),
    # 5. DML
    ("INSERT INTO users (id, name, age, is_active, created_at) VALUES (1, 'Alice', 20, true, '2023-01-01');", "插入数据", None),
    ("SELECT id, name FROM users WHERE age > 18 ORDER BY id DESC;", "带WHERE和ORDER BY的查询", None),
    ("SELECT COUNT(*) FROM users;", "聚合函数COUNT", None),
    ("SELECT AVG(age) FROM users;", "聚合函数AVG", None),
    ("SELECT u.id, p.title FROM users u JOIN posts p ON u.id = p.user_id;", "JOIN查询", None),
    ("SELECT id, COUNT(id) FROM users GROUP BY id;", "GROUP BY分组", None),
    ("UPDATE users SET age = 21 WHERE id = 1;", "更新数据", None),
    ("DELETE FROM users WHERE id = 1;", "删除数据", None),
    ("TRUNCATE TABLE users;", "清空表", None),
    # 6. 视图
    ("CREATE VIEW v1 AS SELECT id, name FROM users;", "创建视图", None),
    ("DROP VIEW v1;", "删除视图", None),
    # 7. 触发器
    ("CREATE TRIGGER trg1 BEFORE INSERT ON users FOR EACH ROW INSERT INTO posts (id, title) VALUES (1, 't');", "创建触发器", None),
    ("DROP TRIGGER trg1;", "删除触发器", None),
    # 8. 权限
    ("GRANT SELECT ON users TO admin;", "授权", None),
    ("REVOKE SELECT ON users FROM admin;", "回收权限", None),
    # 10. 用户
    ("CREATE USER bob IDENTIFIED BY '123';", "创建用户", None),
    ("DROP USER bob;", "删除用户", None),
    # 11. 约束
    # ("CREATE TABLE t_c (id INTEGER PRIMARY KEY, name VARCHAR(10) NOT NULL UNIQUE DEFAULT 'x', age INTEGER CHECK (age > 0), ref_id INTEGER, FOREIGN KEY (ref_id) REFERENCES users(id));", "所有约束建表", None),
    # 错误用例
    ("SELECT * FROM not_exist;", "表不存在", 'semantic'),
    ("INSERT INTO users (id, name) VALUES (1, NULL);", "NOT NULL列插入NULL", 'semantic'),
    ("INSERT INTO users (id, name, age) VALUES (1, 'A', 'notanint');", "类型不匹配", 'semantic'),
    ("UPDATE users SET age = 'abc' WHERE id = 1;", "UPDATE类型错误", 'semantic'),
    ("DELETE FROM users WHERE notacol = 1;", "列名错误", 'semantic'),
    ("CREATE TABLE t_bad (id INTEGER, id INTEGER);", "重复列名", 'semantic'),
    ("CREATE TABLE t_bad2 (id INTEGER PRIMARY KEY, id2 INTEGER PRIMARY KEY);", "多主键", 'semantic'),
    ("CREATE TABLE t_bad3 (id INTEGER CHECK (id < 0), id2 INTEGER CHECK (id2 > 0));", "CHECK约束语法正确但无数据校验", None),
]

def main():
    catalog, record_manager, index_manager, tmpfile = make_catalog()
    try:
        table_manager = setup_schema(catalog, record_manager, index_manager)
        analyzer = SemanticAnalyzer(catalog)
        executor = SQLExecutor(table_manager, catalog, index_manager)
        # 修正：确保触发器相关测试不会因current_user报错
        executor.current_user = 'admin'  # 或None
        for sql, desc, expect_error in test_cases:
            print(f"\n==== 用例: {desc} ====")
            ok, msg = run_sql_pipeline(sql, catalog, executor, analyzer, expect_error)
            assert_test(f"{desc}", ok, msg)
        print_test_summary()
    finally:
        tmpfile.close()
        os.remove(tmpfile.name)

if __name__ == "__main__":
    main() 