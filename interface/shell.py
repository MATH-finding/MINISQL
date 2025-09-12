"""
交互式SQL Shell
"""

import sys
from typing import Optional
from .database import SimpleDatabase
from .formatter import format_query_result, format_table_info, format_database_stats

# 可选：增强型交互输入（灰色联想、补全）
try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.completion import Completer, Completion
    from prompt_toolkit.auto_suggest import AutoSuggest
    from prompt_toolkit.document import Document
    from prompt_toolkit.formatted_text import HTML
    HAS_PT = True
except Exception:
    HAS_PT = False

# 防御性回退：某些 prompt_toolkit 版本可能没有 Completer/AutoSuggest 符号
if HAS_PT:
    try:
        Completer  # type: ignore
    except NameError:
        class Completer(object):  # type: ignore
            pass
    try:
        AutoSuggest  # type: ignore
    except NameError:
        class AutoSuggest(object):  # type: ignore
            pass


if HAS_PT:
    class _SQLCompleter(Completer):
        def __init__(self, database: SimpleDatabase):
            self.db = database
            self.keywords = [
                "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "CREATE", "TABLE",
                "UPDATE", "DELETE", "DROP", "TRUNCATE", "JOIN", "INNER", "LEFT", "RIGHT",
                "ON", "GROUP", "BY", "ORDER", "ASC", "DESC", "INDEX", "UNIQUE", "VIEW", "AS",
                "COUNT", "SUM", "AVG", "MIN", "MAX",
            ]

        def get_completions(self, document: 'Document', complete_event):
            text = document.text_before_cursor
            word = document.get_word_before_cursor(WORD=True)
            if not word:
                # 提供顶层建议
                for kw in self.keywords:
                    yield Completion(kw, start_position=0)
                # 表名
                try:
                    for t in self.db.list_tables() or []:
                        yield Completion(t, start_position=0)
                except Exception:
                    pass
                return

            low = word.lower()
            # 关键字补全
            for kw in self.keywords:
                if kw.lower().startswith(low):
                    yield Completion(kw, start_position=-len(word))
            # 表名补全
            try:
                for t in self.db.list_tables() or []:
                    if t.lower().startswith(low):
                        yield Completion(t, start_position=-len(word))
            except Exception:
                pass

            # 简单列补全：如果文本包含一个已存在的表名，则补全该表列
            try:
                tables = set(self.db.list_tables() or [])
                for t in tables:
                    if t.lower() in text.lower():
                        schema = self.db.catalog.get_table_schema(t)
                        if schema:
                            for col in schema.columns:
                                name = col.name
                                # 支持裸列与 table.col
                                if name.lower().startswith(low):
                                    yield Completion(name, start_position=-len(word))
                                dotted = f"{t}.{name}"
                                if dotted.lower().startswith(low):
                                    yield Completion(dotted, start_position=-len(word))
            except Exception:
                pass


    class _InlineSuggest(AutoSuggest):
        def __init__(self, database: SimpleDatabase):
            self.db = database
            self.seed_words = [
                "help", "tables", "views", "stats", "indexes", "describe ", "show ",
                "CREATE TABLE ", "SELECT ", "INSERT INTO ", "UPDATE ", "DELETE FROM ",
            ]

        def get_suggestion(self, buffer, document: 'Document'):
            text = document.text_before_cursor
            if not text:
                return None
            # 基于固定词典的灰色联想（当输入是前缀时补足建议）
            for w in self.seed_words:
                if w.lower().startswith(text.lower()) and w.lower() != text.lower():
                    from prompt_toolkit.auto_suggest import Suggestion
                    return Suggestion(w[len(text):])
            # 针对表名提供联想
            try:
                for t in self.db.list_tables() or []:
                    if t.lower().startswith(text.lower()) and t.lower() != text.lower():
                        from prompt_toolkit.auto_suggest import Suggestion
                        return Suggestion(t[len(text):])
            except Exception:
                pass
            return None
else:
    _SQLCompleter = None
    _InlineSuggest = None


class SQLShell:
    """SQL交互式Shell"""

    def __init__(self, database: SimpleDatabase):
        self.database = database
        self.running = True
        self._pt_session = None
        if HAS_PT:
            try:
                self._pt_session = PromptSession(
                    completer=_SQLCompleter(database) if _SQLCompleter else None,
                    auto_suggest=_InlineSuggest(database) if _InlineSuggest else None,
                )
            except Exception:
                self._pt_session = None

    def start(self):
        """启动Shell"""
        print("=" * 60)
        print("🗄️  欢迎使用简化版数据库系统 SQL Shell")
        print("=" * 60)

        # 登录流程
        if not self._login():
            return

        print("输入 'help' 查看帮助，输入 'quit' 或 'exit' 退出")
        if HAS_PT and self._pt_session:
            print("提示: 支持灰色联想与补全，输入 'hel' 会提示 'help'")
        print()

        while self.running:
            try:
                user_input = self._get_input()
                if user_input:
                    self._process_command(user_input)
            except KeyboardInterrupt:
                self._safe_exit()
                break
            except EOFError:
                self._safe_exit()
                break

    def _login(self) -> bool:
        """登录流程"""
        print("请登录数据库系统:")
        max_attempts = 3
        attempts = 0

        while attempts < max_attempts:
            try:
                username = input("用户名: ").strip()
                if not username:
                    print("用户名不能为空")
                    continue

                import getpass

                password = getpass.getpass("密码: ")

                result = self.database.login(username, password)
                if result["success"]:
                    print(f"✅ {result['message']}")
                    return True
                else:
                    attempts += 1
                    print(f"❌ {result['message']}")
                    if attempts < max_attempts:
                        print(f"还有 {max_attempts - attempts} 次机会")

            except KeyboardInterrupt:
                print("\n登录已取消")
                return False

        print("登录失败次数过多，程序退出")
        return False
        
    def _get_input(self) -> Optional[str]:
        """获取用户输入（多行，空行提交）"""
        lines = []
        prompt_main = "SQL> "
        prompt_more = "...> "
        try:
            while True:
                if HAS_PT and self._pt_session:
                    line = self._pt_session.prompt(prompt_main if not lines else prompt_more)
                else:
                    line = input(prompt_main if not lines else prompt_more)
                # 只输入回车（空行）表示输入结束
                if not line.strip() and lines:
                    break
                # 首行和续行都允许注释
                line = line.split('#', 1)[0].rstrip()
                if line.strip():
                    lines.append(line)
        except EOFError:
            return None
        except KeyboardInterrupt:
            print()
            return None
        if not lines:
            return None
        return " ".join(lines)

    def _process_command(self, command: str):
        """处理命令 - 添加用户管理命令"""
        command = command.strip()
        if not command:
            return

        # 支持#注释，自动忽略#及其后内容
        if '#' in command:
            command = command.split('#', 1)[0].rstrip()

        if not command:
            return

        # 用户管理命令
        if command.lower() == "users":
            self._show_users()
            return

        if command.lower().startswith("show privileges "):
            username = command.split()[2]
            self._show_user_privileges(username)
            return

        if command.lower() == "whoami":
            current_user = self.database.get_current_user()
            print(f"当前登录用户: {current_user}")
            return

        if command.lower() == "logout":
            result = self.database.logout()
            print(f"✅ {result['message']}")
            # 重新登录
            if self._login():
                return
            else:
                self.running = False
                return

        # 视图管理命令
        if command.lower() in ("views", "show views"):
            views = (
                self.database.list_views()
                if hasattr(self.database, "list_views")
                else []
            )
            if not views:
                print("数据库中没有视图")
            else:
                print(f"数据库中的视图 ({len(views)} 个):")
                for view in views:
                    print(f"  👁️  {view}")
            return

        if command.lower().startswith("describe view "):
            view_name = command.split()[2]
            if hasattr(self.database, "get_view_definition"):
                definition = self.database.get_view_definition(view_name)
                if definition:
                    print(f"视图 '{view_name}' 的定义: {definition}")
                else:
                    print(f"视图 '{view_name}' 不存在")
            else:
                print("当前数据库不支持视图定义查询")
            return

        if command.lower().startswith("show view "):
            parts = command.split()
            if len(parts) >= 3:
                alias = f"describe view {parts[2]}"
                self._process_command(alias)
                return

        if command.lower().startswith("drop view "):
            view_name = command.split()[2]
            result = self.database.execute_sql(f"DROP VIEW {view_name}")
            format_query_result(result)
            return

        # 会话管理命令
        if command.startswith("\\session"):
            parts = command.split()
            if len(parts) == 1 or parts[1] == "list":
                sessions = self.database.list_sessions()
                print("Sessions:")
                for s in sessions:
                    star = "*" if s["current"] else " "
                    print(
                        f"  {star} [{s['id']}] sid={s['session_id']} autocommit={'1' if s['autocommit'] else '0'} in_txn={'1' if s['in_txn'] else '0'} iso={s['isolation']}"
                    )
                return
            elif parts[1] == "new":
                idx = self.database.new_session()
                print(f"新建会话: {idx}")
                return
            elif parts[1] == "use" and len(parts) >= 3:
                try:
                    idx = int(parts[2])
                    if self.database.use_session(idx):
                        print(f"切换到会话: {idx}")
                    else:
                        print("无效的会话编号")
                except ValueError:
                    print("请输入有效的会话编号")
                return
            else:
                print("用法: \\session [list|new|use <id>]")
                return

        # 系统控制命令
        if command.lower() in ("quit", "exit"):
            print("正在保存数据...")
            try:
                self.database.flush_all()
                print("💾 数据已保存，再见！")
            except Exception as e:
                print(f"⚠️ 保存数据时出错: {e}")
            self.running = False
            return

        if command.lower() in ("help", "?"):
            self._show_help()
            return

        if command.lower() == "help sql":
            self._show_help_sql()
            return
        if command.lower() == "help views":
            self._show_help_views()
            return

        if command.lower() == "clear":
            print("\033[2J\033[H", end="")  # 清屏
            return

        # 数据库查询命令
        if command.lower() == "tables":
            self._show_tables()
            return

        if command.lower().startswith("describe ") or command.lower().startswith(
            "desc "
        ):
            table_name = command.split()[1]
            self._describe_table(table_name)
            return

        if command.lower().startswith("show "):
            parts = command.split()
            if len(parts) >= 2:
                table_name = parts[1]
                self._show_table_data(table_name)
                return

        if command.lower().startswith("indexes"):
            parts = command.split()
            table_name = parts[1] if len(parts) > 1 else None
            self._show_indexes(table_name)
            return

        if command.lower() == "stats":
            self._show_stats()
            return

        # 演示命令
        if command.lower() == "demo views":
            self._demo_views()
            return
        if command.lower() == "demo constraints":
            self._demo_constraints()
            return

        # 日志和缓存命令
        if command.lower().startswith("log level "):
            level = command.split()[2] if len(command.split()) > 2 else ""
            self._set_log_level(level)
            return

        if command.lower() == "log stats":
            self._show_log_stats()
            return

        if command.lower() == "cache stats":
            self._show_cache_stats()
            return

        # SQL语句处理
        # 支持多条SQL（英文分号分隔）依次执行
        if '；' in command:
            print("❌ 错误: 仅支持英文分号 ';' 作为语句结束符，检测到中文分号 '；'")
            return
        if not command.rstrip().endswith(';'):
            print("❌ 错误: SQL语句必须以英文分号 ';' 结尾")
            return
        stmts = [s.strip() for s in command.split(';') if s.strip()]
        for stmt in stmts:
            if not stmt:
                continue
            result = self.database.execute_sql(stmt + ';')
            format_query_result(result)
        return

        # 调试信息输出
        if (
            "DEBUG" in result.get("message", "")
            or "debug" in result.get("message", "").lower()
        ):
            print("[调试信息]", result.get("message"))
        print()  # 空行

        # 智能保存逻辑（保留事务支持）
        upper = command.upper()
        if upper.startswith(("CREATE", "INSERT", "UPDATE", "DELETE", "DROP")):
            try:
                exec_ref = getattr(self.database, "sql_executor", None)
                in_txn = exec_ref and exec_ref.txn.in_txn()
                autocommit = exec_ref and exec_ref.txn.autocommit()
            except Exception:
                in_txn = False
                autocommit = True
            if autocommit and not in_txn:
                self.database.flush_all()

        # 对于用户管理命令，强制保存
        if any(
            command.upper().startswith(cmd)
            for cmd in [
                "CREATE",
                "INSERT",
                "UPDATE",
                "DELETE",
                "DROP",
                "TRUNCATE",
                "GRANT",
                "REVOKE",
            ]
        ):
            try:
                self.database.flush_all()
                print("💾 数据已保存")
            except Exception as e:
                print(f"⚠️ 保存数据时出错: {e}")

        print()  # 空行

    def _show_table_data(self, table_name: str):
        """显示表的所有数据"""
        try:
            result = self.database.execute_sql(f"SELECT * FROM {table_name}")
            if result.get("success"):
                print(f"表 '{table_name}' 的数据:")
                format_query_result(result)
            else:
                print(f"❌ 错误: {result.get('message', '未知错误')}")
        except Exception as e:
            print(f"❌ 查询表数据时出错: {e}")

    def _show_users(self):
        """显示所有用户"""
        users = self.database.catalog.list_users()
        if not users:
            print("数据库中没有用户")
        else:
            print(f"数据库用户 ({len(users)} 个):")
            for user in users:
                print(f"  👤 {user}")

    def _show_user_privileges(self, username: str):
        """显示用户权限"""
        privileges = self.database.catalog.get_user_privileges(username)
        if not privileges:
            print(f"用户 {username} 没有任何权限")
        else:
            print(f"用户 {username} 的权限:")
            for table, privs in privileges.items():
                privs_str = ", ".join(privs)
                print(f"  📋 {table}: {privs_str}")

    def _safe_exit(self):
        """安全退出"""
        print("\n正在保存数据...")
        try:
            self.database.flush_all()
            self.database.logout()
            print("💾 数据已保存，再见！")
        except Exception as e:
            print(f"⚠️ 保存数据时出错: {e}")

    # 新增：SQL 帮助
    def _show_help_sql(self):
        print(
            """
            📋 SQL语句:
            CREATE TABLE table_name (col1 type, col2 type, ...)  - 创建表
            INSERT INTO table_name VALUES (val1, val2, ...)      - 插入数据
            SELECT columns FROM table_name [WHERE condition]     - 查询数据
            [JOIN ... ON ...]、聚合 COUNT/SUM/AVG/MIN/MAX        - 进阶查询
            GROUP BY col1, col2                                  - 分组聚合
            ORDER BY col [ASC|DESC], col2 [ASC|DESC]             - 排序
            UPDATE table_name SET col=val [WHERE ...]            - 更新数据
            DELETE FROM table_name [WHERE ...]                   - 删除数据
            CREATE INDEX idx ON table (column)                   - 创建索引
            CREATE UNIQUE INDEX idx ON table (column)            - 创建唯一索引
            DROP INDEX idx                                       - 删除索引
            CREATE VIEW v AS <select>                            - 创建视图
            DROP VIEW v                                          - 删除视图
            """
        )

    # 新增：视图命令帮助
    def _show_help_views(self):
        print(
            """
            👁️ 视图命令:
            views | show views           - 列出所有视图
            describe view <name>         - 查看视图定义
            show view <name>             - 别名，与上等价
            CREATE VIEW v AS <select>    - 创建视图
            DROP VIEW v                  - 删除视图
            示例:
            CREATE VIEW adult AS SELECT id, name FROM users WHERE age >= 18;
            CREATE VIEW alice AS SELECT * FROM adult WHERE name = 'Alice';
            SELECT * FROM alice;
            """
        )

    # 新增：演示 - 视图
    def _demo_views(self):
        print("\n=== DEMO: 视图与嵌套视图 ===")
        # 预清理：尽力删除视图并清空用户表（在不支持 DROP TABLE 的环境）
        preclean = [
            "DROP VIEW alice_only",
            "DROP VIEW adult_users",
            "DELETE FROM users",
        ]
        # 调试：列出当前表
        try:
            if hasattr(self.database, "list_tables"):
                tbls = self.database.list_tables() or []
                print(f"[DEMO DEBUG] 预清理前的表: {tbls}")
        except Exception as e:
            print(f"[DEMO DEBUG] 列表表失败: {e}")
        for sql in preclean:
            try:
                res0 = self.database.execute_sql(sql)
                print(f"SQL> {sql}")
                format_query_result(res0)
            except Exception as e:
                print(f"SQL> {sql}")
                print(f"    预期或可忽略错误: {e}")
        # 额外：若接口支持列表表名，则在存在 users 表时再尝试清空一次
        try:
            if hasattr(self.database, "list_tables"):
                tables = self.database.list_tables() or []
                if any(t.lower() == "users" for t in tables):
                    sql = "DELETE FROM users"
                    resx = self.database.execute_sql(sql)
                    print(f"SQL> {sql}")
                    format_query_result(resx)

                    # 调试：再验计数
                    try:
                        chk2 = self.database.execute_sql("SELECT COUNT(*) FROM users")
                        print("SQL> SELECT COUNT(*) FROM users")
                        format_query_result(chk2)
                    except Exception as e:
                        print(f"[DEMO DEBUG] 二次计数失败(可忽略): {e}")

        except Exception as e:
            print("    预清理(users)检查失败，可忽略:", e)

        steps = [
            "DROP VIEW alice_only",
            "DROP VIEW adult_users",
            "DROP TABLE users",
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
            "INSERT INTO users VALUES (1, 'Alice', 20)",
            "INSERT INTO users VALUES (2, 'Bob', 17)",
            "INSERT INTO users VALUES (3, 'Carol', 25)",
            "CREATE VIEW adult_users AS SELECT id, name FROM users WHERE age >= 18",
            "SELECT * FROM adult_users",
            "CREATE VIEW alice_only AS SELECT * FROM adult_users WHERE name = 'Alice'",
            "SELECT * FROM alice_only",
        ]
        for sql in steps:
            try:
                res = self.database.execute_sql(sql)
                print(f"SQL> {sql}")
                format_query_result(res)
                # 处理不抛异常但失败的情况：
                if not res.get("success", True):
                    upper_sql = sql.strip().upper()
                    # 不支持 DROP TABLE 时，尝试清空表数据
                    if upper_sql == "DROP TABLE USERS":
                        try:
                            alt = "DELETE FROM users"
                            res2 = self.database.execute_sql(alt)
                            print(f"SQL> {alt}")
                            format_query_result(res2)
                        except Exception as ee:
                            print(f"    无法清空表 users: {ee}")
                    # CREATE TABLE 已存在，则清空旧数据以便重复演示
                    if upper_sql.startswith("CREATE TABLE USERS"):
                        msg = res.get("message", "") or res.get("error", "")
                        if "已存在" in msg or "存在" in msg:
                            try:
                                alt = "DELETE FROM users"
                                res3 = self.database.execute_sql(alt)
                                print(f"SQL> {alt}")
                                format_query_result(res3)
                            except Exception as ee:
                                print(f"    无法清空表 users: {ee}")
            except Exception as e:
                print(f"SQL> {sql}")
                print(f"    预期或可忽略错误: {e}")
                # 兼容：不支持 DROP TABLE 时，尝试清空表数据
                if sql.strip().upper() == "DROP TABLE USERS":
                    try:
                        alt = "DELETE FROM users"
                        res2 = self.database.execute_sql(alt)
                        print(f"SQL> {alt}")
                        format_query_result(res2)
                    except Exception as ee:
                        print(f"    无法清空表 users: {ee}")
        # 清理
        for sql in [
            "DROP VIEW alice_only",
            "DROP VIEW adult_users",
            "DELETE FROM users",
            "DROP TABLE users",
        ]:
            try:
                res = self.database.execute_sql(sql)
                print(f"SQL> {sql}")
                format_query_result(res)
            except Exception as e:
                print(f"SQL> {sql}")
                print(f"    预期或可忽略错误: {e}")
                if sql.strip().upper() == "DROP TABLE USERS":
                    try:
                        alt = "DELETE FROM users"
                        res2 = self.database.execute_sql(alt)
                        print(f"SQL> {alt}")
                        format_query_result(res2)
                    except Exception as ee:
                        print(f"    无法清空表 users: {ee}")
        print()

    # 新增：演示 - DEFAULT / CHECK / FOREIGN KEY
    def _demo_constraints(self):
        print("\n=== DEMO: DEFAULT / CHECK / FOREIGN KEY ===")
        # 清理可能存在的残留
        cleanup = [
            "DROP TABLE products_fk",
            "DROP TABLE categories",
            "DROP TABLE products",
            "DROP TABLE t1",
        ]
        for sql in cleanup:
            try:
                res = self.database.execute_sql(sql)
                print(f"SQL> {sql}")
                format_query_result(res)
            except Exception:
                pass
        # DEFAULT
        seq = [
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name VARCHAR(20) DEFAULT 'unknown', age INTEGER DEFAULT 18)",
            "INSERT INTO t1 (id) VALUES (1)",
            "SELECT * FROM t1",
        ]
        for sql in seq:
            res = self.database.execute_sql(sql)
            print(f"SQL> {sql}")
            format_query_result(res)
        # CHECK
        seq2 = [
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL CHECK (price > 0), stock INTEGER CHECK (stock >= 0))",
            "INSERT INTO products VALUES (1, 'Apple', 1.5, 100)",
            "INSERT INTO products VALUES (2, 'Banana', 0.8, 0)",
        ]
        for sql in seq2:
            res = self.database.execute_sql(sql)
            print(f"SQL> {sql}")
            format_query_result(res)
        # 预期失败
        for sql in [
            "INSERT INTO products VALUES (3, 'Lemon', -1, 50)",
            "INSERT INTO products VALUES (4, 'Orange', 2.0, -10)",
        ]:
            try:
                res = self.database.execute_sql(sql)
                print(f"SQL> {sql}")
                if res.get("success"):
                    print("    ❌ 预期失败，但成功了")
                else:
                    print(f"    ✅ 预期失败: {res.get('message')}")
            except Exception as e:
                print(f"SQL> {sql}")
                print(f"    ✅ 抛出异常(符合预期): {e}")
        # FOREIGN KEY
        seq3 = [
            "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
            "CREATE TABLE products_fk (id INTEGER PRIMARY KEY, name TEXT, category_id INTEGER, FOREIGN KEY (category_id) REFERENCES categories(id))",
            "INSERT INTO categories VALUES (10, 'Fruits')",
            "INSERT INTO categories VALUES (20, 'Vegetables')",
            "INSERT INTO products_fk VALUES (101, 'Apple', 10)",
            "INSERT INTO products_fk VALUES (102, 'Orphan Product', NULL)",
        ]
        for sql in seq3:
            res = self.database.execute_sql(sql)
            print(f"SQL> {sql}")
            format_query_result(res)
        # 预期失败：不存在的外键
        for sql in [
            "INSERT INTO products_fk VALUES (103, 'Cabbage', 99)",
            "UPDATE products_fk SET category_id = 88 WHERE id = 101",
        ]:
            try:
                res = self.database.execute_sql(sql)
                print(f"SQL> {sql}")
                if res.get("success"):
                    print("    ❌ 预期失败，但成功了")
                else:
                    print(f"    ✅ 预期失败: {res.get('message')}")
            except Exception as e:
                print(f"SQL> {sql}")
                print(f"    ✅ 抛出异常(符合预期): {e}")
        print()

    def _set_log_level(self, level: str):
        """设置日志级别"""
        if not level:
            print("请指定日志级别: DEBUG, INFO, WARNING, ERROR, CRITICAL")
            return

        result = self.database.set_log_level(level)
        if result.get("success"):
            print(f"✅ {result['message']}")
        else:
            print(f"❌ {result['message']}")

    def _show_log_stats(self):
        """显示日志统计信息"""
        try:
            log_stats = self.database.get_log_stats()
            print("📊 日志统计信息:")
            print(f"  日志文件: {log_stats['log_file']}")
            print(f"  当前日志级别: {log_stats['current_log_level']}")
            print(f"  缓存命中次数: {log_stats['cache_hits']}")
            print(f"  缓存未命中次数: {log_stats['cache_misses']}")
            print(f"  缓存命中率: {log_stats['hit_rate']:.2%}")
        except Exception as e:
            print(f"❌ 获取日志统计失败: {e}")

    def _show_cache_stats(self):
        """显示缓存详细统计"""
        try:
            cache_stats = self.database.buffer_manager.get_detailed_stats()
            print("🗂️ 缓存详细统计:")
            print(f"  缓存大小: {cache_stats['cache_size']} 页")
            print(f"  已缓存页面: {cache_stats['cached_pages']} 页")
            print(f"  空闲槽位: {cache_stats['free_slots']} 页")
            print(f"  固定页面: {cache_stats['pinned_pages']} 页")
            print(f"  脏页面: {cache_stats['dirty_pages']} 页")
            print(f"  缓存命中: {cache_stats['cache_hits']} 次")
            print(f"  缓存未命中: {cache_stats['cache_misses']} 次")
            print(f"  总请求: {cache_stats['total_requests']} 次")
            print(f"  命中率: {cache_stats['hit_rate']:.2%}")
        except Exception as e:
            print(f"❌ 获取缓存统计失败: {e}")

    def _show_help(self):
        """显示帮助信息 - 添加用户管理帮助"""
        print(
            """
📚 MiniSQL 命令帮助

👤 用户管理:
CREATE USER username IDENTIFIED BY 'password'        - 创建用户
DROP USER username                                   - 删除用户
GRANT privilege ON table TO user                     - 授权
REVOKE privilege ON table FROM user                  - 撤权
users                                               - 列出所有用户
show privileges username                            - 查看用户权限
whoami                                             - 显示当前用户
logout                                             - 登出并重新登录

🔐 权限类型:
SELECT, INSERT, UPDATE, DELETE                      - 数据操作权限
CREATE, DROP                                        - 结构操作权限
ALL                                                 - 所有权限

💡 示例:
CREATE USER alice IDENTIFIED BY 'password123';
GRANT SELECT ON users TO alice;
GRANT ALL ON products TO alice;
REVOKE INSERT ON products FROM alice;

📋 SQL语句:
CREATE TABLE table_name (col1 type, col2 type, ...)  - 创建表
INSERT INTO table_name VALUES (val1, val2, ...)      - 插入数据
SELECT columns FROM table_name [WHERE condition]     - 查询数据
UPDATE table_name SET col=val [WHERE condition]      - 更新数据
DELETE FROM table_name [WHERE condition]             - 删除数据
DROP TABLE table_name                                - 删除表（包括结构和数据）
TRUNCATE TABLE table_name                            - 快速清空表数据（保留结构）

🔄 事务管理:
BEGIN | START TRANSACTION                            - 开启事务
COMMIT                                               - 提交事务
ROLLBACK                                             - 回滚事务（当前未实现）
SET AUTOCOMMIT = 0|1                                 - 设置自动提交
SET SESSION TRANSACTION ISOLATION LEVEL ...          - 设置隔离级别

🧭 会话管理:
\\session list                                       - 列出会话
\\session new                                        - 新建会话
\\session use <id>                                   - 切换会话

🔍 索引操作:
CREATE INDEX index_name ON table_name (column)       - 创建索引
CREATE UNIQUE INDEX idx_name ON table_name (column)  - 创建唯一索引
DROP INDEX index_name                                - 删除索引

📊 系统命令:
tables                     - 列出所有表
describe <table>           - 查看表结构 (可简写为 desc)
show <table>               - 查看表数据内容 (等同于 SELECT * FROM table)
indexes [table_name]       - 查看索引信息
stats                      - 显示数据库统计信息
views | show views         - 列出所有视图
describe view <name>       - 查看视图定义
show view <name>           - 别名，与上等价

📝 日志命令:
log level <LEVEL>          - 设置日志级别 (DEBUG/INFO/WARNING/ERROR/CRITICAL)
log stats                  - 显示日志统计信息
cache stats                - 显示详细缓存统计信息

🛠️ 其他命令:
help, ?                    - 显示此帮助
tables                     - 列出所有表
describe <table>           - 查看表结构
stats                      - 显示数据库统计信息
quit, exit                 - 退出Shell

💡 数据类型:
INTEGER          整数
VARCHAR(n)       字符串，最大长度n
FLOAT            浮点数
BOOLEAN          布尔值 (TRUE/FALSE)
CHAR(n)          固定长度字符串
DECIMAL(p,s)     精确小数
DATE             日期类型
TIME             时间类型
DATETIME         日期时间类型
BIGINT           64位整数
TINYINT          8位整数
TEXT             长文本

🧮 聚合函数:
COUNT(expr|*)    计数；COUNT(*) 统计行数，COUNT(expr) 忽略NULL
SUM(expr)        求和（忽略NULL）
AVG(expr)        平均值（忽略NULL）
MIN(expr)        最小值（忽略NULL）
MAX(expr)        最大值（忽略NULL）
示例: SELECT dept_id, COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary) FROM emp GROUP BY dept_id;

🔒 约束:
PRIMARY KEY      主键
NOT NULL         非空
NULL             允许为空
UNIQUE           唯一值
DEFAULT value    默认值
CHECK (condition) 检查约束
FOREIGN KEY      外键
提示: 输入 'demo constraints' 可运行 DEFAULT/CHECK/FOREIGN KEY 演示

⚠️ DROP vs TRUNCATE 对比:
DROP TABLE       - 完全删除表（结构+数据+索引），无法恢复
TRUNCATE TABLE   - 快速清空数据，保留表结构和索引定义
DELETE FROM      - 逐行删除数据，可加WHERE条件，相对较慢

👁️ 视图提示:
提示: 输入 'help views' 查看视图命令说明；输入 'demo views' 可运行视图演示


💡 示例:
CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL);
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
show users                           -- 查看表数据
UPDATE users SET name = 'NewName' WHERE id = 1;
DELETE FROM users WHERE id = 2;
TRUNCATE TABLE users;                -- 清空所有数据但保留表结构
DROP TABLE users;                    -- 完全删除表
CREATE INDEX idx_user_id ON users (id);
log level DEBUG                      -- 设置调试级别日志
cache stats                          -- 查看缓存详情

        """
        )

    def _show_tables(self):
        """显示所有表"""
        tables = self.database.list_tables()
        if not tables:
            print("数据库中没有表")
        else:
            print(f"数据库中的表 ({len(tables)} 个):")
            for table in tables:
                print(f"  📋 {table}")

    def _describe_table(self, table_name: str):
        """显示表结构"""
        table_info = self.database.get_table_info(table_name)
        format_table_info(table_info)

    def _show_indexes(self, table_name: Optional[str] = None):
        """显示索引信息"""
        if table_name:
            indexes = self.database.list_indexes(table_name)
            print(f"表 '{table_name}' 的索引:")
            if indexes.get("success") and indexes.get("indexes"):
                for idx in indexes["indexes"]:
                    unique_flag = " (UNIQUE)" if idx.get("is_unique") else ""
                    print(
                        f"  🔍 {idx['index_name']} -> {idx['column_name']}{unique_flag}"
                    )
            else:
                print("  (无索引)")
        else:
            # 显示所有表的索引
            tables = self.database.list_tables()
            print("所有索引:")
            total_indexes = 0
            for table in tables:
                indexes = self.database.list_indexes(table)
                if indexes.get("success") and indexes.get("indexes"):
                    for idx in indexes["indexes"]:
                        unique_flag = " (UNIQUE)" if idx.get("is_unique") else ""
                        print(
                            f"  🔍 {idx['index_name']} -> {table}.{idx['column_name']}{unique_flag}"
                        )
                        total_indexes += 1
            if total_indexes == 0:
                print("  (无索引)")

    def _show_stats(self):
        """显示数据库统计信息"""
        stats = self.database.get_database_stats()
        format_database_stats(stats)


def interactive_sql_shell(database: SimpleDatabase):
    """启动交互式SQL Shell"""
    shell = SQLShell(database)
    shell.start()
