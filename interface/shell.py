"""
交互式SQL Shell
"""

import sys
from typing import Optional
from .database import SimpleDatabase
from .formatter import format_query_result, format_table_info, format_database_stats


class SQLShell:
    """SQL交互式Shell"""

    def __init__(self, database: SimpleDatabase):
        self.database = database
        self.running = True

    def start(self):
        """启动Shell"""
        print("=" * 60)
        print("🗄️  欢迎使用简化版数据库系统 SQL Shell")
        print("=" * 60)
        print("输入 'help' 查看帮助，输入 'quit' 或 'exit' 退出")
        print()

        while self.running:
            try:
                user_input = self._get_input()
                if user_input:
                    self._process_command(user_input)
            except KeyboardInterrupt:
                print("\n正在保存数据...")
                try:
                    self.database.flush_all()
                    print("💾 数据已保存，再见！")
                except Exception as e:
                    print(f"⚠️ 保存数据时出错: {e}")
                break
            except EOFError:
                print("\n正在保存数据...")
                try:
                    self.database.flush_all()
                    print("💾 数据已保存，再见！")
                except Exception as e:
                    print(f"⚠️ 保存数据时出错: {e}")
                break

    def _get_input(self) -> Optional[str]:
        """获取用户输入"""
        try:
            line = input("SQL> ").strip()

            # 处理多行输入
            if line and not line.endswith(";"):
                lines = [line]
                while True:
                    continuation = input("...> ").strip()
                    if not continuation:
                        break
                    lines.append(continuation)
                    if continuation.endswith(";"):
                        break
                line = " ".join(lines)

            return line
        except:
            return None

    def _process_command(self, command: str):
        """处理命令"""
        command = command.strip()

        if not command:
            return

        # 会话管理命令（以反斜杠开头）
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

        # 内置命令
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

        if command.lower() == "tables":
            self._show_tables()
            return

        if command.lower().startswith("describe ") or command.lower().startswith(
            "desc "
        ):
            table_name = command.split()[1]
            self._describe_table(table_name)
            return

        # show table_name 命令
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

        # 新增：日志相关命令
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

        if command.lower() == "clear":
            print("\033[2J\033[H", end="")  # 清屏
            return

        # SQL命令
        if command.endswith(";"):
            command = command[:-1]  # 移除分号

        print()  # 空行
        result = self.database.execute_sql(command)
        format_query_result(result)

        # 对于修改数据的操作，事务中或 autocommit=0 时不强制保存
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
        # 对于所有可能修改数据的操作，都强制保存
        if any(
            command.upper().startswith(cmd)
            for cmd in ["CREATE", "INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE"]
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
        """显示帮助信息"""
        print(
            """
    📚 MiniSQL 命令帮助

    📋 SQL语句:
    CREATE TABLE table_name (col1 type, col2 type, ...)  - 创建表
    DROP TABLE table_name                                - 删除表（包括结构和数据）
    INSERT INTO table_name VALUES (val1, val2, ...)      - 插入数据
    SELECT columns FROM table_name [WHERE condition]     - 查询数据
    UPDATE table_name SET col=val [WHERE condition]      - 更新数据
    DELETE FROM table_name [WHERE condition]             - 删除数据
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
    
    📝 日志命令:
    log level <LEVEL>          - 设置日志级别 (DEBUG/INFO/WARNING/ERROR/CRITICAL)
    log stats                  - 显示日志统计信息
    cache stats                - 显示详细缓存统计信息
    
    🛠️ 其他命令:
    help, ?                    - 显示此帮助
    clear                      - 清屏
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

    🔒 约束:
    PRIMARY KEY      主键
    NOT NULL         非空
    NULL             允许为空
    UNIQUE           唯一值
    DEFAULT value    默认值
    CHECK (condition) 检查约束
    FOREIGN KEY      外键

    ⚠️ DROP vs TRUNCATE 对比:
    DROP TABLE       - 完全删除表（结构+数据+索引），无法恢复
    TRUNCATE TABLE   - 快速清空数据，保留表结构和索引定义
    DELETE FROM      - 逐行删除数据，可加WHERE条件，相对较慢

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
