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
                print("\n\n👋 再见！")
                break
            except EOFError:
                print("\n\n👋 再见！")
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

        # 内置命令
        if command.lower() in ("quit", "exit"):
            print("👋 再见！")
            self.running = False
            return

        if command.lower() == "help":
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

        if command.lower() == "stats":
            self._show_stats()
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
        print()  # 空行

    def _show_help(self):
        """显示帮助信息"""
        print(
            """
📚 SQL Shell 帮助
================

SQL 命令:
  CREATE TABLE table_name (col1 type, col2 type, ...)  创建表
  INSERT INTO table_name VALUES (val1, val2, ...)      插入数据
  SELECT * FROM table_name [WHERE condition]           查询数据

内置命令:
  help, ?          显示此帮助
  tables           列出所有表
  describe <table> 显示表结构 (可简写为 desc)
  stats            显示数据库统计信息
  clear            清屏
  quit, exit       退出

数据类型:
  INTEGER          整数
  VARCHAR(n)       字符串，最大长度n
  FLOAT            浮点数
  BOOLEAN          布尔值

约束:
  PRIMARY KEY      主键
  NOT NULL         非空

示例:
  CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL);
  INSERT INTO users VALUES (1, 'Alice');
  SELECT * FROM users WHERE id = 1;
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

    def _show_stats(self):
        """显示数据库统计信息"""
        stats = self.database.get_database_stats()
        format_database_stats(stats)


def interactive_sql_shell(database: SimpleDatabase):
    """启动交互式SQL Shell"""
    shell = SQLShell(database)
    shell.start()
