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

        # 新增：show table_name 命令
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

        if command.lower() == "clear":
            print("\033[2J\033[H", end="")  # 清屏
            return

        # SQL命令
        if command.endswith(";"):
            command = command[:-1]  # 移除分号

        print()  # 空行
        result = self.database.execute_sql(command)
        format_query_result(result)

        # 对于所有可能修改数据的操作，都强制保存
        if any(
            command.upper().startswith(cmd)
            for cmd in ["CREATE", "INSERT", "UPDATE", "DELETE", "DROP"]
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

    def _show_help(self):
        """显示帮助信息"""
        print(
            """
📚 MiniSQL 命令帮助

📋 SQL语句:
  CREATE TABLE table_name (col1 type, col2 type, ...)  - 创建表
  INSERT INTO table_name VALUES (val1, val2, ...)      - 插入数据
  SELECT columns FROM table_name [WHERE condition]     - 查询数据
  UPDATE table_name SET col=value [WHERE condition]    - 更新数据
  DELETE FROM table_name [WHERE condition]             - 删除数据
  
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
  help, ?                    - 显示此帮助
  clear                      - 清屏
  quit, exit                 - 退出Shell
  
💡 数据类型:
  INTEGER          整数
  VARCHAR(n)       字符串，最大长度n
  FLOAT            浮点数
  BOOLEAN          布尔值 (TRUE/FALSE)

🔒 约束:
  PRIMARY KEY      主键
  NOT NULL         非空
  NULL            允许为空

💡 示例:
  CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50));
  INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
  show users                    -- 查看表数据
  UPDATE users SET name = 'NewName' WHERE id = 1;
  DELETE FROM users WHERE id = 2;
  CREATE INDEX idx_user_id ON users (id);
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
