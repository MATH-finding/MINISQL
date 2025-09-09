#!/usr/bin/env python3
"""
简化版数据库系统主程序
"""

from interface import SimpleDatabase, interactive_sql_shell, format_query_result
import sys
import os


def run_demo():
    """运行演示程序"""
    print("🗄️  简化版数据库系统演示")
    print("=" * 40)
    db = SimpleDatabase("demo.db")

    try:
        # 设置演示数据
        setup_commands = [
            """CREATE TABLE students (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                age INTEGER,
                gpa FLOAT,
                active BOOLEAN
            )""",
            "INSERT INTO students VALUES (1, '张三', 20, 3.8, TRUE)",
            "INSERT INTO students VALUES (2, '李四', 21, 3.5, TRUE)",
            "INSERT INTO students VALUES (3, '王五', 19, 3.9, FALSE)",
            "INSERT INTO students VALUES (4, '赵六', 22, 3.2, TRUE)",
        ]

        print("📝 正在创建演示数据...")
        for i, cmd in enumerate(setup_commands, 1):
            print(f"[{i}/{len(setup_commands)}] ", end="")
            result = db.execute_sql(cmd)
            if result.get("success", True):
                print(f"✅ {result['message']}")
            else:
                print(f"❌ {result['message']}")

        print("\n🔍 演示查询:")
        print("-" * 30)

        # 演示查询1
        print("1. 查询所有学生:")
        result = db.execute_sql("SELECT * FROM students")
        format_query_result(result)

        # 演示查询2
        print("\n2. 查询GPA大于3.6的学生:")
        result = db.execute_sql("SELECT name, age, gpa FROM students WHERE gpa > 3.6")
        format_query_result(result)

        # 演示查询3
        print("\n3. 查询活跃且年龄小于21的学生:")
        result = db.execute_sql(
            "SELECT * FROM students WHERE active = TRUE AND age < 21"
        )
        format_query_result(result)

        # 显示统计信息
        print("\n📊 数据库统计信息:")
        stats = db.get_database_stats()
        print(f"  表数量: {stats['tables_count']}")
        print(f"  文件大小: {stats['file_size_pages']} 页")
        print(f"  缓存命中率: {stats['cache_stats']['hit_rate']:.2%}")

        print("\n✨ 演示完成！")

    finally:
        db.close()
        # 清理演示文件
        if os.path.exists("demo.db"):
            os.remove("demo.db")
            print("🗑️  演示文件已清理")


def run_tests():
    """运行基本测试"""
    print("🧪 运行基本功能测试")
    print("=" * 30)

    db = SimpleDatabase("test.db")

    try:
        test_cases = [
            # 测试CREATE TABLE
            {
                "name": "创建表",
                "sql": "CREATE TABLE test (id INTEGER PRIMARY KEY, name VARCHAR(20) NOT NULL)",
                "expect_success": True,
            },
            # 测试INSERT
            {
                "name": "插入数据",
                "sql": "INSERT INTO test VALUES (1, 'test1')",
                "expect_success": True,
            },
            # 测试SELECT
            {"name": "查询数据", "sql": "SELECT * FROM test", "expect_success": True},
            # 测试WHERE条件
            {
                "name": "WHERE条件查询",
                "sql": "SELECT * FROM test WHERE id = 1",
                "expect_success": True,
            },
        ]

        passed = 0
        total = len(test_cases)

        for test in test_cases:
            print(f"测试: {test['name']}")
            result = db.execute_sql(test["sql"])
            success = result.get("success", True)

            if success == test["expect_success"]:
                print("  ✅ 通过")
                passed += 1
            else:
                print(f"  ❌ 失败: {result.get('message', '未知错误')}")

        print(f"\n测试结果: {passed}/{total} 通过")

    finally:
        db.close()
        if os.path.exists("test.db"):
            os.remove("test.db")


def run_demo_with_indexes():
    """运行带索引功能的演示程序"""
    print("🗄️  带索引功能的数据库系统演示")
    print("=" * 40)
    db = SimpleDatabase("demo_with_indexes.db")

    try:
        # 现有的设置演示数据代码...
        setup_commands = [
            """CREATE TABLE students (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                age INTEGER,
                gpa FLOAT,
                active BOOLEAN
            )""",
            "INSERT INTO students VALUES (1, '张三', 20, 3.8, TRUE)",
            "INSERT INTO students VALUES (2, '李四', 21, 3.5, TRUE)",
            "INSERT INTO students VALUES (3, '王五', 19, 3.9, FALSE)",
            "INSERT INTO students VALUES (4, '赵六', 22, 3.2, TRUE)",
        ]

        print("📝 正在创建演示数据...")
        for i, cmd in enumerate(setup_commands, 1):
            print(f"[{i}/{len(setup_commands)}] ", end="")
            result = db.execute_sql(cmd)
            if result.get("success", True):
                print(f"✅ {result['message']}")
            else:
                print(f"❌ {result['message']}")

        # 新增：索引操作演示
        print("\n📚 索引操作演示:")
        print("-" * 30)

        # 创建索引
        index_commands = [
            "CREATE INDEX idx_student_age ON students (age)",
            "CREATE UNIQUE INDEX idx_student_id ON students (id)",
        ]

        for cmd in index_commands:
            result = db.execute_sql(cmd)
            print(f"✅ {result.get('message', result)}")

        # 显示索引信息
        indexes = db.list_indexes("students")
        print(f"students表的索引: {indexes}")

        # 原有的查询演示...
        print("\n🔍 演示查询(现在会自动使用索引优化):")
        print("-" * 30)

        # 演示查询1 - 会使用索引
        print("1. 按ID查询(使用索引):")
        result = db.execute_sql("SELECT * FROM students WHERE id = 1")
        format_query_result(result)

        # 演示查询2 - 会使用索引
        print("\n2. 按年龄查询(使用索引):")
        result = db.execute_sql("SELECT name, gpa FROM students WHERE age = 20")
        format_query_result(result)

        # 显示统计信息
        print("\n📊 数据库统计信息:")
        stats = db.get_database_stats()
        print(f"  表数量: {stats['tables_count']}")
        print(f"  索引数量: {stats['indexes_count']}")  # 新增
        print(f"  文件大小: {stats['file_size_pages']} 页")
        print(f"  缓存命中率: {stats['cache_stats']['hit_rate']:.2%}")

        print("\n✨ 演示完成！")

    finally:
        db.close()
        # 清理演示文件
        if os.path.exists("demo_with_indexes.db"):
            os.remove("demo_with_indexes.db")
            print("🗑️  演示文件已清理")


# 修改main函数，添加新的演示选项
def main():
    """主程序"""
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()

        if command == "demo":
            run_demo()
            return
        elif command == "demo-index":  # 新增
            run_demo_with_indexes()
            return
        elif command == "test":
            run_tests()
            return
        elif command == "shell":
            db_file = sys.argv[2] if len(sys.argv) > 2 else "database.db"
            print(f"🗄️  启动 SQL Shell，使用数据库: {db_file}")
            db = SimpleDatabase(db_file)
            try:
                interactive_sql_shell(db)
            finally:
                db.close()
            return

    # 显示使用说明
    print("🗄️  简化版数据库系统")
    print("=" * 40)
    print("用法:")
    print("  python main.py demo           # 运行功能演示")
    print("  python main.py demo-index     # 运行带索引功能演示")  # 新增
    print("  python main.py test           # 运行基本测试")
    print("  python main.py shell          # 启动交互式Shell")
    print("  python main.py shell <file>   # 使用指定数据库文件")
    print()
    print("示例:")
    print("  python main.py demo")
    print("  python main.py demo-index")  # 新增
    print("  python main.py shell mydb.db")


if __name__ == "__main__":
    main()
