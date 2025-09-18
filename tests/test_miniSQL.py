"""
MiniSQL 综合测试脚本 - 修复版本
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from interface.database import SimpleDatabase
import traceback


class MiniSQLTester:
    def __init__(self):
        self.db = SimpleDatabase("test_database.db")
        self.test_results = []
        self.passed = 0
        self.failed = 0

        # 尝试多种可能的管理员密码
        admin_passwords = ["admin123", "admin", "password", "123456"]
        login_success = False

        for password in admin_passwords:
            login_result = self.db.login("admin", password)
            if login_result.get("success"):
                print(f"成功以admin身份登录，密码: {password}")
                login_success = True
                break

        if not login_success:
            print("警告: 无法以admin身份登录，某些测试可能失败")

    def execute_sql(self, sql_statement):
        """执行SQL语句并返回结果"""
        try:
            return self.db.execute_sql(sql_statement)
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"SQL执行异常: {str(e)}"
            }

    def assert_test(self, test_name, condition, message=""):
        """断言测试结果"""
        if condition:
            print(f"✅ PASS: {test_name}")
            self.passed += 1
        else:
            error_msg = f" - {message}" if message else ""
            print(f"❌ FAIL: {test_name}{error_msg}")
            self.failed += 1
        self.test_results.append((test_name, condition, message))

    def safe_get_count(self, result):
        """安全获取COUNT结果"""
        if not result.get("success"):
            return None

        data = result.get("data", [])
        if not data:
            return None

        first_row = data[0]
        # 尝试不同的COUNT字段名
        count_keys = ["COUNT", "count", "COUNT(*)", "count(*)"]
        for key in count_keys:
            if key in first_row:
                return first_row[key]

        # 如果是数字类型的值，可能直接返回了数字
        if len(first_row) == 1:
            return list(first_row.values())[0]

        return None

    def test_user_management(self):
        """测试用户管理功能"""
        print("\n=== 测试用户管理功能 ===")

        # 先清理可能存在的测试用户
        self.execute_sql("DROP USER testuser;")
        self.execute_sql("DROP USER alice;")

        # 创建用户
        result = self.execute_sql("CREATE USER testuser IDENTIFIED BY 'password123';")
        self.assert_test("创建用户", result.get("success"), result.get("message", ""))

        if result.get("success"):
            # 创建重复用户（应该失败）
            result = self.execute_sql("CREATE USER testuser IDENTIFIED BY 'password456';")
            self.assert_test("创建重复用户应该失败", not result.get("success"))

            # 删除用户
            result = self.execute_sql("DROP USER testuser;")
            self.assert_test("删除用户", result.get("success"))

        # 创建另一个用户
        result = self.execute_sql("CREATE USER alice IDENTIFIED BY 'alice123';")
        self.assert_test("创建用户alice", result.get("success"))

        # 删除不存在的用户（应该失败）
        result = self.execute_sql("DROP USER nonexistent;")
        self.assert_test("删除不存在用户应该失败", not result.get("success"))

    def test_table_operations(self):
        """测试表操作功能"""
        print("\n=== 测试表操作功能 ===")

        # 创建表
        create_sql = """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            age INTEGER,
            email VARCHAR(100) UNIQUE,
            salary FLOAT DEFAULT 5000.0,
            is_active BOOLEAN DEFAULT TRUE
        );
        """
        result = self.execute_sql(create_sql)
        self.assert_test(
            "创建users表",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 创建products表（用于外键测试）
        create_products_sql = """
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            price FLOAT,
            category VARCHAR(50)
        );
        """
        result = self.execute_sql(create_products_sql)
        self.assert_test(
            "创建products表",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 创建orders表（包含约束）
        create_orders_sql = """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product_id INTEGER,
            quantity INTEGER CHECK (quantity > 0),
            order_date DATE
        );
        """
        result = self.execute_sql(create_orders_sql)
        self.assert_test(
            "创建orders表",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

    def test_data_insertion(self):
        """测试数据插入功能"""
        print("\n=== 测试数据插入功能 ===")

        # 先清理可能存在的数据
        self.execute_sql("DELETE FROM users;")

        # 插入用户数据
        insert_users = [
            "INSERT INTO users (id, name, age, email) VALUES (1, 'John Doe', 25, 'john@example.com');",
            "INSERT INTO users (id, name, age, email, salary) VALUES (2, 'Jane Smith', 30, 'jane@example.com', 6000.0);",
            "INSERT INTO users (id, name, age, email) VALUES (3, 'Bob Wilson', 35, 'bob@example.com');",
            "INSERT INTO users (id, name, age, email) VALUES (4, 'Alice Brown', 28, 'alice@example.com');",
            "INSERT INTO users (id, name, age, email, is_active) VALUES (5, 'Charlie Davis', 22, 'charlie@example.com', FALSE);",
        ]

        success_count = 0
        for i, sql in enumerate(insert_users):
            result = self.execute_sql(sql)
            is_success = result.get("success")
            self.assert_test(
                f"插入用户数据{i + 1}",
                is_success,
                result.get("message", result.get("error", "")),
            )
            if is_success:
                success_count += 1

        # 插入产品数据
        insert_products = [
            "INSERT INTO products VALUES (1, 'Laptop', 1200.0, 'Electronics');",
            "INSERT INTO products VALUES (2, 'Mouse', 25.0, 'Electronics');",
            "INSERT INTO products VALUES (3, 'Desk', 300.0, 'Furniture');",
            "INSERT INTO products VALUES (4, 'Chair', 150.0, 'Furniture');",
        ]

        for i, sql in enumerate(insert_products):
            result = self.execute_sql(sql)
            self.assert_test(
                f"插入产品数据{i + 1}",
                result.get("success"),
                result.get("message", result.get("error", "")),
            )

        # 测试约束违反（只在有数据时测试）
        if success_count > 0:
            # 主键冲突
            result = self.execute_sql(
                "INSERT INTO users VALUES (1, 'Duplicate', 40, 'dup@example.com', 5000.0, TRUE);"
            )
            self.assert_test("主键冲突应该失败", not result.get("success"))

            # 唯一约束冲突
            result = self.execute_sql(
                "INSERT INTO users VALUES (6, 'Test', 40, 'john@example.com', 5000.0, TRUE);"
            )
            self.assert_test("唯一约束冲突应该失败", not result.get("success"))

        # NOT NULL约束违反
        result = self.execute_sql("INSERT INTO users (id, age) VALUES (6, 25);")
        self.assert_test("NOT NULL约束违反应该失败", not result.get("success"))

    def test_query_operations(self):
        """测试查询操作"""
        print("\n=== 测试查询操作 ===")

        # 基本SELECT
        result = self.execute_sql("SELECT * FROM users;")
        self.assert_test(
            "SELECT * 查询", result.get("success"), result.get("message", result.get("error", ""))
        )

        # 指定列查询
        result = self.execute_sql("SELECT name, age FROM users;")
        self.assert_test(
            "指定列查询",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # WHERE条件查询
        result = self.execute_sql("SELECT * FROM users WHERE age > 25;")
        self.assert_test(
            "WHERE条件查询",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 复合WHERE条件
        result = self.execute_sql(
            "SELECT * FROM users WHERE age > 25 AND is_active = TRUE;"
        )
        self.assert_test(
            "复合WHERE条件",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 不同操作符测试
        operators = ["=", "!=", "<", "<=", ">", ">="]
        for op in operators:
            result = self.execute_sql(f"SELECT * FROM users WHERE age {op} 30;")
            self.assert_test(
                f"操作符{op}测试",
                result.get("success"),
                result.get("message", result.get("error", "")),
            )

    def test_aggregate_functions(self):
        """测试聚合函数"""
        print("\n=== 测试聚合函数 ===")

        # 先确保有数据
        result = self.execute_sql("SELECT COUNT(*) FROM users;")
        user_count = self.safe_get_count(result)

        if user_count is None or user_count == 0:
            print("警告: users表中没有数据，跳过聚合函数测试")
            return

        # COUNT测试
        result = self.execute_sql("SELECT COUNT(*) FROM users;")
        actual_count = self.safe_get_count(result)
        self.assert_test("COUNT(*) 测试",
                         result.get("success") and actual_count is not None,
                         f"实际返回: {actual_count}")

        result = self.execute_sql("SELECT COUNT(age) FROM users;")
        self.assert_test(
            "COUNT(column) 测试",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 其他聚合函数测试
        aggregate_functions = ["SUM(age)", "AVG(age)", "MIN(age)", "MAX(age)"]

        for func in aggregate_functions:
            result = self.execute_sql(f"SELECT {func} FROM users;")
            self.assert_test(f"{func}测试",
                             result.get("success"),
                             result.get("message", result.get("error", "")))

    def test_update_delete(self):
        """测试更新和删除操作"""
        print("\n=== 测试更新和删除操作 ===")

        # UPDATE测试
        result = self.execute_sql(
            "UPDATE users SET salary = 7000.0 WHERE name = 'John Doe';"
        )
        self.assert_test(
            "UPDATE操作",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 验证更新结果
        result = self.execute_sql("SELECT salary FROM users WHERE name = 'John Doe';")
        success = (
                result.get("success")
                and len(result.get("data", [])) > 0
                and result.get("data")[0].get("salary") == 7000.0
        )
        self.assert_test(
            "验证UPDATE结果", success, result.get("message", result.get("error", ""))
        )

        # DELETE测试
        result = self.execute_sql("DELETE FROM users WHERE name = 'Charlie Davis';")
        self.assert_test(
            "DELETE操作",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 验证删除结果
        result = self.execute_sql("SELECT COUNT(*) FROM users;")
        actual_count = self.safe_get_count(result)
        expected_count = 4  # 原来5个，删除1个
        self.assert_test(
            "验证DELETE结果",
            actual_count == expected_count,
            f"期望{expected_count}条，实际{actual_count}条"
        )

    def test_index_operations(self):
        """测试索引操作"""
        print("\n=== 测试索引操作 ===")

        # 创建索引
        result = self.execute_sql("CREATE INDEX idx_user_email ON users (email);")
        self.assert_test(
            "创建普通索引",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 创建唯一索引
        result = self.execute_sql("CREATE UNIQUE INDEX idx_user_id ON users (id);")
        self.assert_test(
            "创建唯一索引",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 创建重复索引（应该失败）
        result = self.execute_sql("CREATE INDEX idx_user_email ON users (email);")
        self.assert_test("创建重复索引应该失败", not result.get("success"))

        # 使用索引查询
        result = self.execute_sql(
            "SELECT * FROM users WHERE email = 'john@example.com';"
        )
        self.assert_test(
            "索引查询",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 删除索引
        result = self.execute_sql("DROP INDEX idx_user_email;")
        self.assert_test(
            "删除索引",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 删除不存在的索引（应该失败）
        result = self.execute_sql("DROP INDEX nonexistent_index;")
        self.assert_test("删除不存在索引应该失败", not result.get("success"))

    def test_view_operations(self):
        """测试视图操作"""
        print("\n=== 测试视图操作 ===")

        # 创建简单视图
        create_view_sql = (
            "CREATE VIEW adult_users AS SELECT * FROM users WHERE age >= 25;"
        )
        result = self.execute_sql(create_view_sql)
        self.assert_test(
            "创建视图",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 查询视图
        result = self.execute_sql("SELECT * FROM adult_users;")
        self.assert_test(
            "查询视图",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 在视图上使用WHERE
        result = self.execute_sql("SELECT * FROM adult_users WHERE age > 30;")
        self.assert_test(
            "视图WHERE查询",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 创建复杂视图
        create_complex_view_sql = "CREATE VIEW user_summary AS SELECT name, age, salary FROM users WHERE is_active = TRUE;"
        result = self.execute_sql(create_complex_view_sql)
        self.assert_test(
            "创建复杂视图",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 删除视图
        result = self.execute_sql("DROP VIEW adult_users;")
        self.assert_test(
            "删除视图",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

    def test_privileges(self):
        """测试权限管理"""
        print("\n=== 测试权限管理 ===")

        # 授权
        result = self.execute_sql("GRANT SELECT ON users TO alice;")
        self.assert_test(
            "授予SELECT权限",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        result = self.execute_sql("GRANT INSERT ON users TO alice;")
        self.assert_test(
            "授予INSERT权限",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        result = self.execute_sql("GRANT ALL ON products TO alice;")
        self.assert_test(
            "授予ALL权限",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 撤权
        result = self.execute_sql("REVOKE INSERT ON users FROM alice;")
        self.assert_test(
            "撤销INSERT权限",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 给不存在用户授权（应该失败）
        result = self.execute_sql("GRANT SELECT ON users TO nonexistent;")
        self.assert_test("给不存在用户授权应该失败", not result.get("success"))

    def test_table_management(self):
        """测试表管理操作"""
        print("\n=== 测试表管理操作 ===")

        # 创建测试表
        result = self.execute_sql(
            "CREATE TABLE temp_table (id INTEGER, name VARCHAR(50));"
        )
        self.assert_test(
            "创建临时表",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 插入数据
        result = self.execute_sql("INSERT INTO temp_table VALUES (1, 'Test1');")
        self.assert_test(
            "插入测试数据1",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )
        result = self.execute_sql("INSERT INTO temp_table VALUES (2, 'Test2');")
        self.assert_test(
            "插入测试数据2",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # TRUNCATE测试
        result = self.execute_sql("TRUNCATE TABLE temp_table;")
        self.assert_test(
            "TRUNCATE表",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 验证TRUNCATE结果
        result = self.execute_sql("SELECT COUNT(*) FROM temp_table;")
        actual_count = self.safe_get_count(result)
        self.assert_test(
            "验证TRUNCATE结果",
            actual_count == 0,
            f"期望0条，实际{actual_count}条"
        )

        # DROP表测试
        result = self.execute_sql("DROP TABLE temp_table;")
        self.assert_test(
            "DROP表",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 查询已删除的表（应该失败）
        result = self.execute_sql("SELECT * FROM temp_table;")
        self.assert_test("查询已删除表应该失败", not result.get("success"))

    def test_constraint_validation(self):
        """测试约束验证"""
        print("\n=== 测试约束验证 ===")

        # 创建带约束的表
        constraint_table_sql = """
        CREATE TABLE test_constraints (
            id INTEGER PRIMARY KEY,
            age INTEGER CHECK (age > 0),
            score FLOAT DEFAULT 0.0,
            name VARCHAR(50) NOT NULL,
            email VARCHAR(100) UNIQUE
        );
        """
        result = self.execute_sql(constraint_table_sql)
        self.assert_test(
            "创建约束测试表",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 正常插入
        result = self.execute_sql(
            "INSERT INTO test_constraints (id, age, name, email) VALUES (1, 25, 'Test User', 'test@example.com');"
        )
        self.assert_test(
            "正常插入数据",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # DEFAULT值测试
        result = self.execute_sql(
            "INSERT INTO test_constraints (id, age, name, email) VALUES (2, 30, 'User2', 'user2@example.com');"
        )
        self.assert_test(
            "DEFAULT值测试",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # CHECK约束测试（应该失败）
        result = self.execute_sql(
            "INSERT INTO test_constraints VALUES (3, -5, 0.0, 'Invalid', 'invalid@example.com');"
        )
        self.assert_test("CHECK约束违反应该失败", not result.get("success"))

    def test_data_types(self):
        """测试数据类型"""
        print("\n=== 测试数据类型 ===")

        # 创建包含各种数据类型的表
        types_sql = """
        CREATE TABLE data_types_test (
            int_col INTEGER,
            varchar_col VARCHAR(100),
            float_col FLOAT,
            bool_col BOOLEAN,
            char_col CHAR(10),
            bigint_col BIGINT,
            text_col TEXT
        );
        """
        result = self.execute_sql(types_sql)
        self.assert_test(
            "创建数据类型测试表",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 插入各种类型的数据
        insert_types_sql = """
        INSERT INTO data_types_test VALUES (
            123, 'Hello World', 3.14, TRUE, 'ABCDE', 9999999999, 'This is a long text'
        );
        """
        result = self.execute_sql(insert_types_sql)
        self.assert_test(
            "插入各种类型数据",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 查询验证
        result = self.execute_sql("SELECT * FROM data_types_test")
        self.assert_test(
            "查询数据类型",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

    def test_edge_cases(self):
        """测试边界情况"""
        print("\n=== 测试边界情况 ===")

        # 空字符串
        result = self.execute_sql(
            "INSERT INTO users (id, name, age, email) VALUES (10, '', 25, 'empty@example.com');"
        )
        self.assert_test(
            "插入空字符串",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 特殊字符
        result = self.execute_sql(
            "INSERT INTO users (id, name, age, email) VALUES (11, 'User with spaces', 25, 'special@example.com');"
        )
        self.assert_test(
            "插入特殊字符",
            result.get("success"),
            result.get("message", result.get("error", "")),
        )

        # 查询不存在的表
        result = self.execute_sql("SELECT * FROM nonexistent_table;")
        self.assert_test("查询不存在表应该失败", not result.get("success"))

        # 查询不存在的列
        result = self.execute_sql("SELECT nonexistent_column FROM users;")
        self.assert_test("查询不存在列应该失败", not result.get("success"))

    def run_all_tests(self):
        """运行所有测试"""
        print("🚀 开始运行 MiniSQL 综合测试...")
        print("=" * 60)

        test_methods = [
            self.test_user_management,
            self.test_table_operations,
            self.test_data_insertion,
            self.test_query_operations,
            self.test_aggregate_functions,
            self.test_update_delete,
            self.test_index_operations,
            self.test_view_operations,
            self.test_privileges,
            self.test_table_management,
            self.test_constraint_validation,
            self.test_data_types,
            self.test_edge_cases
        ]

        for test_method in test_methods:
            try:
                test_method()
            except Exception as e:
                print(f"❌ 测试方法 {test_method.__name__} 发生异常: {str(e)}")
                traceback.print_exc()
                self.failed += 1

        # 输出测试结果
        self.print_test_summary()
        return self.failed == 0

    def print_test_summary(self):
        """打印测试总结"""
        print("\n" + "=" * 60)
        print("📊 测试结果统计:")
        print(f"✅ 通过: {self.passed}")
        print(f"❌ 失败: {self.failed}")

        total = self.passed + self.failed
        if total > 0:
            print(f"📈 通过率: {self.passed / total * 100:.1f}%")

        if self.failed == 0:
            print("🎉 所有测试通过！MiniSQL 功能正常！")
        else:
            print("⚠️  部分测试失败，请检查相关功能")
            print("\n失败的测试:")
            for test_name, passed, message in self.test_results:
                if not passed:
                    print(f"  - {test_name}: {message}")

    def cleanup(self):
        """清理测试环境"""
        try:
            # 清理测试数据
            cleanup_sqls = [
                "DROP VIEW adult_users",
                "DROP VIEW user_summary",
                "DROP TABLE test_constraints",
                "DROP TABLE data_types_test",
                "DROP TABLE temp_table",
                "DROP INDEX idx_user_email",
                "DROP INDEX idx_user_id",
                "DROP USER testuser",
                "DROP USER alice"
            ]

            for sql in cleanup_sqls:
                try:
                    self.execute_sql(sql)
                except:
                    pass  # 忽略清理错误

            self.db.close()

            # 删除测试数据库文件
            db_files = ["test_database.db", "test_database.db-journal", "test_database.db-wal"]
            for db_file in db_files:
                if os.path.exists(db_file):
                    os.remove(db_file)

        except Exception as e:
            print(f"清理测试文件时发生错误: {e}")


if __name__ == "__main__":
    tester = MiniSQLTester()
    try:
        success = tester.run_all_tests()
        sys.exit(0 if success else 1)
    finally:
        tester.cleanup()
