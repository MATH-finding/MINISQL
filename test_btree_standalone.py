"""
debug_index_test.py - 索引功能深度调试测试
"""

import os
import sys

sys.path.append(".")

from interface.database import SimpleDatabase
from catalog.index_manager import IndexManager
from storage.btree import BPlusTree


def detailed_index_debug():
    """详细的索引调试测试"""
    print("🔍 开始深度索引调试测试")
    print("=" * 60)

    # 清理测试数据库
    db_file = "debug_index.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    db = SimpleDatabase(db_file)

    try:
        # 步骤 1: 创建表
        print("\n步骤 1: 创建测试表")
        result = db.execute_sql(
            "CREATE TABLE test_users (id INTEGER PRIMARY KEY, name VARCHAR(50))"
        )
        print(f"创建表结果: {result}")

        # 步骤 2: 检查索引管理器状态
        print("\n步骤 2: 检查索引管理器")
        print(f"索引管理器存在: {db.index_manager is not None}")
        if db.index_manager:
            print(f"当前索引数量: {len(db.index_manager.indexes)}")
            print(f"索引列表: {list(db.index_manager.indexes.keys())}")

        # 步骤 3: 创建索引
        print("\n步骤 3: 创建索引")
        result = db.execute_sql("CREATE INDEX idx_user_id ON test_users (id)")
        print(f"创建索引结果: {result}")

        # 步骤 4: 再次检查索引管理器
        print("\n步骤 4: 索引创建后的状态")
        if db.index_manager:
            print(f"索引数量: {len(db.index_manager.indexes)}")
            print(f"索引列表: {list(db.index_manager.indexes.keys())}")

            # 检查特定索引
            if "idx_user_id" in db.index_manager.indexes:
                index_info = db.index_manager.indexes["idx_user_id"]
                print(f"索引 idx_user_id 详情: {index_info}")

                # 获取B+树实例
                btree = db.index_manager.get_index("idx_user_id")
                print(f"B+树实例: {btree}")
                if btree:
                    print(f"B+树根节点: {btree.root}")

        # 步骤 5: 插入测试数据
        print("\n步骤 5: 插入测试数据")
        result1 = db.execute_sql("INSERT INTO test_users VALUES (1, 'Alice')")
        print(f"插入记录1: {result1}")

        result2 = db.execute_sql("INSERT INTO test_users VALUES (2, 'Bob')")
        print(f"插入记录2: {result2}")

        # 步骤 6: 检查B+树内容
        print("\n步骤 6: 检查B+树内容")
        if db.index_manager:
            btree = db.index_manager.get_index("idx_user_id")
            if btree:
                print(f"B+树根节点: {btree.root}")
                if hasattr(btree.root, "keys"):
                    print(f"根节点键: {btree.root.keys}")
                    print(f"根节点值: {btree.root.values}")

                # 手动测试搜索
                print(f"搜索键1: {btree.search(1)}")
                print(f"搜索键2: {btree.search(2)}")
                print(f"搜索键3: {btree.search(3)}")

        # 步骤 7: 全表扫描验证
        print("\n步骤 7: 全表扫描验证")
        result = db.execute_sql("SELECT * FROM test_users")
        print(f"全表扫描结果: {result}")

        # 步骤 8: 带WHERE条件的查询（问题所在）
        print("\n步骤 8: WHERE条件查询测试")

        # 首先测试执行器内部的分析
        from sql.lexer import SQLLexer
        from sql.parser import SQLParser

        sql = "SELECT * FROM test_users WHERE id = 1"
        lexer = SQLLexer(sql)
        tokens = lexer.tokenize()
        parser = SQLParser(tokens)
        ast = parser.parse()

        print(f"SQL AST: {ast}")
        print(f"WHERE子句: {ast.where_clause}")
        print(f"WHERE子句类型: {type(ast.where_clause)}")

        if hasattr(ast.where_clause, "left"):
            print(
                f"左操作数: {ast.where_clause.left} (类型: {type(ast.where_clause.left)})"
            )
        if hasattr(ast.where_clause, "right"):
            print(
                f"右操作数: {ast.where_clause.right} (类型: {type(ast.where_clause.right)})"
            )
        if hasattr(ast.where_clause, "operator"):
            print(f"操作符: {ast.where_clause.operator}")

        # 手动测试执行器的索引分析
        executor = db.executor
        if executor.index_manager:
            print("\n手动测试索引分析:")
            index_info = executor._analyze_where_for_index(
                "test_users", ast.where_clause
            )
            print(f"索引分析结果: {index_info}")

            if index_info:
                index_name, column_name, operator, value = index_info
                print(
                    f"找到可用索引: {index_name}, 列: {column_name}, 操作符: {operator}, 值: {value}"
                )

                # 手动测试索引扫描
                optimized_records = executor._try_index_scan(
                    "test_users", ast.where_clause
                )
                print(f"索引扫描结果: {optimized_records}")

        # 步骤 9: 实际查询测试
        print("\n步骤 9: 实际查询测试")
        result = db.execute_sql("SELECT * FROM test_users WHERE id = 1")
        print(f"WHERE查询结果: {result}")

        # 步骤 10: 索引管理器状态检查
        print("\n步骤 10: 最终状态检查")
        indexes_result = db.list_indexes("test_users")
        print(f"表索引列表: {indexes_result}")

    except Exception as e:
        print(f"测试过程中出错: {e}")
        import traceback

        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    detailed_index_debug()
