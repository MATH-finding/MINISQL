#!/usr/bin/env python3
"""
CHECK 与 COUNT 功能专项测试

用法:
  python test_check_and_count.py

说明:
  - 会创建并清理临时数据库文件 check_count_test.db
  - 覆盖内容:
    1) 列级 CHECK 约束：成功与失败用例
    2) COUNT(*) 与 COUNT(col) 行为（COUNT(col) 不计 NULL）
    3) GROUP BY 下的 COUNT 聚合
"""

import os
from interface.database import SimpleDatabase
from interface.formatter import format_query_result


def run():
    db_file = "check_count_test.db"
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
        except Exception:
            pass

    db = SimpleDatabase(db_file, cache_size=16)
    try:
        print("\n=== 准备数据 (带 CHECK 约束) ===")
        # 列级 CHECK: price > 0, stock >= 0
        format_query_result(db.execute_sql(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER CHECK (price > 0), stock INTEGER CHECK (stock >= 0))"
        ))

        # 成功插入
        print("\n[CASE A1] CHECK 成功 - 插入合法数据")
        format_query_result(db.execute_sql("INSERT INTO products VALUES (1, 'Apple', 10, 100)"))
        format_query_result(db.execute_sql("INSERT INTO products VALUES (2, 'Banana', 5, 0)"))

        # 失败插入: 违反 price > 0
        print("\n[CASE A2] CHECK 失败 - price 不大于 0 (应失败)")
        res_fail1 = db.execute_sql("INSERT INTO products VALUES (3, 'BadPrice', 0, 10)")
        format_query_result(res_fail1)

        # 失败插入: 违反 stock >= 0
        print("\n[CASE A3] CHECK 失败 - stock 小于 0 (应失败)")
        res_fail2 = db.execute_sql("INSERT INTO products VALUES (4, 'BadStock', 8, -1)")
        format_query_result(res_fail2)

        print("\n=== COUNT 基本测试 ===")
        # 加入一行包含 NULL 列值，便于测试 COUNT(col) 忽略 NULL
        format_query_result(db.execute_sql("INSERT INTO products (id, name, price, stock) VALUES (5, 'NullStock', 12, NULL)"))

        print("\n[CASE B1] COUNT(*) 应统计所有行数")
        res_count_all = db.execute_sql("SELECT COUNT(*) FROM products")
        format_query_result(res_count_all)

        print("\n[CASE B2] COUNT(stock) 应忽略 NULL")
        res_count_col = db.execute_sql("SELECT COUNT(stock) FROM products")
        format_query_result(res_count_col)

        print("\n[CASE B3] COUNT(price) 列无 NULL，应等于总行数")
        res_count_price = db.execute_sql("SELECT COUNT(price) FROM products")
        format_query_result(res_count_price)

        print("\n=== GROUP BY + COUNT 测试 ===")
        # 构造简单的 dept/stock 情况（借用 name 列区分组）
        # 这里再插入一些不同 name 的数据，用于按 name 分组
        format_query_result(db.execute_sql("INSERT INTO products VALUES (6, 'Apple', 15, 10)"))
        format_query_result(db.execute_sql("INSERT INTO products VALUES (7, 'Banana', 7, 5)"))
        format_query_result(db.execute_sql("INSERT INTO products VALUES (8, 'Apple', 20, NULL)"))

        print("\n[CASE C1] GROUP BY name, COUNT(*) 统计每组行数")
        res_group_count_all = db.execute_sql("SELECT name, COUNT(*) FROM products GROUP BY name ORDER BY name ASC")
        format_query_result(res_group_count_all)

        print("\n[CASE C2] GROUP BY name, COUNT(stock) 忽略 NULL")
        res_group_count_col = db.execute_sql("SELECT name, COUNT(stock) FROM products GROUP BY name ORDER BY name ASC")
        format_query_result(res_group_count_col)

    finally:
        try:
            db.flush_all()
            db.close()
        except Exception:
            pass
        try:
            os.remove(db_file)
        except Exception:
            pass


if __name__ == "__main__":
    run() 