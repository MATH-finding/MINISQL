#!/usr/bin/env python3
"""
Diagnostics/Auto-correction 集成测试

用法:
  python test_diagnostics.py

说明:
  - 会临时创建一个数据库文件 diagnostics_test.db
  - 覆盖表名/列名大小写与拼写纠错、ORDER BY 列自动补投影、失败场景回退提示等
"""

import os
from interface.formatter import format_query_result
from interface.database import SimpleDatabase


def run():
    db_file = "diagnostics_test.db"
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
        except Exception:
            pass

    db = SimpleDatabase(db_file, cache_size=16)
    try:
        print("\n=== 准备数据 ===")
        format_query_result(db.execute_sql("CREATE TABLE Emp (Id INTEGER PRIMARY KEY, Name TEXT, Dept TEXT, Salary INTEGER)"))
        db.execute_sql("INSERT INTO Emp VALUES (1, 'Alice', 'HR', 100)")
        db.execute_sql("INSERT INTO Emp VALUES (2, 'Bob', 'ENG', 200)")
        db.execute_sql("INSERT INTO Emp VALUES (3, 'Carol', 'ENG', 300)")

        # 1) 表名大小写纠错 + 列名大小写纠错
        print("\n[CASE 1] 表名/列名大小写纠错")
        sql1 = "select Name from emp order by NAME desc"
        res1 = db.execute_sql(sql1)
        format_query_result(res1)
        if "hints" in res1:
            print("hints:", res1["hints"])

        # 2) 表名近似纠错（empp -> Emp）
        print("\n[CASE 2] 表名近似纠错")
        sql2 = "select name from empp"
        res2 = db.execute_sql(sql2)
        format_query_result(res2)
        if "hints" in res2:
            print("hints:", res2["hints"])

        # 3) 列名近似纠错（salery -> Salary），ORDER BY 列自动补投影
        print("\n[CASE 3] 列名近似纠错 + ORDER BY 列自动补投影")
        sql3 = "select dept from Emp order by salery desc"
        res3 = db.execute_sql(sql3)
        format_query_result(res3)
        if "hints" in res3:
            print("hints:", res3["hints"])

        # 4) GROUP BY 规则仍生效（纠错不放宽规则）
        print("\n[CASE 4] GROUP BY 规则校验 (应失败)")
        sql4 = "select dept, salary from Emp group by dept"
        res4 = db.execute_sql(sql4)
        format_query_result(res4)

        # 5) 多表歧义不做自动裸列纠错，仅提示错误（保守策略）
        print("\n[CASE 5] 多表歧义 (应提示歧义，不自动更改)")
        format_query_result(db.execute_sql("CREATE TABLE Dept (Id INTEGER PRIMARY KEY, Name TEXT)"))
        db.execute_sql("INSERT INTO Dept VALUES (10, 'HR')")
        db.execute_sql("INSERT INTO Dept VALUES (20, 'ENG')")
        sql5 = "select id from Emp join Dept on Emp.Dept = Dept.Name"
        res5 = db.execute_sql(sql5)
        format_query_result(res5)

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