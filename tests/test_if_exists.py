import sys
from interface.database import SimpleDatabase
from interface.formatter import format_query_result

def run_if_exists_tests():
    print("=== IF EXISTS/IF NOT EXISTS 兼容性测试 ===")
    db = SimpleDatabase('test_if_exists.db')
    db.login('admin', 'admin123')

    # 1. CREATE TABLE IF NOT EXISTS
    print("\n-- CREATE TABLE IF NOT EXISTS --")
    res1 = db.execute_sql("CREATE TABLE t1 (id INTEGER);")
    format_query_result(res1)
    res2 = db.execute_sql("CREATE TABLE IF NOT EXISTS t1 (id INTEGER);")
    format_query_result(res2)

    # 2. DROP TABLE IF EXISTS
    print("\n-- DROP TABLE IF EXISTS --")
    res3 = db.execute_sql("DROP TABLE IF EXISTS t2;")
    format_query_result(res3)
    res4 = db.execute_sql("DROP TABLE IF EXISTS t1;")
    format_query_result(res4)
    res5 = db.execute_sql("DROP TABLE IF EXISTS t1;")  # 再次删除
    format_query_result(res5)

    # 3. CREATE INDEX IF NOT EXISTS
    print("\n-- CREATE INDEX IF NOT EXISTS --")
    db.execute_sql("CREATE TABLE t3 (id INTEGER);")
    res6 = db.execute_sql("CREATE INDEX idx_t3_id ON t3 (id);")
    format_query_result(res6)
    res7 = db.execute_sql("CREATE INDEX IF NOT EXISTS idx_t3_id ON t3 (id);")
    format_query_result(res7)

    # 4. DROP INDEX IF EXISTS
    print("\n-- DROP INDEX IF EXISTS --")
    res8 = db.execute_sql("DROP INDEX IF EXISTS idx_not_exist;")
    format_query_result(res8)
    res9 = db.execute_sql("DROP INDEX IF EXISTS idx_t3_id;")
    format_query_result(res9)
    res10 = db.execute_sql("DROP INDEX IF EXISTS idx_t3_id;")  # 再次删除
    format_query_result(res10)

    # 5. CREATE VIEW IF NOT EXISTS
    print("\n-- CREATE VIEW IF NOT EXISTS --")
    db.execute_sql("CREATE TABLE t4 (id INTEGER);")
    res11 = db.execute_sql("CREATE VIEW v1 AS SELECT * FROM t4;")
    format_query_result(res11)
    res12 = db.execute_sql("CREATE VIEW IF NOT EXISTS v1 AS SELECT * FROM t4;")
    format_query_result(res12)

    # 6. DROP VIEW IF EXISTS
    print("\n-- DROP VIEW IF EXISTS --")
    res13 = db.execute_sql("DROP VIEW IF EXISTS v_not_exist;")
    format_query_result(res13)
    res14 = db.execute_sql("DROP VIEW IF EXISTS v1;")
    format_query_result(res14)
    res15 = db.execute_sql("DROP VIEW IF EXISTS v1;")  # 再次删除
    format_query_result(res15)

    # 7. CREATE USER IF NOT EXISTS
    print("\n-- CREATE USER IF NOT EXISTS --")
    res16 = db.execute_sql("CREATE USER alice IDENTIFIED BY 'pwd';")
    format_query_result(res16)
    res17 = db.execute_sql("CREATE USER IF NOT EXISTS alice IDENTIFIED BY 'pwd';")
    format_query_result(res17)

    # 8. DROP USER IF EXISTS
    print("\n-- DROP USER IF EXISTS --")
    res18 = db.execute_sql("DROP USER IF EXISTS bob;")
    format_query_result(res18)
    res19 = db.execute_sql("DROP USER IF EXISTS alice;")
    format_query_result(res19)
    res20 = db.execute_sql("DROP USER IF EXISTS alice;")  # 再次删除
    format_query_result(res20)

    print("\n=== IF EXISTS/IF NOT EXISTS 测试完成 ===")

if __name__ == "__main__":
    run_if_exists_tests() 