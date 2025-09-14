import sys
from interface.database import SimpleDatabase
from interface.formatter import format_query_result

def run_trigger_tests():
    print("=== 触发器功能测试 ===")
    db = SimpleDatabase('test_trigger.db')
    db.login('admin', 'admin123')

    # 清理环境
    db.execute_sql("DROP TABLE IF EXISTS t1;")
    db.execute_sql("DROP TABLE IF EXISTS t2;")
    db.execute_sql("DROP TRIGGER IF EXISTS trg_bi;")
    db.execute_sql("DROP TRIGGER IF EXISTS trg_ai;")
    db.execute_sql("DROP TRIGGER IF EXISTS trg_bu;")
    db.execute_sql("DROP TRIGGER IF EXISTS trg_bd;")

    # 创建测试表
    db.execute_sql("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val VARCHAR(20));")
    db.execute_sql("CREATE TABLE t2 (log_id INTEGER, log_val VARCHAR(20));")

    # BEFORE INSERT 触发器
    print("\n-- 创建 BEFORE INSERT 触发器 --")
    res = db.execute_sql("""
        CREATE TRIGGER trg_bi BEFORE INSERT ON t1 FOR EACH ROW
        INSERT INTO t2 VALUES (NEW.id, 'before_insert');
    """)
    format_query_result(res)

    # AFTER INSERT 触发器
    print("\n-- 创建 AFTER INSERT 触发器 --")
    res = db.execute_sql("""
        CREATE TRIGGER trg_ai AFTER INSERT ON t1 FOR EACH ROW
        INSERT INTO t2 VALUES (NEW.id, 'after_insert');
    """)
    format_query_result(res)

    # BEFORE UPDATE 触发器
    print("\n-- 创建 BEFORE UPDATE 触发器 --")
    res = db.execute_sql("""
        CREATE TRIGGER trg_bu BEFORE UPDATE ON t1 FOR EACH ROW
        INSERT INTO t2 VALUES (NEW.id, 'before_update');
    """)
    format_query_result(res)

    # BEFORE DELETE 触发器
    print("\n-- 创建 BEFORE DELETE 触发器 --")
    res = db.execute_sql("""
        CREATE TRIGGER trg_bd BEFORE DELETE ON t1 FOR EACH ROW
        INSERT INTO t2 VALUES (OLD.id, 'before_delete');
    """)
    format_query_result(res)

    # 测试 INSERT
    print("\n-- 测试 INSERT 触发器 --")
    res = db.execute_sql("INSERT INTO t1 VALUES (1, 'A');")
    format_query_result(res)
    res = db.execute_sql("SELECT * FROM t2;")
    print("t2 日志:")
    format_query_result(res)

    # 测试 UPDATE
    print("\n-- 测试 UPDATE 触发器 --")
    res = db.execute_sql("UPDATE t1 SET val='B' WHERE id=1;")
    format_query_result(res)
    res = db.execute_sql("SELECT * FROM t2;")
    print("t2 日志:")
    format_query_result(res)

    # 测试 DELETE
    print("\n-- 测试 DELETE 触发器 --")
    res = db.execute_sql("DELETE FROM t1 WHERE id=1;")
    format_query_result(res)
    res = db.execute_sql("SELECT * FROM t2;")
    print("t2 日志:")
    format_query_result(res)

    # 测试 DROP TRIGGER
    print("\n-- 测试 DROP TRIGGER --")
    res = db.execute_sql("DROP TRIGGER trg_bi;")
    format_query_result(res)
    res = db.execute_sql("DROP TRIGGER trg_ai;")
    format_query_result(res)
    res = db.execute_sql("DROP TRIGGER trg_bu;")
    format_query_result(res)
    res = db.execute_sql("DROP TRIGGER trg_bd;")
    format_query_result(res)

    # 测试异常：重复创建
    print("\n-- 测试重复创建同名触发器 --")
    res1 = db.execute_sql("""
        CREATE TRIGGER trg_bi BEFORE INSERT ON t1 FOR EACH ROW
        INSERT INTO t2 VALUES (NEW.id, 'before_insert');
    """)
    format_query_result(res1)
    res2 = db.execute_sql("""
        CREATE TRIGGER trg_bi BEFORE INSERT ON t1 FOR EACH ROW
        INSERT INTO t2 VALUES (NEW.id, 'before_insert');
    """)
    format_query_result(res2)

    # 测试异常：删除不存在的触发器
    print("\n-- 测试删除不存在的触发器 --")
    res = db.execute_sql("DROP TRIGGER not_exist_trg;")
    format_query_result(res)

    print("\n=== 触发器功能测试结束 ===")

if __name__ == "__main__":
    run_trigger_tests()
