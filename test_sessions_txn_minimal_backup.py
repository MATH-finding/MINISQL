import os
from interface.database import SimpleDatabase


def run(db, sess, sql):
    db.use_session(sess)
    res = db.execute_sql(sql)
    print(f"[S{sess}] SQL> {sql}\n      -> {res}")
    return res


def reset_and_seed(db, table_name):
    # 使用全新表名，避免历史版本干扰
    run(db, 0, f"CREATE TABLE {table_name} (id INTEGER, v INTEGER)")
    run(db, 0, f"INSERT INTO {table_name} VALUES (1, 10)")


def test_insert_commit_and_rollback(db):
    print("\n=== 不设隔离级别: INSERT 提交/回滚 可见性 ===")
    tbl = "t2_ins"
    reset_and_seed(db, tbl)
    s0 = 0
    s1 = db.new_session()

    # 未提交对他会话不可见
    run(db, s0, "BEGIN")
    run(db, s0, f"INSERT INTO {tbl} VALUES (2, 20)")
    r = run(db, s1, f"SELECT * FROM {tbl}")
    assert not any(row.get('id') == 2 for row in r.get('data', []))
    run(db, s0, "COMMIT")
    r2 = run(db, s1, f"SELECT * FROM {tbl}")
    assert any(row.get('id') == 2 for row in r2.get('data', []))

    # 回滚不生效
    run(db, s0, "BEGIN")
    run(db, s0, f"INSERT INTO {tbl} VALUES (3, 30)")
    run(db, s0, "ROLLBACK")
    r3 = run(db, s1, f"SELECT * FROM {tbl}")
    assert not any(row.get('id') == 3 for row in r3.get('data', []))


def test_update_commit_and_rollback(db):
    print("\n=== 不设隔离级别: UPDATE 提交/回滚 可见性 ===")
    tbl = "t2_upd"
    reset_and_seed(db, tbl)
    s0 = 0
    s1 = db.new_session()

    run(db, s0, "BEGIN")
    run(db, s0, f"UPDATE {tbl} SET v=99 WHERE id=1")
    # 他会话仍见旧值
    r0 = run(db, s1, f"SELECT * FROM {tbl} WHERE id=1")
    assert any(row.get('v') == 10 for row in r0.get('data', []))
    # 回滚 -> 仍旧值
    run(db, s0, "ROLLBACK")
    r1 = run(db, s1, f"SELECT * FROM {tbl} WHERE id=1")
    assert any(row.get('v') == 10 for row in r1.get('data', []))

    # 再次更新并提交 -> 新值可见
    run(db, s0, "BEGIN")
    run(db, s0, f"UPDATE {tbl} SET v=50 WHERE id=1")
    run(db, s0, "COMMIT")
    r2 = run(db, s1, f"SELECT * FROM {tbl} WHERE id=1")
    assert any(row.get('v') == 50 for row in r2.get('data', []))


def test_delete_commit_and_rollback(db):
    print("\n=== 不设隔离级别: DELETE 提交/回滚 可见性 ===")
    tbl = "t2_del"
    reset_and_seed(db, tbl)
    s0 = 0
    s1 = db.new_session()

    run(db, s0, "BEGIN")
    run(db, s0, f"DELETE FROM {tbl} WHERE id=1")
    # 他会话仍能看到该行
    r0 = run(db, s1, f"SELECT * FROM {tbl} WHERE id=1")
    assert any(row.get('id') == 1 for row in r0.get('data', []))
    # 回滚 -> 仍可见
    run(db, s0, "ROLLBACK")
    r1 = run(db, s1, f"SELECT * FROM {tbl} WHERE id=1")
    assert any(row.get('id') == 1 for row in r1.get('data', []))

    # 提交后 -> 不可见
    run(db, s0, "BEGIN")
    run(db, s0, f"DELETE FROM {tbl} WHERE id=1")
    run(db, s0, "COMMIT")
    r2 = run(db, s1, f"SELECT * FROM {tbl} WHERE id=1")
    assert not any(row.get('id') == 1 for row in r2.get('data', []))


if __name__ == "__main__":
    db_path = os.path.join(os.path.dirname(__file__), "database.db")
    db = SimpleDatabase(db_path)
    try:
        test_insert_commit_and_rollback(db)
        test_update_commit_and_rollback(db)
        test_delete_commit_and_rollback(db)
        print("\n基本事务/会话测试通过（未显式设置隔离级别）！")
    finally:
        db.close()


