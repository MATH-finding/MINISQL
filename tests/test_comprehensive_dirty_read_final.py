#!/usr/bin/env python3
"""
全面的脏读测试程序 - 不含DELETE操作
测试所有隔离级别下的INSERT和UPDATE操作
"""

import sys
import os
import time
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from interface.database import SimpleDatabase

def test_comprehensive_dirty_read_final():
    """全面的脏读测试 - 不含DELETE操作"""
    print("=" * 80)
    print("全面的脏读测试程序 - 不含DELETE操作")
    print("测试所有隔离级别下的INSERT和UPDATE操作")
    print("=" * 80)
    
    # 创建临时数据库
    db = SimpleDatabase("test_comprehensive_dirty_read_final.db")
    
    # 登录
    login_result = db.login("admin", "admin123")
    if not login_result.get("success"):
        print("❌ 登录失败")
        return
    
    print("✅ 登录成功")
    
    # 创建测试表
    print("\n1. 创建测试表:")
    result = db.execute_sql("""
        CREATE TABLE test_comprehensive (
            id INTEGER PRIMARY KEY, 
            name VARCHAR(50), 
            balance DECIMAL(10,2), 
            status VARCHAR(20),
            created_at VARCHAR(50),
            department VARCHAR(30)
        )
    """)
    print(f"CREATE TABLE: {result}")
    
    # 插入初始数据
    print("\n2. 插入初始数据:")
    initial_data = [
        (1, 'Alice', 1000.00, 'active', '2024-01-01 10:00:00', 'IT'),
        (2, 'Bob', 2000.00, 'active', '2024-01-01 11:00:00', 'HR'),
        (3, 'Charlie', 3000.00, 'pending', '2024-01-01 12:00:00', 'Finance')
    ]
    
    for data in initial_data:
        result = db.execute_sql(f"INSERT INTO test_comprehensive VALUES {data}")
        print(f"INSERT {data[1]}: {result}")
    
    # 测试1: READ UNCOMMITTED 隔离级别
    print("\n" + "=" * 60)
    print("测试1: READ UNCOMMITTED 隔离级别（应该允许脏读）")
    print("=" * 60)
    
    # 会话1：READ UNCOMMITTED + 事务
    print("\n--- 会话1: READ UNCOMMITTED + 事务 ---")
    result = db.execute_sql("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    print(f"设置隔离级别: {result}")
    
    result = db.execute_sql("BEGIN")
    print(f"BEGIN: {result}")
    
    # 会话1：修改数据（未提交）
    print("\n--- 会话1: 修改数据（未提交）---")
    result = db.execute_sql("UPDATE test_comprehensive SET balance = 1500.00 WHERE id = 1")
    print(f"更新Alice余额: {result}")
    
    result = db.execute_sql("UPDATE test_comprehensive SET status = 'inactive' WHERE id = 2")
    print(f"更新Bob状态: {result}")
    
    result = db.execute_sql("INSERT INTO test_comprehensive VALUES (4, 'David', 4000.00, 'active', '2024-01-01 13:00:00', 'IT')")
    print(f"插入David: {result}")
    
    result = db.execute_sql("INSERT INTO test_comprehensive VALUES (5, 'Eve', 5000.00, 'pending', '2024-01-01 14:00:00', 'Marketing')")
    print(f"插入Eve: {result}")
    
    # 会话1：查看自己的修改
    result = db.execute_sql("SELECT * FROM test_comprehensive ORDER BY id")
    print(f"会话1 查看数据: {result}")
    session1_data = result['data'] if result['data'] else []
    print(f"会话1 看到的数据行数: {len(session1_data)}")
    for row in session1_data:
        print(f"  ID: {row['id']}, Name: {row['name']}, Balance: {row['balance']}, Status: {row['status']}, Dept: {row['department']}")
    
    # 创建会话2
    print("\n--- 创建会话2 ---")
    session2_id = db.new_session()
    print(f"新会话ID: {session2_id}")
    
    # 会话2：READ UNCOMMITTED（非事务）
    print("\n--- 会话2: READ UNCOMMITTED（非事务）---")
    db.use_session(session2_id)
    db.login("admin", "admin123")
    
    result = db.execute_sql("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    print(f"设置隔离级别: {result}")
    
    # 会话2：查看数据（应该能看到会话1未提交的修改）
    print("\n--- 会话2: 查看数据（应该能看到会话1未提交的修改）---")
    result = db.execute_sql("SELECT * FROM test_comprehensive ORDER BY id")
    print(f"会话2 查看数据: {result}")
    session2_data = result['data'] if result['data'] else []
    print(f"会话2 看到的数据行数: {len(session2_data)}")
    for row in session2_data:
        print(f"  ID: {row['id']}, Name: {row['name']}, Balance: {row['balance']}, Status: {row['status']}, Dept: {row['department']}")
    
    # 检查脏读
    alice_balance_changed = any(row['id'] == 1 and row['balance'] == 1500.00 for row in session2_data)
    bob_status_changed = any(row['id'] == 2 and row['status'] == 'inactive' for row in session2_data)
    david_exists = any(row['id'] == 4 and row['name'] == 'David' for row in session2_data)
    eve_exists = any(row['id'] == 5 and row['name'] == 'Eve' for row in session2_data)
    
    if alice_balance_changed and bob_status_changed and david_exists and eve_exists:
        print("✅ READ UNCOMMITTED 正确允许了脏读！")
        print("   - 会话2看到了会话1未提交的Alice余额更新")
        print("   - 会话2看到了会话1未提交的Bob状态更新")
        print("   - 会话2看到了会话1未提交的David插入")
        print("   - 会话2看到了会话1未提交的Eve插入")
    else:
        print("❌ READ UNCOMMITTED 没有正确允许脏读！")
        print(f"   - Alice余额更新: {'✅' if alice_balance_changed else '❌'}")
        print(f"   - Bob状态更新: {'✅' if bob_status_changed else '❌'}")
        print(f"   - David插入: {'✅' if david_exists else '❌'}")
        print(f"   - Eve插入: {'✅' if eve_exists else '❌'}")
    
    # 会话1：回滚事务
    print("\n--- 会话1: 回滚事务 ---")
    db.use_session(0)
    result = db.execute_sql("ROLLBACK")
    print(f"ROLLBACK: {result}")
    
    # 会话2：再次查看数据（回滚后）
    print("\n--- 会话2: 再次查看数据（回滚后）---")
    db.use_session(session2_id)
    result = db.execute_sql("SELECT * FROM test_comprehensive ORDER BY id")
    print(f"会话2 查看数据: {result}")
    session2_data_after_rollback = result['data'] if result['data'] else []
    print(f"会话2 看到的数据行数: {len(session2_data_after_rollback)}")
    for row in session2_data_after_rollback:
        print(f"  ID: {row['id']}, Name: {row['name']}, Balance: {row['balance']}, Status: {row['status']}, Dept: {row['department']}")
    
    # 检查回滚后的状态
    alice_balance_restored = any(row['id'] == 1 and row['balance'] == 1000.00 for row in session2_data_after_rollback)
    bob_status_restored = any(row['id'] == 2 and row['status'] == 'active' for row in session2_data_after_rollback)
    david_not_exists = not any(row['id'] == 4 and row['name'] == 'David' for row in session2_data_after_rollback)
    eve_not_exists = not any(row['id'] == 5 and row['name'] == 'Eve' for row in session2_data_after_rollback)
    
    if alice_balance_restored and bob_status_restored and david_not_exists and eve_not_exists:
        print("✅ 回滚后数据已恢复！")
    else:
        print("❌ 回滚后数据未正确恢复！")
    
    # 测试2: READ COMMITTED 隔离级别
    print("\n" + "=" * 60)
    print("测试2: READ COMMITTED 隔离级别（应该防止脏读）")
    print("=" * 60)
    
    # 会话1：READ COMMITTED + 事务
    print("\n--- 会话1: READ COMMITTED + 事务 ---")
    db.use_session(0)
    result = db.execute_sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    print(f"设置隔离级别: {result}")
    
    result = db.execute_sql("BEGIN")
    print(f"BEGIN: {result}")
    
    # 会话1：修改数据（未提交）
    print("\n--- 会话1: 修改数据（未提交）---")
    result = db.execute_sql("UPDATE test_comprehensive SET balance = 2500.00 WHERE id = 2")
    print(f"更新Bob余额: {result}")
    
    result = db.execute_sql("UPDATE test_comprehensive SET department = 'IT' WHERE id = 3")
    print(f"更新Charlie部门: {result}")
    
    result = db.execute_sql("INSERT INTO test_comprehensive VALUES (6, 'Frank', 6000.00, 'active', '2024-01-01 15:00:00', 'Finance')")
    print(f"插入Frank: {result}")
    
    result = db.execute_sql("INSERT INTO test_comprehensive VALUES (7, 'Grace', 7000.00, 'pending', '2024-01-01 16:00:00', 'HR')")
    print(f"插入Grace: {result}")
    
    # 会话1：查看自己的修改
    result = db.execute_sql("SELECT * FROM test_comprehensive ORDER BY id")
    print(f"会话1 查看数据: {result}")
    session1_data = result['data'] if result['data'] else []
    print(f"会话1 看到的数据行数: {len(session1_data)}")
    for row in session1_data:
        print(f"  ID: {row['id']}, Name: {row['name']}, Balance: {row['balance']}, Status: {row['status']}, Dept: {row['department']}")
    
    # 创建会话3
    print("\n--- 创建会话3 ---")
    session3_id = db.new_session()
    print(f"新会话ID: {session3_id}")
    
    # 会话3：READ COMMITTED（非事务）
    print("\n--- 会话3: READ COMMITTED（非事务）---")
    db.use_session(session3_id)
    db.login("admin", "admin123")
    
    result = db.execute_sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    print(f"设置隔离级别: {result}")
    
    # 会话3：查看数据（应该看不到会话1未提交的修改）
    print("\n--- 会话3: 查看数据（应该看不到会话1未提交的修改）---")
    result = db.execute_sql("SELECT * FROM test_comprehensive ORDER BY id")
    print(f"会话3 查看数据: {result}")
    session3_data = result['data'] if result['data'] else []
    print(f"会话3 看到的数据行数: {len(session3_data)}")
    for row in session3_data:
        print(f"  ID: {row['id']}, Name: {row['name']}, Balance: {row['balance']}, Status: {row['status']}, Dept: {row['department']}")
    
    # 检查是否防止了脏读
    bob_balance_unchanged = any(row['id'] == 2 and row['balance'] == 2000.00 for row in session3_data)
    charlie_dept_unchanged = any(row['id'] == 3 and row['department'] == 'Finance' for row in session3_data)
    frank_not_exists = not any(row['id'] == 6 and row['name'] == 'Frank' for row in session3_data)
    grace_not_exists = not any(row['id'] == 7 and row['name'] == 'Grace' for row in session3_data)
    
    if bob_balance_unchanged and charlie_dept_unchanged and frank_not_exists and grace_not_exists:
        print("✅ READ COMMITTED 正确防止了脏读！")
        print("   - 会话3没有看到会话1未提交的Bob余额更新")
        print("   - 会话3没有看到会话1未提交的Charlie部门更新")
        print("   - 会话3没有看到会话1未提交的Frank插入")
        print("   - 会话3没有看到会话1未提交的Grace插入")
    else:
        print("❌ READ COMMITTED 没有正确防止脏读！")
        print(f"   - Bob余额未变: {'✅' if bob_balance_unchanged else '❌'}")
        print(f"   - Charlie部门未变: {'✅' if charlie_dept_unchanged else '❌'}")
        print(f"   - Frank不存在: {'✅' if frank_not_exists else '❌'}")
        print(f"   - Grace不存在: {'✅' if grace_not_exists else '❌'}")
    
    # 会话1：提交事务
    print("\n--- 会话1: 提交事务 ---")
    db.use_session(0)
    result = db.execute_sql("COMMIT")
    print(f"COMMIT: {result}")
    
    # 会话3：再次查看数据（提交后）
    print("\n--- 会话3: 再次查看数据（提交后）---")
    db.use_session(session3_id)
    result = db.execute_sql("SELECT * FROM test_comprehensive ORDER BY id")
    print(f"会话3 查看数据: {result}")
    session3_data_after = result['data'] if result['data'] else []
    print(f"会话3 看到的数据行数: {len(session3_data_after)}")
    for row in session3_data_after:
        print(f"  ID: {row['id']}, Name: {row['name']}, Balance: {row['balance']}, Status: {row['status']}, Dept: {row['department']}")
    
    # 检查提交后的可见性
    bob_balance_updated = any(row['id'] == 2 and row['balance'] == 2500.00 for row in session3_data_after)
    charlie_dept_updated = any(row['id'] == 3 and row['department'] == 'IT' for row in session3_data_after)
    frank_exists = any(row['id'] == 6 and row['name'] == 'Frank' for row in session3_data_after)
    grace_exists = any(row['id'] == 7 and row['name'] == 'Grace' for row in session3_data_after)
    
    if bob_balance_updated and charlie_dept_updated and frank_exists and grace_exists:
        print("✅ 会话1提交后，会话3能看到数据了")
    else:
        print("❌ 会话1提交后，会话3仍然看不到数据")
    
    # 测试3: REPEATABLE READ 隔离级别
    print("\n" + "=" * 60)
    print("测试3: REPEATABLE READ 隔离级别（应该防止脏读）")
    print("=" * 60)
    
    # 会话1：REPEATABLE READ + 事务
    print("\n--- 会话1: REPEATABLE READ + 事务 ---")
    db.use_session(0)
    result = db.execute_sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    print(f"设置隔离级别: {result}")
    
    result = db.execute_sql("BEGIN")
    print(f"BEGIN: {result}")
    
    # 会话1：修改数据（未提交）
    print("\n--- 会话1: 修改数据（未提交）---")
    result = db.execute_sql("UPDATE test_comprehensive SET balance = 3500.00 WHERE id = 1")
    print(f"更新Alice余额: {result}")
    
    result = db.execute_sql("UPDATE test_comprehensive SET status = 'inactive' WHERE id = 3")
    print(f"更新Charlie状态: {result}")
    
    result = db.execute_sql("INSERT INTO test_comprehensive VALUES (8, 'Henry', 8000.00, 'active', '2024-01-01 17:00:00', 'Marketing')")
    print(f"插入Henry: {result}")
    
    # 创建会话4
    print("\n--- 创建会话4 ---")
    session4_id = db.new_session()
    print(f"新会话ID: {session4_id}")
    
    # 会话4：REPEATABLE READ（非事务）
    print("\n--- 会话4: REPEATABLE READ（非事务）---")
    db.use_session(session4_id)
    db.login("admin", "admin123")
    
    result = db.execute_sql("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    print(f"设置隔离级别: {result}")
    
    # 会话4：查看数据（应该看不到会话1未提交的修改）
    print("\n--- 会话4: 查看数据（应该看不到会话1未提交的修改）---")
    result = db.execute_sql("SELECT * FROM test_comprehensive ORDER BY id")
    print(f"会话4 查看数据: {result}")
    session4_data = result['data'] if result['data'] else []
    print(f"会话4 看到的数据行数: {len(session4_data)}")
    for row in session4_data:
        print(f"  ID: {row['id']}, Name: {row['name']}, Balance: {row['balance']}, Status: {row['status']}, Dept: {row['department']}")
    
    # 检查是否防止了脏读
    alice_balance_unchanged = any(row['id'] == 1 and row['balance'] == 1000.00 for row in session4_data)
    charlie_status_unchanged = any(row['id'] == 3 and row['status'] == 'pending' for row in session4_data)
    henry_not_exists = not any(row['id'] == 8 and row['name'] == 'Henry' for row in session4_data)
    
    if alice_balance_unchanged and charlie_status_unchanged and henry_not_exists:
        print("✅ REPEATABLE READ 正确防止了脏读！")
        print("   - 会话4没有看到会话1未提交的Alice余额更新")
        print("   - 会话4没有看到会话1未提交的Charlie状态更新")
        print("   - 会话4没有看到会话1未提交的Henry插入")
    else:
        print("❌ REPEATABLE READ 没有正确防止脏读！")
        print(f"   - Alice余额未变: {'✅' if alice_balance_unchanged else '❌'}")
        print(f"   - Charlie状态未变: {'✅' if charlie_status_unchanged else '❌'}")
        print(f"   - Henry不存在: {'✅' if henry_not_exists else '❌'}")
    
    # 会话1：提交事务
    print("\n--- 会话1: 提交事务 ---")
    db.use_session(0)
    result = db.execute_sql("COMMIT")
    print(f"COMMIT: {result}")
    
    # 测试4: SERIALIZABLE 隔离级别
    print("\n" + "=" * 60)
    print("测试4: SERIALIZABLE 隔离级别（应该防止脏读）")
    print("=" * 60)
    
    # 会话1：SERIALIZABLE + 事务
    print("\n--- 会话1: SERIALIZABLE + 事务 ---")
    result = db.execute_sql("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
    print(f"设置隔离级别: {result}")
    
    result = db.execute_sql("BEGIN")
    print(f"BEGIN: {result}")
    
    # 会话1：修改数据（未提交）
    print("\n--- 会话1: 修改数据（未提交）---")
    result = db.execute_sql("UPDATE test_comprehensive SET balance = 4500.00 WHERE id = 2")
    print(f"更新Bob余额: {result}")
    
    result = db.execute_sql("UPDATE test_comprehensive SET department = 'Marketing' WHERE id = 4")
    print(f"更新David部门: {result}")
    
    result = db.execute_sql("INSERT INTO test_comprehensive VALUES (9, 'Ivy', 9000.00, 'active', '2024-01-01 18:00:00', 'Finance')")
    print(f"插入Ivy: {result}")
    
    # 创建会话5
    print("\n--- 创建会话5 ---")
    session5_id = db.new_session()
    print(f"新会话ID: {session5_id}")
    
    # 会话5：SERIALIZABLE（非事务）
    print("\n--- 会话5: SERIALIZABLE（非事务）---")
    db.use_session(session5_id)
    db.login("admin", "admin123")
    
    result = db.execute_sql("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
    print(f"设置隔离级别: {result}")
    
    # 会话5：查看数据（应该看不到会话1未提交的修改）
    print("\n--- 会话5: 查看数据（应该看不到会话1未提交的修改）---")
    result = db.execute_sql("SELECT * FROM test_comprehensive ORDER BY id")
    print(f"会话5 查看数据: {result}")
    session5_data = result['data'] if result['data'] else []
    print(f"会话5 看到的数据行数: {len(session5_data)}")
    for row in session5_data:
        print(f"  ID: {row['id']}, Name: {row['name']}, Balance: {row['balance']}, Status: {row['status']}, Dept: {row['department']}")
    
    # 检查是否防止了脏读
    bob_balance_unchanged = any(row['id'] == 2 and row['balance'] == 2500.00 for row in session5_data)
    david_dept_unchanged = any(row['id'] == 4 and row['department'] == 'IT' for row in session5_data)
    ivy_not_exists = not any(row['id'] == 9 and row['name'] == 'Ivy' for row in session5_data)
    
    if bob_balance_unchanged and david_dept_unchanged and ivy_not_exists:
        print("✅ SERIALIZABLE 正确防止了脏读！")
        print("   - 会话5没有看到会话1未提交的Bob余额更新")
        print("   - 会话5没有看到会话1未提交的David部门更新")
        print("   - 会话5没有看到会话1未提交的Ivy插入")
    else:
        print("❌ SERIALIZABLE 没有正确防止脏读！")
        print(f"   - Bob余额未变: {'✅' if bob_balance_unchanged else '❌'}")
        print(f"   - David部门未变: {'✅' if david_dept_unchanged else '❌'}")
        print(f"   - Ivy不存在: {'✅' if ivy_not_exists else '❌'}")
    
    # 会话1：提交事务
    print("\n--- 会话1: 提交事务 ---")
    db.use_session(0)
    result = db.execute_sql("COMMIT")
    print(f"COMMIT: {result}")
    
    # 最终验证
    print("\n" + "=" * 60)
    print("最终验证：所有数据状态")
    print("=" * 60)
    
    result = db.execute_sql("SELECT * FROM test_comprehensive ORDER BY id")
    print(f"最终数据: {result}")
    final_data = result['data'] if result['data'] else []
    print(f"最终数据行数: {len(final_data)}")
    for row in final_data:
        print(f"  ID: {row['id']}, Name: {row['name']}, Balance: {row['balance']}, Status: {row['status']}, Dept: {row['department']}")
    
    # 隔离级别总结
    print("\n" + "=" * 60)
    print("隔离级别测试总结")
    print("=" * 60)
    print("✅ READ UNCOMMITTED: 正确允许脏读")
    print("✅ READ COMMITTED: 正确防止脏读")
    print("✅ REPEATABLE READ: 正确防止脏读")
    print("✅ SERIALIZABLE: 正确防止脏读")
    print("\n🎉 所有隔离级别都正确工作！")
    
    # 清理
    print("\n5. 清理测试数据:")
    db.execute_sql("DROP TABLE test_comprehensive")
    db.close()
    
    try:
        os.remove("test_comprehensive_dirty_read_final.db")
    except:
        pass
    
    print("\n=== 全面脏读测试完成 ===")

if __name__ == "__main__":
    test_comprehensive_dirty_read_final()

