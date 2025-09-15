#!/usr/bin/env python3
"""
简单的脏读测试 - 验证READ UNCOMMITTED和READ COMMITTED的行为
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from interface.database import SimpleDatabase

def test_simple_dirty_read():
    """简单的脏读测试"""
    print("=== 简单的脏读测试 ===")
    
    # 创建临时数据库
    db = SimpleDatabase("test_simple_dirty_read.db")
    
    # 登录
    login_result = db.login("admin", "admin123")
    if not login_result.get("success"):
        print("❌ 登录失败")
        return
    
    print("✅ 登录成功")
    
    # 创建测试表
    print("\n1. 创建测试表:")
    result = db.execute_sql("CREATE TABLE test_simple (id INTEGER PRIMARY KEY, name VARCHAR(50), value INTEGER)")
    print(f"CREATE TABLE: {result}")
    
    # 插入初始数据
    print("\n2. 插入初始数据:")
    result = db.execute_sql("INSERT INTO test_simple VALUES (1, 'Alice', 100)")
    print(f"INSERT Alice: {result}")
    
    # 测试1: READ UNCOMMITTED 隔离级别
    print("\n" + "="*50)
    print("测试1: READ UNCOMMITTED 隔离级别（应该允许脏读）")
    print("="*50)
    
    # 会话1：READ UNCOMMITTED + 事务
    print("\n--- 会话1: READ UNCOMMITTED + 事务 ---")
    result = db.execute_sql("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    print(f"设置隔离级别: {result}")
    
    result = db.execute_sql("BEGIN")
    print(f"BEGIN: {result}")
    
    # 会话1：修改数据（未提交）
    print("\n--- 会话1: 修改数据（未提交）---")
    result = db.execute_sql("UPDATE test_simple SET value = 200 WHERE id = 1")
    print(f"更新Alice值: {result}")
    
    result = db.execute_sql("INSERT INTO test_simple VALUES (2, 'Bob', 300)")
    print(f"插入Bob: {result}")
    
    # 会话1：查看自己的修改
    result = db.execute_sql("SELECT * FROM test_simple ORDER BY id")
    print(f"会话1 查看数据: {result}")
    session1_data = result['data'] if result['data'] else []
    print(f"会话1 看到的数据行数: {len(session1_data)}")
    for row in session1_data:
        print(f"  ID: {row['id']}, Name: {row['name']}, Value: {row['value']}")
    
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
    result = db.execute_sql("SELECT * FROM test_simple ORDER BY id")
    print(f"会话2 查看数据: {result}")
    session2_data = result['data'] if result['data'] else []
    print(f"会话2 看到的数据行数: {len(session2_data)}")
    for row in session2_data:
        print(f"  ID: {row['id']}, Name: {row['name']}, Value: {row['value']}")
    
    # 检查脏读
    alice_value_changed = any(row['id'] == 1 and row['value'] == 200 for row in session2_data)
    bob_exists = any(row['id'] == 2 and row['name'] == 'Bob' for row in session2_data)
    
    if alice_value_changed and bob_exists:
        print("✅ READ UNCOMMITTED 正确允许了脏读！")
        print("   - 会话2看到了会话1未提交的Alice值更新")
        print("   - 会话2看到了会话1未提交的Bob插入")
    else:
        print("❌ READ UNCOMMITTED 没有正确允许脏读！")
        print(f"   - Alice值更新: {'✅' if alice_value_changed else '❌'}")
        print(f"   - Bob插入: {'✅' if bob_exists else '❌'}")
    
    # 会话1：提交事务
    print("\n--- 会话1: 提交事务 ---")
    db.use_session(0)
    result = db.execute_sql("COMMIT")
    print(f"COMMIT: {result}")
    
    # 测试2: READ COMMITTED 隔离级别
    print("\n" + "="*50)
    print("测试2: READ COMMITTED 隔离级别（应该防止脏读）")
    print("="*50)
    
    # 会话1：READ COMMITTED + 事务
    print("\n--- 会话1: READ COMMITTED + 事务 ---")
    result = db.execute_sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    print(f"设置隔离级别: {result}")
    
    result = db.execute_sql("BEGIN")
    print(f"BEGIN: {result}")
    
    # 会话1：修改数据（未提交）
    print("\n--- 会话1: 修改数据（未提交）---")
    result = db.execute_sql("UPDATE test_simple SET value = 400 WHERE id = 1")
    print(f"更新Alice值: {result}")
    
    result = db.execute_sql("INSERT INTO test_simple VALUES (3, 'Charlie', 500)")
    print(f"插入Charlie: {result}")
    
    # 会话1：查看自己的修改
    result = db.execute_sql("SELECT * FROM test_simple ORDER BY id")
    print(f"会话1 查看数据: {result}")
    session1_data = result['data'] if result['data'] else []
    print(f"会话1 看到的数据行数: {len(session1_data)}")
    for row in session1_data:
        print(f"  ID: {row['id']}, Name: {row['name']}, Value: {row['value']}")
    
    # 会话2：READ COMMITTED（非事务）
    print("\n--- 会话2: READ COMMITTED（非事务）---")
    db.use_session(session2_id)
    result = db.execute_sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    print(f"设置隔离级别: {result}")
    
    # 会话2：查看数据（应该看不到会话1未提交的修改）
    print("\n--- 会话2: 查看数据（应该看不到会话1未提交的修改）---")
    result = db.execute_sql("SELECT * FROM test_simple ORDER BY id")
    print(f"会话2 查看数据: {result}")
    session2_data = result['data'] if result['data'] else []
    print(f"会话2 看到的数据行数: {len(session2_data)}")
    for row in session2_data:
        print(f"  ID: {row['id']}, Name: {row['name']}, Value: {row['value']}")
    
    # 检查是否防止了脏读
    alice_value_unchanged = any(row['id'] == 1 and row['value'] == 200 for row in session2_data)
    charlie_not_exists = not any(row['id'] == 3 and row['name'] == 'Charlie' for row in session2_data)
    
    if alice_value_unchanged and charlie_not_exists:
        print("✅ READ COMMITTED 正确防止了脏读！")
        print("   - 会话2没有看到会话1未提交的Alice值更新")
        print("   - 会话2没有看到会话1未提交的Charlie插入")
    else:
        print("❌ READ COMMITTED 没有正确防止脏读！")
        print(f"   - Alice值未变: {'✅' if alice_value_unchanged else '❌'}")
        print(f"   - Charlie不存在: {'✅' if charlie_not_exists else '❌'}")
    
    # 会话1：提交事务
    print("\n--- 会话1: 提交事务 ---")
    db.use_session(0)
    result = db.execute_sql("COMMIT")
    print(f"COMMIT: {result}")
    
    # 会话2：再次查看数据（现在应该能看到）
    print("\n--- 会话2: 再次查看数据（提交后）---")
    db.use_session(session2_id)
    result = db.execute_sql("SELECT * FROM test_simple ORDER BY id")
    print(f"会话2 查看数据: {result}")
    session2_data_after = result['data'] if result['data'] else []
    print(f"会话2 看到的数据行数: {len(session2_data_after)}")
    for row in session2_data_after:
        print(f"  ID: {row['id']}, Name: {row['name']}, Value: {row['value']}")
    
    # 检查提交后的可见性
    alice_value_updated = any(row['id'] == 1 and row['value'] == 400 for row in session2_data_after)
    charlie_exists = any(row['id'] == 3 and row['name'] == 'Charlie' for row in session2_data_after)
    
    if alice_value_updated and charlie_exists:
        print("✅ 会话1提交后，会话2能看到数据了")
    else:
        print("❌ 会话1提交后，会话2仍然看不到数据")
    
    # 最终验证
    print("\n" + "="*50)
    print("最终验证：所有数据状态")
    print("="*50)
    
    result = db.execute_sql("SELECT * FROM test_simple ORDER BY id")
    print(f"最终数据: {result}")
    final_data = result['data'] if result['data'] else []
    print(f"最终数据行数: {len(final_data)}")
    for row in final_data:
        print(f"  ID: {row['id']}, Name: {row['name']}, Value: {row['value']}")
    
    # 清理
    print("\n3. 清理测试数据:")
    db.execute_sql("DROP TABLE test_simple")
    db.close()
    
    try:
        os.remove("test_simple_dirty_read.db")
    except:
        pass
    
    print("\n=== 简单脏读测试完成 ===")

if __name__ == "__main__":
    test_simple_dirty_read()