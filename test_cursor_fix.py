#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""测试游标Web API修复"""

import requests
import json
import time

def test_cursor_web_api():
    base_url = 'http://127.0.0.1:5000'
    
    # 启动会话
    session = requests.Session()
    
    print("1. 测试登录...")
    login_response = session.post(f'{base_url}/api/auth/login', json={
        'username': 'admin',
        'password': 'admin123'
    })
    
    if login_response.status_code != 200:
        print(f"登录失败: {login_response.text}")
        return False
    
    print("登录成功")
    
    # 创建测试表并插入数据
    print("\n2. 准备测试数据...")
    create_table_sql = """
    CREATE TABLE cursor_test (
        id INTEGER PRIMARY KEY,
        name VARCHAR(50),
        age INTEGER
    )
    """
    
    session.post(f'{base_url}/api/sql/execute', json={'sql': create_table_sql})
    
    # 插入测试数据
    for i in range(20):
        insert_sql = f"INSERT INTO cursor_test VALUES ({i}, 'User{i}', {20+i})"
        session.post(f'{base_url}/api/sql/execute', json={'sql': insert_sql})
    
    print("测试数据准备完成")
    
    # 测试游标操作
    print("\n3. 测试打开游标...")
    open_response = session.post(f'{base_url}/api/cursors/open', json={
        'sql': 'SELECT * FROM cursor_test ORDER BY id'
    })
    
    if open_response.status_code != 200:
        print(f"打开游标失败: {open_response.text}")
        return False
    
    cursor_data = open_response.json()
    if not cursor_data.get('success'):
        print(f"打开游标失败: {cursor_data}")
        return False
    
    cursor_id = cursor_data['cursor_id']
    print(f"游标打开成功，ID: {cursor_id}")
    
    # 分批获取数据
    print("\n4. 测试分批获取数据...")
    batch_count = 0
    while True:
        fetch_response = session.post(f'{base_url}/api/cursors/fetch', json={
            'cursor_id': cursor_id,
            'n': 5  # 每批5条记录
        })
        
        if fetch_response.status_code != 200:
            print(f"获取数据失败: {fetch_response.text}")
            break
        
        fetch_data = fetch_response.json()
        if not fetch_data.get('success'):
            print(f"获取数据失败: {fetch_data}")
            break
        
        rows = fetch_data.get('rows', [])
        is_done = fetch_data.get('done', True)
        
        batch_count += 1
        print(f"批次 {batch_count}: 获取到 {len(rows)} 条记录")
        
        if len(rows) > 0:
            print(f"  示例记录: {rows[0]}")
        
        if is_done or len(rows) == 0:
            print("数据获取完成")
            break
    
    # 关闭游标
    print("\n5. 测试关闭游标...")
    close_response = session.post(f'{base_url}/api/cursors/close', json={
        'cursor_id': cursor_id
    })
    
    if close_response.status_code == 200:
        close_data = close_response.json()
        if close_data.get('success'):
            print("游标关闭成功")
        else:
            print(f"游标关闭失败: {close_data}")
    else:
        print(f"游标关闭请求失败: {close_response.text}")
    
    # 清理测试数据
    print("\n6. 清理测试数据...")
    session.post(f'{base_url}/api/sql/execute', json={'sql': 'DROP TABLE cursor_test'})
    print("测试完成")
    
    return True

if __name__ == "__main__":
    print("游标Web API测试")
    print("请确保Web服务器正在运行 (python -m interface.web_api)")
    print("等待3秒后开始测试...")
    time.sleep(3)
    
    try:
        success = test_cursor_web_api()
        if success:
            print("\n✅ 游标功能测试通过!")
        else:
            print("\n❌ 游标功能测试失败!")
    except Exception as e:
        print(f"\n❌ 测试过程中出现异常: {e}")
        import traceback
        traceback.print_exc()