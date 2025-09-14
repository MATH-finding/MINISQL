"""
触发器Web API测试脚本
测试MiniDBMS触发器的Web API功能
"""

import requests
import json
import time

class TriggerWebAPITest:
    def __init__(self, base_url='http://127.0.0.1:5000'):
        self.base_url = base_url
        self.session = requests.Session()
        
    def login(self, username='admin', password='admin123'):
        """登录"""
        url = f"{self.base_url}/api/auth/login"
        data = {
            'username': username,
            'password': password
        }
        
        print(f"登录用户: {username}")
        response = self.session.post(url, json=data)
        result = response.json()
        
        if result.get('success'):
            print("✅ 登录成功")
            return True
        else:
            print(f"❌ 登录失败: {result.get('message', '')}")
            return False
    
    def create_test_tables(self):
        """创建测试表"""
        print("\n创建测试表...")
        
        tables_sql = [
            "CREATE TABLE IF NOT EXISTS api_test_users (id INTEGER PRIMARY KEY, name VARCHAR(50));",
            "CREATE TABLE IF NOT EXISTS api_test_logs (log_id INTEGER, message VARCHAR(100));"
        ]
        
        url = f"{self.base_url}/api/sql/execute"
        
        for sql in tables_sql:
            data = {'sql': sql}
            response = self.session.post(url, json=data)
            result = response.json()
            
            if not result.get('success'):
                print(f"❌ 创建表失败: {result.get('message', '')}")
                return False
        
        print("✅ 测试表创建成功")
        return True
    
    def test_create_trigger(self):
        """测试创建触发器"""
        print("\n测试创建触发器...")
        
        url = f"{self.base_url}/api/triggers"
        
        # 测试数据
        triggers = [
            {
                "trigger_name": "api_trg_before_insert",
                "timing": "BEFORE",
                "event": "INSERT",
                "table_name": "api_test_users",
                "statement": "INSERT INTO api_test_logs VALUES (1, 'before insert')"
            },
            {
                "trigger_name": "api_trg_after_insert",
                "timing": "AFTER", 
                "event": "INSERT",
                "table_name": "api_test_users",
                "statement": "INSERT INTO api_test_logs VALUES (2, 'after insert')"
            },
            {
                "trigger_name": "api_trg_before_update",
                "timing": "BEFORE",
                "event": "UPDATE", 
                "table_name": "api_test_users",
                "statement": "INSERT INTO api_test_logs VALUES (3, 'before update')"
            }
        ]
        
        for trigger_data in triggers:
            response = self.session.post(url, json=trigger_data)
            result = response.json()
            
            if result.get('success'):
                print(f"✅ 触发器 {trigger_data['trigger_name']} 创建成功")
            else:
                print(f"❌ 触发器 {trigger_data['trigger_name']} 创建失败: {result.get('message', '')}")
    
    def test_list_triggers(self):
        """测试获取触发器列表"""
        print("\n测试获取触发器列表...")
        
        url = f"{self.base_url}/api/triggers"
        response = self.session.get(url)
        result = response.json()
        
        if result.get('success'):
            triggers = result.get('data', [])
            print(f"✅ 成功获取触发器列表，共 {len(triggers)} 个触发器:")
            for trigger in triggers:
                print(f"  - {trigger['name']}: {trigger['timing']} {trigger['event']} ON {trigger['table_name']}")
        else:
            print(f"❌ 获取触发器列表失败: {result.get('message', '')}")
    
    def test_get_trigger_info(self, trigger_name):
        """测试获取触发器详情"""
        print(f"\n测试获取触发器 {trigger_name} 的详情...")
        
        url = f"{self.base_url}/api/triggers/{trigger_name}"
        response = self.session.get(url)
        result = response.json()
        
        if result.get('success'):
            trigger = result.get('data', {})
            print(f"✅ 触发器 {trigger_name} 详情:")
            print(f"  名称: {trigger.get('name', '')}")
            print(f"  时机: {trigger.get('timing', '')}")
            print(f"  事件: {trigger.get('event', '')}")
            print(f"  表名: {trigger.get('table_name', '')}")
            print(f"  触发器体: {trigger.get('statement', '')}")
            if 'created_at' in trigger:
                import datetime
                created_time = datetime.datetime.fromtimestamp(trigger['created_at'])
                print(f"  创建时间: {created_time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print(f"❌ 获取触发器详情失败: {result.get('message', '')}")
    
    def test_trigger_execution(self):
        """测试触发器执行"""
        print("\n测试触发器执行...")
        
        sql_url = f"{self.base_url}/api/sql/execute"
        
        # 清空测试数据
        print("清空测试数据...")
        cleanup_sqls = [
            "DELETE FROM api_test_users;",
            "DELETE FROM api_test_logs;"
        ]
        
        for sql in cleanup_sqls:
            data = {'sql': sql}
            self.session.post(sql_url, json=data)
        
        # 测试INSERT触发器
        print("测试INSERT触发器...")
        data = {'sql': "INSERT INTO api_test_users VALUES (1, 'Alice');"}
        response = self.session.post(sql_url, json=data)
        result = response.json()
        
        if result.get('success'):
            print("✅ INSERT操作成功")
            
            # 查看日志表
            data = {'sql': "SELECT * FROM api_test_logs;"}
            response = self.session.post(sql_url, json=data)
            result = response.json()
            
            if result.get('success'):
                logs = result.get('data', [])
                print(f"✅ 日志表中有 {len(logs)} 条记录:")
                for log in logs:
                    print(f"  log_id: {log.get('log_id', '')}, message: {log.get('message', '')}")
            else:
                print("❌ 查看日志表失败")
        else:
            print(f"❌ INSERT操作失败: {result.get('message', '')}")
        
        # 测试UPDATE触发器
        print("\n测试UPDATE触发器...")
        data = {'sql': "UPDATE api_test_users SET name = 'Alice Updated' WHERE id = 1;"}
        response = self.session.post(sql_url, json=data)
        result = response.json()
        
        if result.get('success'):
            print("✅ UPDATE操作成功")
            
            # 再次查看日志表
            data = {'sql': "SELECT * FROM api_test_logs ORDER BY log_id;"}
            response = self.session.post(sql_url, json=data)
            result = response.json()
            
            if result.get('success'):
                logs = result.get('data', [])
                print(f"✅ 日志表中现在有 {len(logs)} 条记录:")
                for log in logs:
                    print(f"  log_id: {log.get('log_id', '')}, message: {log.get('message', '')}")
            else:
                print("❌ 查看日志表失败")
        else:
            print(f"❌ UPDATE操作失败: {result.get('message', '')}")
    
    def test_drop_trigger(self, trigger_name):
        """测试删除触发器"""
        print(f"\n测试删除触发器 {trigger_name}...")
        
        url = f"{self.base_url}/api/triggers/{trigger_name}"
        response = self.session.delete(url)
        result = response.json()
        
        if result.get('success'):
            print(f"✅ 触发器 {trigger_name} 删除成功")
        else:
            print(f"❌ 触发器 {trigger_name} 删除失败: {result.get('message', '')}")
    
    def test_error_cases(self):
        """测试错误情况"""
        print("\n测试错误情况...")
        
        # 1. 创建重复触发器
        print("1. 测试创建重复触发器...")
        url = f"{self.base_url}/api/triggers"
        
        duplicate_trigger = {
            "trigger_name": "duplicate_trigger",
            "timing": "BEFORE",
            "event": "INSERT",
            "table_name": "api_test_users",
            "statement": "INSERT INTO api_test_logs VALUES (99, 'duplicate')"
        }
        
        # 创建第一次
        response1 = self.session.post(url, json=duplicate_trigger)
        result1 = response1.json()
        
        # 创建第二次（应该失败）
        response2 = self.session.post(url, json=duplicate_trigger)
        result2 = response2.json()
        
        if result1.get('success') and not result2.get('success'):
            print("✅ 重复创建触发器正确被拒绝")
        else:
            print("❌ 重复创建触发器测试失败")
        
        # 2. 在不存在的表上创建触发器
        print("2. 测试在不存在的表上创建触发器...")
        
        invalid_trigger = {
            "trigger_name": "invalid_table_trigger",
            "timing": "BEFORE",
            "event": "INSERT", 
            "table_name": "nonexistent_table",
            "statement": "INSERT INTO api_test_logs VALUES (98, 'invalid')"
        }
        
        response = self.session.post(url, json=invalid_trigger)
        result = response.json()
        
        if not result.get('success'):
            print("✅ 在不存在的表上创建触发器正确被拒绝")
        else:
            print("❌ 在不存在的表上创建触发器应该失败但成功了")
        
        # 3. 获取不存在的触发器详情
        print("3. 测试获取不存在的触发器详情...")
        
        url = f"{self.base_url}/api/triggers/nonexistent_trigger"
        response = self.session.get(url)
        
        if response.status_code == 404:
            print("✅ 获取不存在的触发器正确返回404")
        else:
            print("❌ 获取不存在的触发器应该返回404")
        
        # 4. 删除不存在的触发器
        print("4. 测试删除不存在的触发器...")
        
        url = f"{self.base_url}/api/triggers/nonexistent_trigger"
        response = self.session.delete(url)
        result = response.json()
        
        if not result.get('success'):
            print("✅ 删除不存在的触发器正确被拒绝")
        else:
            print("❌ 删除不存在的触发器应该失败但成功了")
        
        # 5. 使用IF EXISTS删除不存在的触发器
        print("5. 测试使用IF EXISTS删除不存在的触发器...")
        
        url = f"{self.base_url}/api/triggers/nonexistent_trigger?if_exists=true"
        response = self.session.delete(url)
        result = response.json()
        
        if result.get('success'):
            print("✅ 使用IF EXISTS删除不存在的触发器成功")
        else:
            print("❌ 使用IF EXISTS删除不存在的触发器应该成功")
    
    def cleanup(self):
        """清理测试数据"""
        print("\n清理测试数据...")
        
        sql_url = f"{self.base_url}/api/sql/execute"
        
        # 删除触发器
        trigger_names = [
            "api_trg_before_insert",
            "api_trg_after_insert", 
            "api_trg_before_update",
            "duplicate_trigger"
        ]
        
        for trigger_name in trigger_names:
            url = f"{self.base_url}/api/triggers/{trigger_name}?if_exists=true"
            self.session.delete(url)
        
        # 删除表
        cleanup_sqls = [
            "DROP TABLE IF EXISTS api_test_users;",
            "DROP TABLE IF EXISTS api_test_logs;"
        ]
        
        for sql in cleanup_sqls:
            data = {'sql': sql}
            self.session.post(sql_url, json=data)
        
        print("✅ 清理完成")
    
    def run_all_tests(self):
        """运行所有测试"""
        print("=== 触发器Web API测试开始 ===")
        print("注意: 请确保Web API服务器正在运行（python main.py web）")
        
        try:
            # 登录
            if not self.login():
                return
            
            # 创建测试表
            if not self.create_test_tables():
                return
            
            # 测试创建触发器
            self.test_create_trigger()
            
            # 测试获取触发器列表
            self.test_list_triggers()
            
            # 测试获取触发器详情
            self.test_get_trigger_info("api_trg_before_insert")
            
            # 测试触发器执行
            self.test_trigger_execution()
            
            # 测试错误情况
            self.test_error_cases()
            
            # 测试删除触发器
            self.test_drop_trigger("api_trg_before_insert")
            self.test_drop_trigger("api_trg_after_insert")
            
        except requests.exceptions.ConnectionError:
            print("❌ 无法连接到Web API服务器")
            print("请确保服务器正在运行: python main.py web")
        except Exception as e:
            print(f"❌ 测试过程中发生异常: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            # 清理
            try:
                self.cleanup()
            except:
                pass
            
            print("\n=== 触发器Web API测试结束 ===")

def test_api_with_curl():
    """提供curl命令示例"""
    print("\n=== curl命令测试示例 ===")
    
    base_url = "http://127.0.0.1:5000"
    
    print("1. 登录:")
    print(f"""curl -X POST {base_url}/api/auth/login \\
  -H "Content-Type: application/json" \\
  -d '{{"username": "admin", "password": "admin123"}}' \\
  -c cookies.txt""")
    
    print("\n2. 创建触发器:")
    print(f"""curl -X POST {base_url}/api/triggers \\
  -H "Content-Type: application/json" \\
  -b cookies.txt \\
  -d '{{
    "trigger_name": "test_trigger",
    "timing": "BEFORE", 
    "event": "INSERT",
    "table_name": "test_table",
    "statement": "INSERT INTO log_table VALUES (1, \\"test\\");"
  }}'""")
    
    print("\n3. 获取触发器列表:")
    print(f"curl -X GET {base_url}/api/triggers -b cookies.txt")
    
    print("\n4. 获取触发器详情:")
    print(f"curl -X GET {base_url}/api/triggers/test_trigger -b cookies.txt")
    
    print("\n5. 删除触发器:")
    print(f"curl -X DELETE {base_url}/api/triggers/test_trigger -b cookies.txt")
    
    print("\n6. 使用IF EXISTS删除触发器:")
    print(f"curl -X DELETE '{base_url}/api/triggers/test_trigger?if_exists=true' -b cookies.txt")

if __name__ == "__main__":
    # 运行API测试
    test = TriggerWebAPITest()
    test.run_all_tests()
    
    # 显示curl命令示例
    test_api_with_curl()