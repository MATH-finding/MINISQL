# fixed_test.py
import os
import sys

sys.path.append(".")

from interface.database import SimpleDatabase

# 清理
db_file = "test_fixed.db"
if os.path.exists(db_file):
    os.remove(db_file)

print("🔍 测试索引WHERE查询修复")
print("=" * 50)

db = SimpleDatabase(db_file)

try:
    print("1. 创建表")
    result = db.execute_sql(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))"
    )
    print(f"   结果: {result['success']}")

    print("\n2. 创建索引")
    result = db.execute_sql("CREATE INDEX idx_user_id ON users (id)")
    print(f"   结果: {result['success']}")

    print("\n3. 插入数据")
    result1 = db.execute_sql("INSERT INTO users VALUES (1, 'Alice')")
    result2 = db.execute_sql("INSERT INTO users VALUES (2, 'Bob')")
    print(f"   插入1: {result1['success']}, 插入2: {result2['success']}")

    print("\n4. 检查B+树内容")
    if db.index_manager:
        btree = db.index_manager.get_index("idx_user_id")
        if btree and hasattr(btree.root, "keys"):
            print(f"   B+树键: {btree.root.keys}")
            print(f"   B+树值: {btree.root.values}")  # 应该是 [0, 1]
            print(f"   搜索键1: {btree.search(1)}")
            print(f"   搜索键2: {btree.search(2)}")

    print("\n5. 全表查询验证")
    result = db.execute_sql("SELECT * FROM users")
    print(f"   查询到 {result['rows_returned']} 行")
    for row in result["data"]:
        print(f"   {row}")

    print("\n6. WHERE查询测试（关键测试）")
    result = db.execute_sql("SELECT * FROM users WHERE id = 1")
    print(f"   查询结果: {result['success']}")
    print(f"   返回行数: {result['rows_returned']}")
    if result["data"]:
        for row in result["data"]:
            print(f"   数据: {row}")
    else:
        print("   ❌ 没有返回数据")

    print("\n7. 另一个WHERE查询")
    result = db.execute_sql("SELECT * FROM users WHERE id = 2")
    print(f"   查询Bob: {result['rows_returned']} 行")
    if result["data"]:
        print(f"   数据: {result['data'][0]}")

    print("\n8. 不存在的记录查询")
    result = db.execute_sql("SELECT * FROM users WHERE id = 999")
    print(f"   查询不存在记录: {result['rows_returned']} 行")

finally:
    db.close()

print("\n✅ 测试完成")
