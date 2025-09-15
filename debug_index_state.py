#!/usr/bin/env python3
"""
Debug index state
"""

from interface.database import SimpleDatabase

def main():
    db = SimpleDatabase('debug_index.db')
    db.login('admin', 'admin123')
    
    # Create table
    result = db.execute_sql('''
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        age INTEGER,
        email VARCHAR(100) UNIQUE
    )
    ''')
    print(f"Create table result: {result}")
    
    # Check index state before any inserts
    print("\n=== Index state before inserts ===")
    if hasattr(db.executor.index_manager, 'get_unique_indexes_for_table'):
        unique_indexes = db.executor.index_manager.get_unique_indexes_for_table('users')
        for idx in unique_indexes:
            print(f"Index: {idx.name}")
            if idx.name in db.executor.index_manager.indexes:
                index_info = db.executor.index_manager.indexes[idx.name]
                print(f"  Index info: {index_info.table_name}, {index_info.column_name}, unique={index_info.is_unique}")
                
                # Get B+ tree
                btree = db.executor.index_manager.get_index(idx.name)
                if btree:
                    print(f"  B+ tree root: {btree.root_page_id}")
                    # Try to search for a key that shouldn't exist
                    search_result = btree.search('john@test.com')
                    print(f"  Search for 'john@test.com': {search_result}")
                    
                    # Try to get all keys
                    try:
                        all_keys = list(btree.get_all_keys())
                        print(f"  All keys in tree: {all_keys}")
                    except Exception as e:
                        print(f"  Error getting keys: {e}")
    
    # Try to insert
    print("\n=== Attempting insert ===")
    result = db.execute_sql("INSERT INTO users (id, name, age, email) VALUES (1, 'John', 25, 'john@test.com')")
    print(f"Insert result: {result}")

if __name__ == '__main__':
    main()
