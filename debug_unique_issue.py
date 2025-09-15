#!/usr/bin/env python3
"""
Debug unique constraint issue
"""

from interface.database import SimpleDatabase

def main():
    db = SimpleDatabase('debug_unique.db')
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
    
    # Check unique indexes
    if hasattr(db.executor.index_manager, 'get_unique_indexes_for_table'):
        unique_indexes = db.executor.index_manager.get_unique_indexes_for_table('users')
        print(f"Unique indexes: {len(unique_indexes)}")
        for idx in unique_indexes:
            print(f"  Index: {idx.name}, columns: {idx.columns}")
    
    # Try to insert first record
    print("\n=== Inserting first record ===")
    result = db.execute_sql("INSERT INTO users (id, name, age, email) VALUES (1, 'John', 25, 'john@test.com')")
    print(f"Insert result: {result}")

if __name__ == '__main__':
    main()
