#!/usr/bin/env python3
"""
Simple test for INSERT operations
"""

from interface.database import SimpleDatabase

def main():
    db = SimpleDatabase('test_simple.db')
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
    print(f"Create table: {result.get('success')}")
    
    # Test multiple inserts
    test_data = [
        (1, 'John', 25, 'john@test.com'),
        (2, 'Jane', 30, 'jane@test.com'),
        (3, 'Bob', 35, 'bob@test.com')
    ]
    
    for i, (id, name, age, email) in enumerate(test_data):
        result = db.execute_sql(f"INSERT INTO users (id, name, age, email) VALUES ({id}, '{name}', {age}, '{email}')")
        print(f"Insert {i+1}: {result.get('success')} - {result.get('message', result.get('error', ''))}")
    
    # Test duplicate email (should fail)
    result = db.execute_sql("INSERT INTO users (id, name, age, email) VALUES (4, 'Duplicate', 40, 'john@test.com')")
    print(f"Duplicate email test: {result.get('success')} - {result.get('message', result.get('error', ''))}")

if __name__ == '__main__':
    main()
