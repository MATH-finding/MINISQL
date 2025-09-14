#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sql.lexer import SQLLexer

def test_backslash():
    # 测试单个反斜杠
    print("Testing single backslash:")
    try:
        lexer = SQLLexer("\\")
        tokens = lexer.tokenize()
        print("Success! Tokens:", [(t.type.name, t.value) for t in tokens])
    except Exception as e:
        print("Error:", str(e))
    
    # 测试反斜杠在SQL语句中
    print("\nTesting backslash in SQL:")
    try:
        lexer = SQLLexer("SELECT * FROM table WHERE path = 'C:\\\\data\\\\file.txt'")
        tokens = lexer.tokenize()
        print("Success! Tokens:", [(t.type.name, t.value) for t in tokens])
    except Exception as e:
        print("Error:", str(e))
    
    # 测试可能的游标语法
    print("\nTesting potential cursor syntax:")
    try:
        lexer = SQLLexer("\\cursor_command")
        tokens = lexer.tokenize()
        print("Success! Tokens:", [(t.type.name, t.value) for t in tokens])
    except Exception as e:
        print("Error:", str(e))

if __name__ == "__main__":
    test_backslash()