"""
/tests/test_lexer.py

词法分析器单元测试
"""
import sys
import os

# 将上级目录（项目根目录）添加到 sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sql.lexer import SQLLexer, TokenType, Token

passed = 0
failed = 0

def assert_test(test_name, condition, message=""):
    global passed, failed
    if condition:
        print(f"✅ PASS: {test_name}")
        passed += 1
    else:
        print(f"❌ FAIL: {test_name} - {message}")
        failed += 1

def print_test_summary():
    total = passed + failed
    print("\n" + "=" * 60)
    print(f"📊 测试结果统计: 通过: {passed}  失败: {failed}")
    if total > 0:
        print(f"📈 通过率: {passed / total * 100:.1f}%")
    if failed == 0:
        print("🎉 所有测试通过！")
    else:
        print("⚠️  部分测试失败，请检查相关功能")

def test_keywords_and_identifiers():
    sql = "SELECT id, user_name FROM users_table WHERE id = 1;"
    lexer = SQLLexer(sql)
    try:
        tokens = lexer.tokenize()
        for token in tokens:
            print(str(token))
        expected_tokens = [
            (TokenType.SELECT, 'SELECT'),
            (TokenType.IDENTIFIER, 'id'),
            (TokenType.COMMA, ','),
            (TokenType.IDENTIFIER, 'user_name'),
            (TokenType.FROM, 'FROM'),
            (TokenType.IDENTIFIER, 'users_table'),
            (TokenType.WHERE, 'WHERE'),
            (TokenType.IDENTIFIER, 'id'),
            (TokenType.EQUALS, '='),
            (TokenType.NUMBER, '1'),
            (TokenType.SEMICOLON, ';'),
            (TokenType.EOF, '')
        ]
        cond = len(tokens) == len(expected_tokens) and all(
            tokens[i].type == expected_tokens[i][0] and tokens[i].value == expected_tokens[i][1]
            for i in range(len(tokens))
        )
        assert_test("测试SQL关键字和标识符的识别", cond)
    except Exception as e:
        assert_test("测试SQL关键字和标识符的识别", False, str(e))

def test_ddl_statements():
    sql = "CREATE TABLE students (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL);"
    lexer = SQLLexer(sql)
    try:
        tokens = lexer.tokenize()
        for token in tokens:
            print(str(token))
        token_types = [t.type for t in tokens]
        expected_types = [
            TokenType.CREATE, TokenType.TABLE, TokenType.IDENTIFIER, TokenType.LEFT_PAREN,
            TokenType.IDENTIFIER, TokenType.INTEGER, TokenType.PRIMARY, TokenType.KEY, TokenType.COMMA,
            TokenType.IDENTIFIER, TokenType.VARCHAR, TokenType.LEFT_PAREN, TokenType.NUMBER, TokenType.RIGHT_PAREN,
            TokenType.NOT, TokenType.NULL, TokenType.RIGHT_PAREN, TokenType.SEMICOLON, TokenType.EOF
        ]
        cond = token_types == expected_types
        assert_test("测试CREATE/DROP/ALTER等DDL语句的关键字", cond)
    except Exception as e:
        assert_test("测试CREATE/DROP/ALTER等DDL语句的关键字", False, str(e))

def test_dml_statements():
    sql = "INSERT INTO t (c1) VALUES ('hello'); UPDATE t SET c1 = 'world'; DELETE FROM t;"
    lexer = SQLLexer(sql)
    try:
        tokens = lexer.tokenize()
        for token in tokens:
            print(str(token))
        token_types = [t.type for t in tokens]
        expected_types = [
            TokenType.INSERT, TokenType.INTO, TokenType.IDENTIFIER, TokenType.LEFT_PAREN, TokenType.IDENTIFIER, 
            TokenType.RIGHT_PAREN, TokenType.VALUES, TokenType.LEFT_PAREN, TokenType.STRING, TokenType.RIGHT_PAREN, 
            TokenType.SEMICOLON,
            TokenType.UPDATE, TokenType.IDENTIFIER, TokenType.SET, TokenType.IDENTIFIER, TokenType.EQUALS, 
            TokenType.STRING, TokenType.SEMICOLON,
            TokenType.DELETE, TokenType.FROM, TokenType.IDENTIFIER, TokenType.SEMICOLON,
            TokenType.EOF
        ]
        cond = token_types == expected_types
        assert_test("测试INSERT/UPDATE/DELETE等DML语句的关键字", cond)
    except Exception as e:
        assert_test("测试INSERT/UPDATE/DELETE等DML语句的关键字", False, str(e))

def test_operators_and_separators():
    sql = "a > b, c < d, e >= f, g <= h, i != j, k = l.m * (n);"
    lexer = SQLLexer(sql)
    try:
        tokens = lexer.tokenize()
        for token in tokens:
            print(str(token))
        token_values = [t.value for t in tokens if t.type != TokenType.IDENTIFIER]
        expected_values = [
            '>', ',', '<', ',', '>=', ',', '<=', ',', '!=', ',', '=', '.', '*', '(', ')', ';', ''
        ]
        cond = token_values == expected_values
        assert_test("测试所有运算符和分隔符", cond)
    except Exception as e:
        assert_test("测试所有运算符和分隔符", False, str(e))

def test_literals():
    sql = "123, 45.67, 'a string', \"another string\", -99;"
    lexer = SQLLexer(sql)
    try:
        tokens = lexer.tokenize()
        for token in tokens:
            print(str(token))
        expected = [
            (TokenType.NUMBER, '123'),
            (TokenType.COMMA, ','),
            (TokenType.NUMBER, '45.67'),
            (TokenType.COMMA, ','),
            (TokenType.STRING, 'a string'),
            (TokenType.COMMA, ','),
            (TokenType.STRING, 'another string'),
            (TokenType.COMMA, ','),
            (TokenType.NUMBER, '-99'),
            (TokenType.SEMICOLON, ';'),
            (TokenType.EOF, '')
        ]
        cond = len(tokens) == len(expected) and all(
            tokens[i].type == expected[i][0] and tokens[i].value == expected[i][1]
            for i in range(len(tokens))
        )
        assert_test("测试字符串、数字等常量", cond)
    except Exception as e:
        assert_test("测试字符串、数字等常量", False, str(e))

def test_line_and_column_tracking():
    sql = "SELECT\n  id\nFROM users;"
    lexer = SQLLexer(sql)
    try:
        tokens = lexer.tokenize()
        for token in tokens:
            print(str(token))
        select_token = tokens[0]
        id_token = tokens[1]
        from_token = tokens[2]
        cond = (
            select_token.line == 1 and select_token.column == 1 and
            id_token.line == 2 and id_token.column == 3 and  # 两个空格+id
            from_token.line == 3 and from_token.column == 1
        )
        assert_test("测试行号和列号的跟踪", cond)
    except Exception as e:
        assert_test("测试行号和列号的跟踪", False, str(e))

def test_illegal_character_error():
    sql = "SELECT id FROM users WHERE id @ 1;"
    lexer = SQLLexer(sql)
    try:
        tokens = lexer.tokenize()
        for token in tokens:
            print(str(token))
        assert_test("测试非法字符错误提示", False, "Expected SyntaxError not raised")
    except SyntaxError as e:
        line = getattr(e, 'line', '?')
        column = getattr(e, 'column', '?')
        assert_test("测试非法字符错误提示", True, f"Expected position: {line}, {column}")

def test_unclosed_string_error():
    sql = "SELECT 'hello world;"
    lexer = SQLLexer(sql)
    try:
        tokens = lexer.tokenize()
        for token in tokens:
            print(str(token))
        assert_test("测试未闭合的字符串错误", False, "Expected SyntaxError not raised")
    except SyntaxError as e:
        line = getattr(e, 'line', '?')
        column = getattr(e, 'column', '?')
        assert_test("测试未闭合的字符串错误", True, f"Expected position: {line}, {column}")

def test_all_transaction_keywords():
    sql = "BEGIN TRANSACTION; COMMIT; ROLLBACK; SET AUTOCOMMIT=0; SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
    lexer = SQLLexer(sql)
    try:
        tokens = lexer.tokenize()
        for token in tokens:
            print(str(token))
        token_types = [t.type for t in tokens]
        expected_types = [
            TokenType.BEGIN, TokenType.TRANSACTION, TokenType.SEMICOLON,
            TokenType.COMMIT, TokenType.SEMICOLON,
            TokenType.ROLLBACK, TokenType.SEMICOLON,
            TokenType.SET, TokenType.AUTOCOMMIT, TokenType.EQUALS, TokenType.NUMBER, TokenType.SEMICOLON,
            TokenType.SET, TokenType.SESSION, TokenType.TRANSACTION, TokenType.ISOLATION, TokenType.LEVEL, 
            TokenType.REPEATABLE, TokenType.READ, TokenType.SEMICOLON,
            TokenType.EOF
        ]
        cond = token_types == expected_types
        assert_test("测试所有事务相关的关键字", cond)
    except Exception as e:
        assert_test("测试所有事务相关的关键字", False, str(e))

def main():
    test_keywords_and_identifiers()
    test_ddl_statements()
    test_dml_statements()
    test_operators_and_separators()
    test_literals()
    test_line_and_column_tracking()
    test_illegal_character_error()
    test_unclosed_string_error()
    test_all_transaction_keywords()
    print_test_summary()

if __name__ == "__main__":
    main()