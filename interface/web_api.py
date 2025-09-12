"""
Web API 接口
基于 Flask 提供 RESTful API，直接调用 SimpleDatabase
"""

from flask import Flask, request, jsonify, session
from flask_cors import CORS
import os
import json
from typing import Dict, Any, Optional
from .database import SimpleDatabase
import hashlib
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseWebAPI:
    """数据库 Web API"""

    def __init__(self, db_file: str = "web.db"):
        self.app = Flask(__name__)
        self.app.secret_key = os.environ.get('SECRET_KEY', 'minisql-web-secret-key-2024')

        # 启用 CORS 支持前端跨域访问
        CORS(self.app, supports_credentials=True)

        # 数据库连接池（简化版：每个会话一个连接）
        self.db_connections: Dict[str, SimpleDatabase] = {}
        self.default_db_file = db_file

        self._setup_routes()

    def _get_session_id(self) -> str:
        """获取或创建会话ID"""
        if 'session_id' not in session:
            session['session_id'] = hashlib.md5(
                f"{request.remote_addr}_{os.urandom(16).hex()}".encode()
            ).hexdigest()
        return session['session_id']

    def _get_db(self, session_id: Optional[str] = None) -> SimpleDatabase:
        """获取数据库连接"""
        if session_id is None:
            session_id = self._get_session_id()

        if session_id not in self.db_connections:
            self.db_connections[session_id] = SimpleDatabase(self.default_db_file)

        return self.db_connections[session_id]

    def _require_auth(self):
        """检查用户是否已认证"""
        if 'username' not in session:
            return jsonify({
                'success': False,
                'message': '请先登录',
                'error': 'UNAUTHORIZED'
            }), 401

        session_id = self._get_session_id()
        username = session.get('username')

        try:
            db = self._get_db(session_id)
            # 确保数据库实例有正确的认证状态
            if not db.is_authenticated or db.current_user != username:
                db.current_user = username
                db.is_authenticated = True
            return None  # 认证成功
        except Exception as e:
            logger.error(f"认证检查失败: {e}")
            return jsonify({
                'success': False,
                'message': '认证状态异常',
                'error': str(e)
            }), 500

    def _setup_routes(self):
        """设置路由"""

        @self.app.route('/', methods=['GET'])
        def index():
            """首页 - 数据库管理界面"""
            return '''<!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>MiniSQL 数据库管理系统</title>
            <style>
                * {
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }

                body {
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    color: #333;
                }

                .container {
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 20px;
                }

                .header {
                    text-align: center;
                    color: white;
                    margin-bottom: 30px;
                }

                .card {
                    background: white;
                    border-radius: 12px;
                    box-shadow: 0 8px 32px rgba(0,0,0,0.1);
                    padding: 25px;
                    margin-bottom: 20px;
                }

                .login-card {
                    max-width: 400px;
                    margin: 50px auto;
                }

                .main-interface {
                    display: none;
                }

                .form-group {
                    margin-bottom: 20px;
                }

                label {
                    display: block;
                    margin-bottom: 8px;
                    font-weight: 500;
                    color: #555;
                }

                input, textarea, select {
                    width: 100%;
                    padding: 12px;
                    border: 2px solid #e1e5e9;
                    border-radius: 8px;
                    font-size: 14px;
                    transition: border-color 0.3s;
                }

                input:focus, textarea:focus, select:focus {
                    outline: none;
                    border-color: #667eea;
                }

                .btn {
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    border: none;
                    padding: 12px 24px;
                    border-radius: 8px;
                    cursor: pointer;
                    font-size: 14px;
                    font-weight: 500;
                    transition: transform 0.2s;
                    margin-right: 10px;
                }

                .btn:hover {
                    transform: translateY(-2px);
                }

                .btn-secondary {
                    background: #6c757d;
                }

                .btn-danger {
                    background: #dc3545;
                }

                .tabs {
                    display: flex;
                    border-bottom: 2px solid #e1e5e9;
                    margin-bottom: 20px;
                }

                .tab {
                    padding: 12px 20px;
                    cursor: pointer;
                    border-bottom: 2px solid transparent;
                    transition: all 0.3s;
                }

                .tab.active {
                    border-bottom-color: #667eea;
                    color: #667eea;
                    font-weight: 500;
                }

                .tab-content {
                    display: none;
                }

                .tab-content.active {
                    display: block;
                }

                .sql-editor {
                    height: 200px;
                    font-family: 'Courier New', monospace;
                    font-size: 14px;
                    resize: vertical;
                }

                .result-table {
                    overflow-x: auto;
                }

                table {
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 15px;
                }

                th, td {
                    padding: 12px;
                    text-align: left;
                    border-bottom: 1px solid #e1e5e9;
                }

                th {
                    background: #f8f9fa;
                    font-weight: 600;
                }

                tr:hover {
                    background: #f8f9fa;
                }

                .alert {
                    padding: 12px 16px;
                    border-radius: 8px;
                    margin: 10px 0;
                }

                .alert-success {
                    background: #d4edda;
                    color: #155724;
                    border: 1px solid #c3e6cb;
                }

                .alert-error {
                    background: #f8d7da;
                    color: #721c24;
                    border: 1px solid #f5c6cb;
                }

                .user-info {
                    float: right;
                    color: white;
                }

                .sidebar {
                    position: fixed;
                    left: -250px;
                    top: 0;
                    width: 250px;
                    height: 100vh;
                    background: #2c3e50;
                    transition: left 0.3s;
                    z-index: 1000;
                    padding: 20px;
                }

                .sidebar.open {
                    left: 0;
                }

                .sidebar h3 {
                    color: white;
                    margin-bottom: 20px;
                }

                .sidebar ul {
                    list-style: none;
                }

                .sidebar li {
                    margin-bottom: 10px;
                    cursor: pointer;
                    color: #bdc3c7;
                    padding: 8px;
                    border-radius: 4px;
                    transition: background 0.3s;
                }

                .sidebar li:hover {
                    background: #34495e;
                    color: white;
                }

                .menu-btn {
                    position: fixed;
                    top: 20px;
                    left: 20px;
                    z-index: 1001;
                    background: rgba(255,255,255,0.2);
                    color: white;
                    border: none;
                    padding: 10px;
                    border-radius: 50%;
                    cursor: pointer;
                }
            </style>
        </head>
        <body>
            <button class="menu-btn" onclick="toggleSidebar()">☰</button>

            <div class="sidebar" id="sidebar">
                <h3>数据库管理</h3>
                <ul>
                    <li onclick="showTab('sql-tab')">SQL 查询</li>
                    <li onclick="showTab('tables-tab')">表管理</li>
                    <li onclick="showTab('indexes-tab')">索引管理</li>
                    <li onclick="showTab('stats-tab')">统计信息</li>
                    <li onclick="logout()" style="color: #e74c3c;">退出登录</li>
                </ul>
            </div>

            <div class="container">
                <!-- 登录界面 -->
                <div id="login-interface">
                    <div class="header">
                        <h1>🗄️ MiniSQL 数据库管理系统</h1>
                        <p>请登录以继续使用</p>
                    </div>

                    <div class="card login-card">
                        <h2 style="text-align: center; margin-bottom: 25px;">用户登录</h2>

                        <div class="form-group">
                            <label for="username">用户名</label>
                            <input type="text" id="username" placeholder="请输入用户名">
                        </div>

                        <div class="form-group">
                            <label for="password">密码</label>
                            <input type="password" id="password" placeholder="请输入密码">
                        </div>

                        <button class="btn" onclick="login()" style="width: 100%;">登录</button>

                        <div id="login-message"></div>

                        <hr style="margin: 20px 0;">
                        <p style="text-align: center; font-size: 12px; color: #666;">
                            首次使用？请使用 SQL 查询创建用户：<br>
                            <code>CREATE USER admin IDENTIFIED BY 'password'</code>
                        </p>
                    </div>
                </div>

                <!-- 主界面 -->
                <div id="main-interface" class="main-interface">
                    <div class="header">
                        <h1>🗄️ MiniSQL 数据库管理系统</h1>
                        <div class="user-info">
                            欢迎，<span id="current-user">用户</span>
                        </div>
                        <div style="clear: both;"></div>
                    </div>

                    <div class="card">
                        <div class="tabs">
                            <div class="tab active" onclick="showTab('sql-tab')">SQL 查询</div>
                            <div class="tab" onclick="showTab('tables-tab')">表管理</div>
                            <div class="tab" onclick="showTab('indexes-tab')">索引管理</div>
                            <div class="tab" onclick="showTab('stats-tab')">统计信息</div>
                        </div>

                        <!-- SQL 查询标签页 -->
                        <div id="sql-tab" class="tab-content active">
                            <div class="form-group">
                                <label for="sql-input">SQL 语句</label>
                                <textarea id="sql-input" class="sql-editor" placeholder="请输入 SQL 语句..."></textarea>
                            </div>

                            <button class="btn" onclick="executeSql()">执行</button>
                            <button class="btn btn-secondary" onclick="clearSql()">清空</button>

                            <div id="sql-result"></div>
                        </div>

                        <!-- 表管理标签页 -->
                        <div id="tables-tab" class="tab-content">
                            <button class="btn" onclick="loadTables()">刷新表列表</button>
                            <div id="tables-result"></div>
                        </div>

                        <!-- 索引管理标签页 -->
                        <div id="indexes-tab" class="tab-content">
                            <button class="btn" onclick="loadIndexes()">刷新索引列表</button>
                            <div id="indexes-result"></div>
                        </div>

                        <!-- 统计信息标签页 -->
                        <div id="stats-tab" class="tab-content">
                            <button class="btn" onclick="loadStats()">刷新统计信息</button>
                            <div id="stats-result"></div>
                        </div>
                    </div>
                </div>
            </div>

            <script>
                let currentUser = null;

                // 切换侧边栏
                function toggleSidebar() {
                    const sidebar = document.getElementById('sidebar');
                    sidebar.classList.toggle('open');
                }

                // 显示消息
                function showMessage(element, message, isError = false) {
                    const alertClass = isError ? 'alert-error' : 'alert-success';
                    element.innerHTML = `<div class="alert ${alertClass}">${message}</div>`;
                    setTimeout(() => {
                        element.innerHTML = '';
                    }, 5000);
                }

                // 登录
                async function login() {
                    const username = document.getElementById('username').value;
                    const password = document.getElementById('password').value;
                    const messageEl = document.getElementById('login-message');

                    if (!username || !password) {
                        showMessage(messageEl, '请输入用户名和密码', true);
                        return;
                    }

                    try {
                        const response = await fetch('/api/auth/login', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            credentials: 'include',
                            body: JSON.stringify({ username, password })
                        });

                        const result = await response.json();

                        if (result.success) {
                            currentUser = result.user;
                            document.getElementById('current-user').textContent = currentUser;
                            document.getElementById('login-interface').style.display = 'none';
                            document.getElementById('main-interface').style.display = 'block';
                        } else {
                            showMessage(messageEl, result.message, true);
                        }
                    } catch (error) {
                        showMessage(messageEl, '登录失败: ' + error.message, true);
                    }
                }

                // 登出
                async function logout() {
                    try {
                        await fetch('/api/auth/logout', {
                            method: 'POST',
                            credentials: 'include'
                        });
                    } catch (error) {
                        console.error('登出错误:', error);
                    }

                    currentUser = null;
                    document.getElementById('login-interface').style.display = 'block';
                    document.getElementById('main-interface').style.display = 'none';
                    document.getElementById('username').value = '';
                    document.getElementById('password').value = '';
                }

                // 切换标签页
                function showTab(tabId) {
                    // 隐藏所有标签页内容
                    const contents = document.querySelectorAll('.tab-content');
                    contents.forEach(content => content.classList.remove('active'));

                    // 移除所有标签激活状态
                    const tabs = document.querySelectorAll('.tab');
                    tabs.forEach(tab => tab.classList.remove('active'));

                    // 显示选中的标签页
                    document.getElementById(tabId).classList.add('active');

                    // 激活对应的标签
                    const activeTab = Array.from(tabs).find(tab => 
                        tab.textContent.includes(getTabText(tabId))
                    );
                    if (activeTab) activeTab.classList.add('active');

                    // 关闭侧边栏
                    document.getElementById('sidebar').classList.remove('open');
                }

                function getTabText(tabId) {
                    const map = {
                        'sql-tab': 'SQL 查询',
                        'tables-tab': '表管理',
                        'indexes-tab': '索引管理',
                        'stats-tab': '统计信息'
                    };
                    return map[tabId] || '';
                }

                // 执行SQL
                async function executeSql() {
                    const sql = document.getElementById('sql-input').value.trim();
                    const resultEl = document.getElementById('sql-result');

                    if (!sql) {
                        showMessage(resultEl, 'SQL语句不能为空', true);
                        return;
                    }

                    try {
                        const response = await fetch('/api/sql/execute', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            credentials: 'include',
                            body: JSON.stringify({ sql })
                        });

                        const result = await response.json();

                        if (result.success) {
                            let html = `<div class="alert alert-success">执行成功`;
                            if (result.message) html += `: ${result.message}`;
                            html += `</div>`;

                            // 如果是SELECT查询，显示表格
                            if (result.formatted && result.formatted.columns) {
                                html += '<div class="result-table"><table>';

                                // 表头
                                html += '<thead><tr>';
                                result.formatted.columns.forEach(col => {
                                    html += `<th>${col}</th>`;
                                });
                                html += '</tr></thead>';

                                // 数据行
                                html += '<tbody>';
                                result.formatted.rows.forEach(row => {
                                    html += '<tr>';
                                    row.forEach(cell => {
                                        html += `<td>${cell}</td>`;
                                    });
                                    html += '</tr>';
                                });
                                html += '</tbody></table></div>';

                                html += `<p><small>共 ${result.formatted.total} 行记录</small></p>`;
                            }

                            resultEl.innerHTML = html;
                        } else {
                            showMessage(resultEl, result.message || '执行失败', true);
                        }
                    } catch (error) {
                        showMessage(resultEl, '请求失败: ' + error.message, true);
                    }
                }

                // 清空SQL
                function clearSql() {
                    document.getElementById('sql-input').value = '';
                    document.getElementById('sql-result').innerHTML = '';
                }

                // 加载表列表
                async function loadTables() {
                    const resultEl = document.getElementById('tables-result');

                    try {
                        const response = await fetch('/api/tables', {
                            method: 'GET',
                            credentials: 'include'
                        });

                        const result = await response.json();

                        if (result.success) {
                            let html = `<div class="alert alert-success">${result.message}</div>`;

                            if (result.data && result.data.length > 0) {
                                html += '<div class="result-table"><table>';
                                html += '<thead><tr><th>表名</th><th>操作</th></tr></thead><tbody>';

                                result.data.forEach(table => {
                                    html += `<tr>
                                        <td>${table}</td>
                                        <td>
                                            <button class="btn" onclick="showTableInfo('${table}')">查看结构</button>
                                        </td>
                                    </tr>`;
                                });

                                html += '</tbody></table></div>';
                            } else {
                                html += '<p>暂无表</p>';
                            }

                            resultEl.innerHTML = html;
                        } else {
                            showMessage(resultEl, result.message, true);
                        }
                    } catch (error) {
                        showMessage(resultEl, '获取表列表失败: ' + error.message, true);
                    }
                }

                // 显示表信息
                async function showTableInfo(tableName) {
                    try {
                        const response = await fetch(`/api/tables/${tableName}`, {
                            method: 'GET',
                            credentials: 'include'
                        });

                        const result = await response.json();

                        if (result.success) {
                            alert(`表 ${tableName} 的结构信息：\\n${JSON.stringify(result.data, null, 2)}`);
                        } else {
                            alert('获取表信息失败: ' + result.message);
                        }
                    } catch (error) {
                        alert('获取表信息失败: ' + error.message);
                    }
                }

                // 加载索引列表
                async function loadIndexes() {
                    const resultEl = document.getElementById('indexes-result');

                    try {
                        const response = await fetch('/api/indexes', {
                            method: 'GET',
                            credentials: 'include'
                        });

                        const result = await response.json();

                        if (result.success) {
                            let html = '<div class="alert alert-success">索引信息加载成功</div>';

                            if (result.data && Object.keys(result.data).length > 0) {
                                html += '<div class="result-table"><table>';
                                html += '<thead><tr><th>表名</th><th>索引名</th><th>列名</th><th>类型</th></tr></thead><tbody>';

                                Object.entries(result.data).forEach(([tableName, indexes]) => {
                                    Object.entries(indexes).forEach(([indexName, info]) => {
                                        html += `<tr>
                                            <td>${tableName}</td>
                                            <td>${indexName}</td>
                                            <td>${info.column}</td>
                                            <td>${info.unique ? '唯一索引' : '普通索引'}</td>
                                        </tr>`;
                                    });
                                });

                                html += '</tbody></table></div>';
                            } else {
                                html += '<p>暂无索引</p>';
                            }

                            resultEl.innerHTML = html;
                        } else {
                            showMessage(resultEl, result.message, true);
                        }
                    } catch (error) {
                        showMessage(resultEl, '获取索引信息失败: ' + error.message, true);
                    }
                }

                // 加载统计信息
                async function loadStats() {
                    const resultEl = document.getElementById('stats-result');

                    try {
                        const response = await fetch('/api/stats', {
                            method: 'GET',
                            credentials: 'include'
                        });

                        const result = await response.json();

                        if (result.success) {
                            let html = '<div class="alert alert-success">统计信息加载成功</div>';
                            html += '<div class="result-table"><table>';
                            html += '<thead><tr><th>项目</th><th>值</th></tr></thead><tbody>';

                            Object.entries(result.data).forEach(([key, value]) => {
                                html += `<tr><td>${key}</td><td>${value}</td></tr>`;
                            });

                            html += '</tbody></table></div>';
                            resultEl.innerHTML = html;
                        } else {
                            showMessage(resultEl, result.message, true);
                        }
                    } catch (error) {
                        showMessage(resultEl, '获取统计信息失败: ' + error.message, true);
                    }
                }

                // 页面加载时的事件处理
                document.addEventListener('DOMContentLoaded', function() {
                    // 回车键执行SQL
                    document.getElementById('sql-input').addEventListener('keydown', function(e) {
                        if (e.ctrlKey && e.key === 'Enter') {
                            executeSql();
                        }
                    });

                    // 登录表单回车键处理
                    document.getElementById('password').addEventListener('keypress', function(e) {
                        if (e.key === 'Enter') {
                            login();
                        }
                    });
                });
            </script>
        </body>
        </html>'''

        @self.app.route('/api/health', methods=['GET'])
        def health_check():
            """健康检查"""
            return jsonify({
                'status': 'ok',
                'message': 'Database Web API is running',
                'connections': len(self.db_connections)
            })

        @self.app.route('/api/auth/login', methods=['POST'])
        def login():
            """用户登录"""
            try:
                data = request.get_json()
                if not data:
                    return jsonify({
                        'success': False,
                        'message': '请求数据格式错误'
                    }), 400

                username = data.get('username', '').strip()
                password = data.get('password', '')

                if not username or not password:
                    return jsonify({
                        'success': False,
                        'message': '用户名和密码不能为空'
                    }), 400

                session_id = self._get_session_id()
                db = self._get_db(session_id)

                # 尝试登录
                login_result = db.login(username, password)

                if login_result.get('success', False):
                    # 设置会话信息
                    session['username'] = username
                    session.permanent = True

                    return jsonify({
                        'success': True,
                        'message': '登录成功',
                        'user': username
                    })
                else:
                    return jsonify({
                        'success': False,
                        'message': login_result.get('message', '登录失败')
                    }), 401

            except Exception as e:
                logger.error(f"登录错误: {e}")
                return jsonify({
                    'success': False,
                    'message': f'登录失败: {str(e)}'
                }), 500

        @self.app.route('/api/auth/logout', methods=['POST'])
        def logout():
            """用户登出"""
            try:
                session_id = session.get('session_id')
                username = session.get('username')

                if session_id and session_id in self.db_connections:
                    try:
                        self.db_connections[session_id].logout()
                        self.db_connections[session_id].close()
                    except Exception as e:
                        logger.warning(f"关闭数据库连接时出错: {e}")
                    finally:
                        del self.db_connections[session_id]

                session.clear()

                return jsonify({
                    'success': True,
                    'message': f'用户 {username} 已登出'
                })
            except Exception as e:
                logger.error(f"登出错误: {e}")
                return jsonify({
                    'success': True,  # 即使出错也返回成功，因为前端需要清理状态
                    'message': '登出完成'
                })

        @self.app.route('/api/sql/execute', methods=['POST'])
        def execute_sql():
            """执行SQL语句"""
            # 认证检查
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                data = request.get_json()
                if not data:
                    return jsonify({
                        'success': False,
                        'message': '请求数据格式错误'
                    }), 400

                sql = data.get('sql', '').strip()
                if not sql:
                    return jsonify({
                        'success': False,
                        'message': 'SQL语句不能为空'
                    }), 400

                session_id = self._get_session_id()
                db = self._get_db(session_id)

                # 执行SQL
                result = db.execute_sql(sql)

                # 确保返回格式正确
                if not isinstance(result, dict):
                    return jsonify({
                        'success': False,
                        'message': '执行结果格式错误',
                        'error': 'INVALID_RESULT_FORMAT'
                    }), 500

                # 补充可能缺失的字段
                if 'success' not in result:
                    result['success'] = True

                # 格式化SELECT查询结果
                if (result.get('success') and
                    result.get('type') == 'SELECT' and
                    'data' in result and
                    isinstance(result['data'], list)):
                    result['formatted'] = self._format_select_for_web(result['data'])

                return jsonify(result)

            except Exception as e:
                logger.error(f"SQL执行错误: {e}")
                return jsonify({
                    'success': False,
                    'message': f'SQL执行失败: {str(e)}',
                    'error': str(e)
                }), 500

        @self.app.route('/api/tables', methods=['GET'])
        def list_tables():
            """获取表列表"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                session_id = self._get_session_id()
                db = self._get_db(session_id)
                tables = db.list_tables()

                return jsonify({
                    'success': True,
                    'data': tables,
                    'message': f'共{len(tables)}个表'
                })
            except Exception as e:
                logger.error(f"获取表列表错误: {e}")
                return jsonify({
                    'success': False,
                    'message': f'获取表列表失败: {str(e)}'
                }), 500

        @self.app.route('/api/tables/<table_name>', methods=['GET'])
        def get_table_info(table_name: str):
            """获取表信息"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                session_id = self._get_session_id()
                db = self._get_db(session_id)
                info = db.get_table_info(table_name)

                # 检查是否有错误
                if 'error' in info:
                    return jsonify({
                        'success': False,
                        'message': info['error']
                    }), 404

                return jsonify({
                    'success': True,
                    'data': info
                })
            except Exception as e:
                logger.error(f"获取表信息错误: {e}")
                return jsonify({
                    'success': False,
                    'message': f'获取表信息失败: {str(e)}'
                }), 500

        @self.app.route('/api/indexes', methods=['GET'])
        def list_all_indexes():
            """获取所有索引"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                session_id = self._get_session_id()
                db = self._get_db(session_id)

                if hasattr(db, 'list_all_indexes'):
                    indexes = db.list_all_indexes()
                    return jsonify({
                        'success': True,
                        'data': indexes
                    })
                else:
                    return jsonify({
                        'success': False,
                        'message': '当前版本不支持索引查询功能'
                    }), 501

            except Exception as e:
                logger.error(f"获取索引信息错误: {e}")
                return jsonify({
                    'success': False,
                    'message': f'获取索引信息失败: {str(e)}'
                }), 500

        @self.app.route('/api/stats', methods=['GET'])
        def get_database_stats():
            """获取数据库统计信息"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                session_id = self._get_session_id()
                db = self._get_db(session_id)
                stats = db.get_database_stats()

                # 格式化统计信息，便于前端显示
                formatted_stats = {}
                for key, value in stats.items():
                    if key == 'cache_stats' and isinstance(value, dict):
                        for cache_key, cache_value in value.items():
                            formatted_stats[f'缓存_{cache_key}'] = cache_value
                    else:
                        formatted_stats[self._translate_stat_key(key)] = value

                return jsonify({
                    'success': True,
                    'data': formatted_stats
                })
            except Exception as e:
                logger.error(f"获取统计信息错误: {e}")
                return jsonify({
                    'success': False,
                    'message': f'获取统计信息失败: {str(e)}'
                }), 500

    def _format_select_for_web(self, data: list) -> dict:
        """将SELECT结果格式化为适合前端展示的格式"""
        if not data:
            return {'columns': [], 'rows': [], 'total': 0}

        # 获取列名
        columns = list(data[0].keys()) if data else []

        # 转换数据行
        rows = []
        for row in data:
            formatted_row = []
            for col in columns:
                value = row.get(col)
                # 处理None值和特殊类型
                if value is None:
                    formatted_row.append('')
                elif isinstance(value, bool):
                    formatted_row.append('是' if value else '否')
                else:
                    formatted_row.append(str(value))
            rows.append(formatted_row)

        return {
            'columns': columns,
            'rows': rows,
            'total': len(rows)
        }

    def _translate_stat_key(self, key: str) -> str:
        """翻译统计信息的键名"""
        translations = {
            'database_file': '数据库文件',
            'file_size_pages': '文件大小(页)',
            'tables_count': '表数量',
            'indexes_count': '索引数量',
            'cache_hits': '缓存命中次数',
            'cache_misses': '缓存未命中次数',
            'hit_rate': '缓存命中率',
            'cached_pages': '已缓存页数',
            'cache_size': '缓存大小'
        }
        return translations.get(key, key)

    def run(self, host: str = '127.0.0.1', port: int = 5000, debug: bool = False):
        """启动Web服务器"""
        print(f"🌐 数据库 Web API 启动中...")
        print(f"   地址: http://{host}:{port}")
        print(f"   数据库文件: {self.default_db_file}")
        print(f"   调试模式: {'开启' if debug else '关闭'}")

        try:
            self.app.run(host=host, port=port, debug=debug)
        except KeyboardInterrupt:
            print("\n正在关闭服务器...")
            self.close_all_connections()

    def close_all_connections(self):
        """关闭所有数据库连接"""
        print("关闭数据库连接...")
        for session_id, db in list(self.db_connections.items()):
            try:
                db.close()
            except Exception as e:
                logger.warning(f"关闭连接 {session_id} 时出错: {e}")
        self.db_connections.clear()
        print("所有连接已关闭")


def create_web_app(db_file: str = "web.db") -> Flask:
    """创建Flask应用实例"""
    web_api = DatabaseWebAPI(db_file)
    return web_api.app


if __name__ == "__main__":
    # 直接运行时的测试代码
    web_api = DatabaseWebAPI("test_web.db")
    try:
        web_api.run(debug=True)
    finally:
        web_api.close_all_connections()
