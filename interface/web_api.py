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

        # 保存文件路径并创建单一数据库连接
        self.default_db_file = db_file  # 添加这行
        self.db = SimpleDatabase(self.default_db_file)
        # 添加执行器引用以支持游标操作
        self.executor = self.db.executor

        self._setup_routes()

    def _get_session_id(self) -> str:
        """获取或创建会话ID"""
        if 'session_id' not in session:
            session['session_id'] = hashlib.md5(
                f"{request.remote_addr}_{os.urandom(16).hex()}".encode()
            ).hexdigest()
        return session['session_id']

    def _get_db(self, session_id: Optional[str] = None) -> SimpleDatabase:
        """获取数据库连接 - 支持多会话"""
        if session_id:
            # 为每个Web会话创建独立的数据库会话
            if not hasattr(self, '_web_sessions'):
                self._web_sessions = {}
            
            if not hasattr(self, '_user_selected_sessions'):
                self._user_selected_sessions = {}
            
            if session_id not in self._web_sessions:
                # 创建新的数据库会话
                self._web_sessions[session_id] = self.db.new_session()
            
            # 检查用户是否通过 \session use 选择了特定会话
            if session_id in self._user_selected_sessions:
                # 使用用户选择的会话 - 确保真正切换
                selected_session_id = self._user_selected_sessions[session_id]
                success = self.db.use_session(selected_session_id)
                if not success:
                    # 如果切换失败，回退到默认会话
                    self.db.use_session(self._web_sessions[session_id])
            else:
                # 使用默认的Web会话
                self.db.use_session(self._web_sessions[session_id])
        
        return self.db

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
    <title>MiniSQL Enterprise Database Management</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --primary-color: #1e40af;
            --primary-dark: #1e3a8a;
            --secondary-color: #64748b;
            --success-color: #059669;
            --error-color: #dc2626;
            --warning-color: #d97706;
            --neutral-50: #f8fafc;
            --neutral-100: #f1f5f9;
            --neutral-200: #e2e8f0;
            --neutral-300: #cbd5e1;
            --neutral-400: #94a3b8;
            --neutral-500: #64748b;
            --neutral-600: #475569;
            --neutral-700: #334155;
            --neutral-800: #1e293b;
            --neutral-900: #0f172a;
            --border-radius: 8px;
            --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.05);
            --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
            --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1);
            --shadow-xl: 0 20px 25px -5px rgb(0 0 0 / 0.1), 0 8px 10px -6px rgb(0 0 0 / 0.1);
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 50%, #f8fafc 100%);
            min-height: 100vh;
            color: var(--neutral-700);
            font-size: 14px;
            line-height: 1.5;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }

        .header {
            text-align: center;
            color: var(--neutral-800);
            margin-bottom: 40px;
        }

        .header h1 {
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 8px;
            letter-spacing: -0.025em;
            background: linear-gradient(135deg, var(--primary-color) 0%, var(--neutral-700) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }

        .header p {
            font-size: 1.125rem;
            opacity: 0.8;
            font-weight: 400;
            color: var(--neutral-600);
        }

        .card {
            background: white;
            border-radius: 16px;
            box-shadow: var(--shadow-xl);
            border: 1px solid var(--neutral-200);
            backdrop-filter: blur(10px);
            overflow: hidden;
        }

        .login-card {
            max-width: 400px;
            margin: 50px auto;
            padding: 40px;
        }

        .main-interface {
            display: none;
        }

        .form-group {
            margin-bottom: 24px;
        }

        label {
            display: block;
            margin-bottom: 8px;
            font-weight: 500;
            color: var(--neutral-700);
            font-size: 14px;
        }

        input, textarea, select {
            width: 100%;
            padding: 12px 16px;
            border: 1px solid var(--neutral-300);
            border-radius: var(--border-radius);
            font-size: 14px;
            font-family: inherit;
            transition: all 0.15s ease;
            background: white;
        }

        input:focus, textarea:focus, select:focus {
            outline: none;
            border-color: var(--primary-color);
            box-shadow: 0 0 0 3px rgba(30, 64, 175, 0.1);
        }

        .btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            background: var(--primary-color);
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: var(--border-radius);
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
            font-family: inherit;
            transition: all 0.15s ease;
            margin-right: 8px;
            text-decoration: none;
            min-height: 40px;
        }

        .btn:hover {
            background: var(--primary-dark);
            transform: translateY(-1px);
            box-shadow: var(--shadow-md);
        }

        .btn:active {
            transform: translateY(0);
        }

        .btn-secondary {
            background: var(--neutral-500);
        }

        .btn-secondary:hover {
            background: var(--neutral-600);
        }

        .btn-danger {
            background: var(--error-color);
        }

        .btn-danger:hover {
            background: #b91c1c;
        }

        .btn-success {
            background: var(--success-color);
        }

        .btn-success:hover {
            background: #047857;
        }

        .btn-warning {
            background: var(--warning-color);
        }

        .btn-warning:hover {
            background: #b45309;
        }

        /* 事务管理样式 */
        .transaction-panel {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 20px;
        }

        .transaction-status, .transaction-controls, .isolation-settings, .autocommit-settings, .session-management {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            border: 1px solid #e9ecef;
        }

        .status-info {
            margin: 15px 0;
        }

        .status-item {
            display: flex;
            justify-content: space-between;
            margin-bottom: 8px;
            padding: 8px 0;
            border-bottom: 1px solid #e9ecef;
        }

        .status-item:last-child {
            border-bottom: none;
        }

        .status-label {
            font-weight: 600;
            color: var(--neutral-700);
        }

        .control-group {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }

        .control-group button {
            flex: 1;
            min-width: 120px;
        }

        .control-group button:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }

        .session-management {
            grid-column: 1 / -1;
        }

        .sessions-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }

        .sessions-table th,
        .sessions-table td {
            padding: 12px;
            text-align: left;
            border: 1px solid #dee2e6;
        }

        .sessions-table th {
            background: #f8f9fa;
            font-weight: 600;
        }

        .sessions-table tr:nth-child(even) {
            background: #f8f9fa;
        }

        /* Shell命令样式 */
        .shell-help {
            background: #f8f9fa;
            border: 1px solid #e9ecef;
            border-radius: 6px;
            padding: 15px;
            margin: 10px 0;
            font-family: 'Courier New', monospace;
            font-size: 13px;
            line-height: 1.4;
            white-space: pre-wrap;
        }

        .shell-help pre {
            margin: 0;
            color: #495057;
        }

        .btn-sm {
            padding: 8px 16px;
            font-size: 13px;
            min-height: 32px;
        }

        .tabs {
            display: flex;
            border-bottom: 1px solid var(--neutral-200);
            background: linear-gradient(135deg, var(--neutral-50) 0%, #f8fafc 100%);
            border-radius: 12px 12px 0 0;
            padding: 0 8px;
        }

        .tab {
            padding: 16px 24px;
            cursor: pointer;
            border-bottom: 2px solid transparent;
            transition: all 0.15s ease;
            font-weight: 500;
            color: var(--neutral-600);
            border-radius: 8px 8px 0 0;
            margin: 8px 4px 0 4px;
            position: relative;
        }

        .tab.active {
            background: white;
            color: var(--primary-color);
            border-bottom-color: var(--primary-color);
            box-shadow: var(--shadow-sm);
        }

        .tab:hover:not(.active) {
            background: rgba(30, 64, 175, 0.05);
            color: var(--primary-color);
        }

        .tab-content {
            display: none;
            padding: 32px;
        }

        .tab-content.active {
            display: block;
        }

        .sql-editor {
            height: 200px;
            font-family: 'SF Mono', 'Monaco', 'Inconsolata', 'Roboto Mono', monospace;
            font-size: 14px;
            resize: vertical;
            line-height: 1.5;
            background: var(--neutral-50);
            border: 1px solid var(--neutral-200);
        }

        .result-table {
            overflow-x: auto;
            border-radius: var(--border-radius);
            border: 1px solid var(--neutral-200);
            margin-top: 20px;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
        }

        th, td {
            padding: 16px 20px;
            text-align: left;
            border-bottom: 1px solid var(--neutral-100);
            font-size: 14px;
        }

        th {
            background: linear-gradient(135deg, var(--neutral-50) 0%, #f8fafc 100%);
            font-weight: 600;
            color: var(--neutral-700);
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.025em;
        }

        tr:hover {
            background: var(--neutral-50);
        }

        .alert {
            padding: 16px 20px;
            border-radius: var(--border-radius);
            margin: 16px 0;
            font-size: 14px;
            border: 1px solid;
        }

        .alert-success {
            background: #f0fdf4;
            color: var(--success-color);
            border-color: #bbf7d0;
        }

        .alert-error {
            background: #fef2f2;
            color: var(--error-color);
            border-color: #fecaca;
        }

        .alert-info {
            background: #eff6ff;
            color: var(--primary-color);
            border-color: #bfdbfe;
        }

        .user-info {
            float: right;
            color: var(--neutral-700);
            font-weight: 500;
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .user-avatar {
            width: 32px;
            height: 32px;
            border-radius: 50%;
            background: linear-gradient(135deg, var(--primary-color) 0%, var(--primary-dark) 100%);
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 600;
            font-size: 14px;
            color: white;
        }

        .sidebar {
            position: fixed;
            left: -280px;
            top: 0;
            width: 280px;
            height: 100vh;
            background: linear-gradient(180deg, var(--neutral-900) 0%, var(--neutral-800) 100%);
            transition: left 0.3s ease;
            z-index: 1000;
            padding: 0;
            border-right: 1px solid var(--neutral-700);
            box-shadow: var(--shadow-xl);
        }

        .sidebar.open {
            left: 0;
        }

        .sidebar-header {
            padding: 24px;
            border-bottom: 1px solid var(--neutral-700);
            background: linear-gradient(135deg, var(--primary-color) 0%, var(--primary-dark) 100%);
        }

        .sidebar h3 {
            color: white;
            font-size: 18px;
            font-weight: 600;
            margin: 0;
        }

        .sidebar-nav {
            padding: 16px 0;
        }

        .sidebar ul {
            list-style: none;
        }

        .sidebar li {
            margin: 2px 16px;
            cursor: pointer;
            color: var(--neutral-400);
            padding: 12px 16px;
            border-radius: var(--border-radius);
            transition: all 0.15s ease;
            font-weight: 500;
            display: flex;
            align-items: center;
            gap: 12px;
            font-size: 14px;
        }

        .sidebar li:hover {
            background: var(--neutral-700);
            color: white;
        }

        .sidebar li.danger {
            color: #f87171;
        }

        .sidebar li.danger:hover {
            background: rgba(220, 38, 38, 0.1);
            color: #ef4444;
        }

        .menu-btn {
            position: fixed;
            top: 24px;
            left: 24px;
            z-index: 1001;
            background: white;
            color: var(--neutral-600);
            border: 1px solid var(--neutral-200);
            padding: 12px;
            border-radius: 12px;
            cursor: pointer;
            transition: all 0.15s ease;
            width: 48px;
            height: 48px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 18px;
            box-shadow: var(--shadow-md);
        }

        .menu-btn:hover {
            background: var(--neutral-50);
            transform: translateY(-1px);
            box-shadow: var(--shadow-lg);
        }

        .loading {
            display: inline-block;
            width: 16px;
            height: 16px;
            border: 2px solid var(--neutral-300);
            border-top: 2px solid var(--primary-color);
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin-right: 8px;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .modal-overlay {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(15, 23, 42, 0.5);
            backdrop-filter: blur(4px);
            z-index: 2000;
            display: flex;
            align-items: center;
            justify-content: center;
            animation: fadeIn 0.15s ease;
        }

        .modal-dialog {
            background: white;
            border-radius: 16px;
            width: 90%;
            max-width: 1200px;
            max-height: 90vh;
            box-shadow: var(--shadow-xl);
            display: flex;
            flex-direction: column;
            animation: slideUp 0.15s ease;
            border: 1px solid var(--neutral-200);
        }

        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }

        @keyframes slideUp {
            from { transform: translateY(20px); opacity: 0; }
            to { transform: translateY(0); opacity: 1; }
        }

        .modal-header {
            padding: 24px 32px;
            border-bottom: 1px solid var(--neutral-200);
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: linear-gradient(135deg, var(--neutral-50) 0%, #f8fafc 100%);
        }

        .modal-title {
            font-size: 20px;
            font-weight: 600;
            color: var(--neutral-800);
        }

        .modal-close {
            background: var(--neutral-100);
            color: var(--neutral-600);
            border: none;
            padding: 8px 16px;
            border-radius: var(--border-radius);
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
            transition: all 0.15s ease;
        }

        .modal-close:hover {
            background: var(--neutral-200);
            color: var(--neutral-700);
        }

        .action-bar {
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 24px;
            padding-bottom: 20px;
            border-bottom: 1px solid var(--neutral-200);
        }

        .search-box {
            flex: 1;
            max-width: 300px;
        }

        .badge {
            display: inline-flex;
            align-items: center;
            padding: 4px 8px;
            border-radius: 6px;
            font-size: 12px;
            font-weight: 500;
            background: var(--neutral-100);
            color: var(--neutral-600);
        }

        .badge-primary {
            background: rgba(30, 64, 175, 0.1);
            color: var(--primary-color);
        }

        .badge-success {
            background: rgba(5, 150, 105, 0.1);
            color: var(--success-color);
        }

        .badge-warning {
            background: rgba(217, 119, 6, 0.1);
            color: var(--warning-color);
        }

        @media (max-width: 768px) {
            .container {
                padding: 16px;
            }
            
            .header h1 {
                font-size: 2rem;
            }
            
            .tabs {
                overflow-x: auto;
                scrollbar-width: none;
                -ms-overflow-style: none;
            }
            
            .tabs::-webkit-scrollbar {
                display: none;
            }
            
            .tab-content {
                padding: 20px;
            }
            
            .modal-dialog {
                width: 95%;
                max-height: 95vh;
            }
            
            .modal-header {
                padding: 20px;
            }
        }
    </style>
</head>
<body>
    <!-- 其余HTML结构保持完全相同 -->
    <button class="menu-btn" onclick="toggleSidebar()">☰</button>

    <div class="sidebar" id="sidebar">
        <div class="sidebar-header">
            <h3>数据库管理</h3>
        </div>
        <div class="sidebar-nav">
            <ul>
                <li onclick="showTab('sql-tab')">
                    <span>📊</span>
                    SQL 查询
                </li>
                <li onclick="showTab('tables-tab')">
                    <span>🗃️</span>
                    表管理
                </li>
                <li onclick="showTab('views-tab')">
                    <span>👁️</span>
                    视图管理
                </li>
                <li onclick="showTab('indexes-tab')">
                    <span>⚡</span>
                    索引管理
                </li>
                <li onclick="showTab('stats-tab')">
                    <span>📈</span>
                    统计信息
                </li>
                <li onclick="logout()" class="danger">
                    <span>🚪</span>
                    退出登录
                </li>
            </ul>
        </div>
    </div>

    <div class="container">
        <!-- 登录界面 -->
        <div id="login-interface">
            <div class="header">
                <h1>MiniSQL Enterprise</h1>
                <p>专业数据库管理平台</p>
            </div>

            <div class="card login-card">
                <h2 style="text-align: center; margin-bottom: 32px; font-weight: 600; color: var(--neutral-800);">用户登录</h2>

                <div class="form-group">
                    <label for="username">用户名</label>
                    <input type="text" id="username" placeholder="请输入用户名">
                </div>

                <div class="form-group">
                    <label for="password">密码</label>
                    <input type="password" id="password" placeholder="请输入密码">
                </div>

                <button class="btn" onclick="login()" style="width: 100%;">登录系统</button>

                <div id="login-message"></div>

                <div style="margin: 32px 0; height: 1px; background: var(--neutral-200);"></div>
                <p style="text-align: center; font-size: 13px; color: var(--neutral-500);">
                    默认管理员账户：admin / admin123
                </p>
            </div>
        </div>

        <!-- 主界面 -->
        <div id="main-interface" class="main-interface">
            <div class="header">
                <h1>MiniSQL Enterprise</h1>
                <div class="user-info">
                    <div class="user-avatar" id="user-avatar">A</div>
                    <span>欢迎，<span id="current-user">用户</span></span>
                </div>
                <div style="clear: both;"></div>
            </div>

            <div class="card">
                <div class="tabs">
                    <div class="tab active" onclick="showTab('sql-tab')">SQL 查询</div>
                    <div class="tab" onclick="showTab('transaction-tab')">事务管理</div>
                    <div class="tab" onclick="showTab('tables-tab')">表管理</div>
                    <div class="tab" onclick="showTab('views-tab')">视图管理</div>
                    <div class="tab" onclick="showTab('indexes-tab')">索引管理</div>
                    <div class="tab" onclick="showTab('stats-tab')">统计信息</div>
                </div>

                <!-- SQL 查询标签页 -->
                <div id="sql-tab" class="tab-content active">
                    <div class="form-group">
                        <label for="sql-input">SQL 语句 <kbd>Ctrl + Enter</kbd> 执行</label>
                        <textarea id="sql-input" class="sql-editor" placeholder="-- 请输入 SQL 语句&#10;SELECT * FROM your_table;"></textarea>
                    </div>

                    <div class="action-bar">
                        <button class="btn" onclick="executeSql()">
                            <span id="execute-loading" style="display: none;" class="loading"></span>
                            执行查询
                        </button>
                        <button class="btn btn-secondary" onclick="clearSql()">清空</button>
                        <button class="btn btn-secondary" onclick="formatSql()">格式化</button>
                    </div>

                    <div id="sql-result"></div>
                </div>

                <!-- 事务管理标签页 -->
                <div id="transaction-tab" class="tab-content">
                    <div class="transaction-panel">
                        <!-- 事务状态显示 -->
                        <div class="transaction-status">
                            <h3>事务状态</h3>
                            <div id="transaction-status-info" class="status-info">
                                <div class="status-item">
                                    <span class="status-label">会话ID:</span>
                                    <span id="session-id">-</span>
                                </div>
                                <div class="status-item">
                                    <span class="status-label">自动提交:</span>
                                    <span id="autocommit-status">-</span>
                                </div>
                                <div class="status-item">
                                    <span class="status-label">事务状态:</span>
                                    <span id="transaction-state">-</span>
                                </div>
                                <div class="status-item">
                                    <span class="status-label">隔离级别:</span>
                                    <span id="isolation-level">-</span>
                                </div>
                                <div class="status-item">
                                    <span class="status-label">当前用户:</span>
                                    <span id="current-user-txn">-</span>
                                </div>
                            </div>
                            <button class="btn btn-secondary" onclick="refreshTransactionStatus()">手动刷新</button>
                        </div>

                        <!-- 事务控制 -->
                        <div class="transaction-controls">
                            <h3>事务控制</h3>
                            <div class="control-group">
                                <button class="btn btn-success" onclick="beginTransaction()" id="begin-btn">开始事务</button>
                                <button class="btn btn-warning" onclick="commitTransaction()" id="commit-btn" disabled>提交事务</button>
                                <button class="btn btn-danger" onclick="rollbackTransaction()" id="rollback-btn" disabled>回滚事务</button>
                            </div>
                        </div>

                        <!-- 隔离级别设置 -->
                        <div class="isolation-settings">
                            <h3>隔离级别设置</h3>
                            <div class="form-group">
                                <label for="isolation-select">选择隔离级别:</label>
                                <select id="isolation-select" onchange="setIsolationLevel()">
                                    <option value="READ UNCOMMITTED">READ UNCOMMITTED</option>
                                    <option value="READ COMMITTED" selected>READ COMMITTED</option>
                                    <option value="REPEATABLE READ">REPEATABLE READ</option>
                                    <option value="SERIALIZABLE">SERIALIZABLE</option>
                                </select>
                            </div>
                        </div>

                        <!-- 自动提交设置 -->
                        <div class="autocommit-settings">
                            <h3>自动提交设置</h3>
                            <div class="form-group">
                                <label>
                                    <input type="checkbox" id="autocommit-checkbox" onchange="setAutocommit()" checked>
                                    启用自动提交
                                </label>
                            </div>
                        </div>

                        <!-- 会话管理 -->
                        <div class="session-management">
                            <h3>会话管理</h3>
                            <div class="action-bar">
                                <button class="btn" onclick="loadSessions()">刷新会话列表</button>
                            </div>
                            <div id="sessions-list"></div>
                        </div>
                    </div>
                </div>

                <!-- 表管理标签页 -->
                <div id="tables-tab" class="tab-content">
                    <div class="action-bar">
                        <button class="btn" onclick="loadTables()">刷新表列表</button>
                        <div class="search-box">
                            <input type="text" id="table-search" placeholder="搜索表名..." 
                                   onkeyup="filterTables()" style="margin-bottom: 0;">
                        </div>
                    </div>
                    <div id="tables-result"></div>
                </div>

                <!-- 视图管理标签页 -->
                <div id="views-tab" class="tab-content">
                    <div class="action-bar">
                        <button class="btn" onclick="loadViews()">刷新视图列表</button>
                        <div class="search-box">
                            <input type="text" id="view-search" placeholder="搜索视图名..." 
                                   onkeyup="filterViews()" style="margin-bottom: 0;">
                        </div>
                    </div>
                    <div id="views-result"></div>
                </div>

                <!-- 索引管理标签页 -->
                <div id="indexes-tab" class="tab-content">
                    <div class="action-bar">
                        <button class="btn" onclick="loadIndexes()">刷新索引列表</button>
                    </div>
                    <div id="indexes-result"></div>
                </div>

                <!-- 统计信息标签页 -->
                <div id="stats-tab" class="tab-content">
                    <div class="action-bar">
                        <button class="btn" onclick="loadStats()">刷新统计信息</button>
                    </div>
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
                    if (!element) {
                        console.warn('showMessage: 目标元素为null');
                        return;
                    }
                    
                    try {
                        const alertClass = isError ? 'alert-error' : 'alert-success';
                        element.innerHTML = `<div class="alert ${alertClass}">${message}</div>`;
                        setTimeout(() => {
                            if (element && element.querySelector('.alert')) {
                                element.querySelector('.alert').remove();
                            }
                        }, 5000);
                    } catch (e) {
                        console.warn('showMessage 执行失败:', e);
                    }
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
                    console.log('切换到标签页:', tabId);
                    
                    // 隐藏所有标签页内容
                    const contents = document.querySelectorAll('.tab-content');
                    contents.forEach(content => content.classList.remove('active'));

                    // 移除所有标签激活状态
                    const tabs = document.querySelectorAll('.tab');
                    tabs.forEach(tab => tab.classList.remove('active'));

                    // 显示选中的标签页
                    const targetContent = document.getElementById(tabId);
                    if (targetContent) {
                        targetContent.classList.add('active');
                        console.log('显示内容区域:', tabId);
                    } else {
                        console.error('未找到内容区域:', tabId);
                    }

                    // 激活对应的标签
                    const tabText = getTabText(tabId);
                    console.log('查找标签文本:', tabText);
                    const activeTab = Array.from(tabs).find(tab => 
                        tab.textContent.includes(tabText)
                    );
                    if (activeTab) {
                        activeTab.classList.add('active');
                        console.log('激活标签:', activeTab.textContent);
                    } else {
                        console.error('未找到对应的标签:', tabText);
                    }

                    // 关闭侧边栏
                    const sidebar = document.getElementById('sidebar');
                    if (sidebar) {
                        sidebar.classList.remove('open');
                    }
                }

                function getTabText(tabId) {
                    const map = {
                        'sql-tab': 'SQL 查询',
                        'transaction-tab': '事务管理',
                        'tables-tab': '表管理',
                        'views-tab': '视图管理',
                        'indexes-tab': '索引管理',
                        'stats-tab': '统计信息'
                    };
                    return map[tabId] || '';
                }

                // 新增：辅助函数，用于渲染结果表格，避免代码重复
                function renderTable(formattedData) {
                    let html = '<div class="result-table"><table>';
                    html += '<thead><tr>';
                    formattedData.columns.forEach(col => {
                        html += `<th>${col}</th>`;
                    });
                    html += '</tr></thead>';
                    html += '<tbody>';
                    formattedData.rows.forEach(row => {
                        html += '<tr>';
                        row.forEach(cell => {
                            html += `<td>${cell}</td>`;
                        });
                        html += '</tr>';
                    });
                    html += '</tbody></table></div>';
                    html += `<p style="margin-top: 8px; font-size: 13px; color: var(--neutral-500);">共 ${formattedData.total} 行记录</p>`;
                    return html;
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
                        let finalHtml = '';

                        // 新增：处理多语句的返回结果
                        if (result.results && Array.isArray(result.results)) {
                            finalHtml += `<div class="alert alert-info">${result.message}</div>`;
                            result.results.forEach((res, index) => {
                                finalHtml += `<div style="border-top: 1px solid var(--neutral-200); margin-top: 1rem; padding-top: 1rem;">`;
                                finalHtml += `<h4>语句 #${index + 1}</h4>`;
                                if (res.success) {
                                    let html = `<div class="alert alert-success">执行成功`;
                                    if (res.message) html += `: ${res.message}`;
                                    html += `</div>`;
                                    if (res.formatted && res.formatted.columns) {
                                       html += renderTable(res.formatted);
                                    }
                                    finalHtml += html;
                                } else {
                                    finalHtml += `<div class="alert alert-error">${res.message || '执行失败'}</div>`;
                                }
                                finalHtml += `</div>`;
                            });
                        } 
                        // 兼容处理单语句的返回结果
                        else if (result.success) {
                            let html = `<div class="alert alert-success">执行成功`;
                            if (result.message) html += `: ${result.message}`;
                            html += `</div>`;

                            // 检查是否是Shell命令
                            if (result.type === 'SHELL_COMMAND') {
                                // 显示Shell命令结果
                                if (result.data && result.data.length > 0) {
                                    if (result.data[0].help) {
                                        // 帮助命令
                                        html += `<div class="shell-help"><pre>${result.data[0].help}</pre></div>`;
                                    } else if (Array.isArray(result.data[0])) {
                                        // 数组数据（如会话列表、表列表等）
                                        html += '<div class="result-table"><table>';
                                        html += '<thead><tr>';
                                        Object.keys(result.data[0]).forEach(key => {
                                            html += `<th>${key}</th>`;
                                        });
                                        html += '</tr></thead><tbody>';
                                        result.data.forEach(item => {
                                            html += '<tr>';
                                            Object.values(item).forEach(value => {
                                                html += `<td>${value}</td>`;
                                            });
                                            html += '</tr>';
                                        });
                                        html += '</tbody></table></div>';
                                    } else {
                                        // 对象数据
                                        html += '<div class="result-table"><table>';
                                        html += '<tbody>';
                                        Object.entries(result.data[0]).forEach(([key, value]) => {
                                            html += `<tr><td><strong>${key}:</strong></td><td>${value}</td></tr>`;
                                        });
                                        html += '</tbody></table></div>';
                                    }
                                }
                            }
                            // 如果是SELECT查询，显示表格
                            else if (result.formatted && result.formatted.columns) {
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
                            finalHtml = html;
                        } else {
                            // 处理单语句执行失败的情况
                            showMessage(resultEl, result.message || '执行失败', true);
                            return; 
                        }
                        
                        resultEl.innerHTML = finalHtml;
                        
                        // 检查是否执行了事务相关的SQL，如果是则刷新事务状态
                        const transactionKeywords = ['BEGIN', 'START TRANSACTION', 'COMMIT', 'ROLLBACK', 'SET AUTOCOMMIT', 'SET SESSION TRANSACTION'];
                        const upperSql = sql.toUpperCase();
                        const hasTransactionCommand = transactionKeywords.some(keyword => upperSql.includes(keyword));
                        
                        if (hasTransactionCommand) {
                            console.log('检测到事务相关命令，刷新事务状态');
                            setTimeout(() => {
                                refreshTransactionStatus();
                            }, 500);
                        } else {
                            // 对于其他SQL命令，也刷新事务状态以确保一致性
                            setTimeout(() => {
                                refreshTransactionStatus();
                            }, 200);
                        }
                        
                    } catch (error) {
                        showMessage(resultEl, '请求失败: ' + error.message, true);
                    }finally {
                        document.getElementById('execute-loading').style.display = 'none';
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

                            // 在loadTables()函数中修改表格生成部分
                            if (result.data && result.data.length > 0) {
                                html += '<div class="result-table"><table>';
                                html += '<thead><tr><th>表名</th><th>操作</th></tr></thead><tbody>';
                            
                                result.data.forEach(table => {
                                    html += `<tr>
                                        <td>${table}</td>
                                        <td>
                                            <button class="btn" onclick="showTableInfo('${table}')" style="margin-right: 5px;">查看详情</button>
                                            <button class="btn btn-secondary" onclick="previewTableData('${table}')">快速预览</button>
                                        </td>
                                    </tr>`;
                                });
                            
                                html += '</tbody></table></div>';
                            }else {
                                html += '<p>暂无表</p>';
                            }

                            resultEl.innerHTML = html;
                            // 保持搜索框已有关键字的过滤效果
                            filterTables();
                        } else {
                            showMessage(resultEl, result.message, true);
                        }
                    } catch (error) {
                        showMessage(resultEl, '获取表列表失败: ' + error.message, true);
                    }
                }
                
                // 修复后的快速预览函数
                async function previewTableData(tableName) {
                    const resultEl = document.getElementById('tables-result');
                    
                    try {
                        // 显示加载状态
                        const loadingHtml = `<div class="alert alert-info">正在加载 ${tableName} 的数据预览...</div>`;
                        resultEl.innerHTML = resultEl.innerHTML + loadingHtml;
                        
                        const response = await fetch(`/api/tables/${tableName}/data?page=1&page_size=10`, {
                            method: 'GET',
                            credentials: 'include'
                        });
                
                        const result = await response.json();
                
                        if (result.success) {
                            let html = `<div class="alert alert-success">表 ${tableName} 数据预览（前10行）</div>`;
                            
                            if (result.data.rows.length > 0) {
                                html += '<div class="result-table"><table>';
                                html += '<thead><tr>';
                                result.data.columns.forEach(col => {
                                    html += `<th>${col}</th>`;
                                });
                                html += '</tr></thead><tbody>';
                                
                                result.data.rows.forEach(row => {
                                    html += '<tr>';
                                    row.forEach(cell => {
                                        html += `<td style="max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${cell}">${cell}</td>`;
                                    });
                                    html += '</tr>';
                                });
                                html += '</tbody></table></div>';
                                
                                if (result.data.total > 10) {
                                    html += `<p><small>显示了前10行，共${result.data.total}行记录。<button class="btn" onclick="showTableInfo('${tableName}')">查看完整数据</button></small></p>`;
                                }
                            } else {
                                html += '<p>该表暂无数据</p>';
                            }
                            
                            // 在当前表格后面添加预览，而不是替换
                            const currentContent = resultEl.innerHTML.replace(/<div class="alert alert-info">.*?<\/div>/, '');
                            resultEl.innerHTML = currentContent + html;
                        } else {
                            showMessage(resultEl, '获取数据失败: ' + result.message, true);
                        }
                    } catch (error) {
                        showMessage(resultEl, '获取数据失败: ' + error.message, true);
                    }
                }


                // 显示表信息 - 改进版，类似Navicat
                async function showTableInfo(tableName) {
                    try {
                        // 获取表结构
                        const structResponse = await fetch(`/api/tables/${tableName}`, {
                            method: 'GET',
                            credentials: 'include'
                        });
                        const structResult = await structResponse.json();
                
                        // 获取表数据
                        const dataResponse = await fetch(`/api/tables/${tableName}/data?page=1&page_size=50`, {
                            method: 'GET',
                            credentials: 'include'
                        });
                        const dataResult = await dataResponse.json();
                
                        if (structResult.success && dataResult.success) {
                            showTableDetailDialog(tableName, structResult.data, dataResult.data);
                        } else {
                            alert('获取表信息失败: ' + (structResult.message || dataResult.message));
                        }
                    } catch (error) {
                        alert('获取表信息失败: ' + error.message);
                    }
                }
                
                // 显示表详情对话框
                function showTableDetailDialog(tableName, structInfo, dataInfo) {
                    // 创建模态对话框
                    const modal = document.createElement('div');
                    modal.style.cssText = `
                        position: fixed; top: 0; left: 0; right: 0; bottom: 0;
                        background: rgba(0,0,0,0.5); z-index: 2000;
                        display: flex; align-items: center; justify-content: center;
                    `;
                
                    const dialog = document.createElement('div');
                    dialog.style.cssText = `
                        background: white; border-radius: 12px; 
                        width: 90%; max-width: 1000px; height: 80%; 
                        box-shadow: 0 10px 30px rgba(0,0,0,0.3);
                        display: flex; flex-direction: column;
                    `;
                
                    // 构建表结构信息HTML
                    let structHtml = `
                        <div style="padding: 20px; border-bottom: 1px solid #e1e5e9;">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <h2>表: ${tableName}</h2>
                                <button onclick="this.closest('.modal').remove()" 
                                        style="background: #dc3545; color: white; border: none; 
                                               padding: 8px 16px; border-radius: 6px; cursor: pointer;">
                                    关闭
                                </button>
                            </div>
                            <div style="margin-top: 10px; color: #666; font-size: 14px;">
                                记录数: ${structInfo.record_count} | 页面数: ${structInfo.pages ? structInfo.pages.length : 0} | 
                                索引数: ${structInfo.indexes ? structInfo.indexes.length : 0}
                            </div>
                        </div>
                    `;
                
                    // 标签页导航
                    structHtml += `
                        <div style="padding: 0 20px;">
                            <div class="table-detail-tabs" style="display: flex; border-bottom: 2px solid #e1e5e9;">
                                <div class="table-detail-tab active" onclick="showTableDetailTab(event, 'structure')" 
                                     style="padding: 12px 20px; cursor: pointer; border-bottom: 2px solid #667eea;">
                                    表结构
                                </div>
                                <div class="table-detail-tab" onclick="showTableDetailTab(event, 'data')" 
                                     style="padding: 12px 20px; cursor: pointer; border-bottom: 2px solid transparent;">
                                    数据内容 (${dataInfo.total}行)
                                </div>
                                <div class="table-detail-tab" onclick="showTableDetailTab(event, 'indexes')" 
                                     style="padding: 12px 20px; cursor: pointer; border-bottom: 2px solid transparent;">
                                    索引 (${structInfo.indexes.length}个)
                                </div>
                            </div>
                        </div>
                    `;
                
                    // 表结构标签页内容
                    structHtml += `
                        <div id="structure-content" class="table-detail-content" style="flex: 1; overflow-y: auto; padding: 20px;">
                            <table style="width: 100%; border-collapse: collapse;">
                                <thead>
                                    <tr style="background: #f8f9fa;">
                                        <th style="padding: 12px; text-align: left; border: 1px solid #dee2e6; font-weight: 600;">列名</th>
                                        <th style="padding: 12px; text-align: left; border: 1px solid #dee2e6; font-weight: 600;">数据类型</th>
                                        <th style="padding: 12px; text-align: left; border: 1px solid #dee2e6; font-weight: 600;">长度</th>
                                        <th style="padding: 12px; text-align: left; border: 1px solid #dee2e6; font-weight: 600;">可空</th>
                                        <th style="padding: 12px; text-align: left; border: 1px solid #dee2e6; font-weight: 600;">主键</th>
                                    </tr>
                                </thead>
                                <tbody>`;
                
                    structInfo.columns.forEach(col => {
                        structHtml += `
                            <tr>
                                <td style="padding: 12px; border: 1px solid #dee2e6;">
                                    <strong>${col.name}</strong>
                                    ${col.primary_key ? '<span style="color: #ffc107; margin-left: 5px;">🔑</span>' : ''}
                                </td>
                                <td style="padding: 12px; border: 1px solid #dee2e6;">${col.type}</td>
                                <td style="padding: 12px; border: 1px solid #dee2e6;">${col.max_length || '-'}</td>
                                <td style="padding: 12px; border: 1px solid #dee2e6;">
                                    ${col.nullable ? '<span style="color: #28a745;">是</span>' : '<span style="color: #dc3545;">否</span>'}
                                </td>
                                <td style="padding: 12px; border: 1px solid #dee2e6;">
                                    ${col.primary_key ? '<span style="color: #ffc107;">是</span>' : '否'}
                                </td>
                            </tr>`;
                    });
                
                    structHtml += `
                                </tbody>
                            </table>
                        </div>
                    `;
                
                    // 数据内容标签页
                    let dataHtml = `
                        <div id="data-content" class="table-detail-content" style="flex: 1; overflow-y: auto; padding: 20px; display: none;">
                            <div style="margin-bottom: 15px;">
                                <span style="color: #666;">共 ${dataInfo.total} 行记录</span>
                                ${dataInfo.total_pages > 1 ? `
                                <span style="margin-left: 20px;">
                                    第 ${dataInfo.page} 页，共 ${dataInfo.total_pages} 页
                                </span>` : ''}
                            </div>
                            <div style="overflow-x: auto;">
                                <table style="width: 100%; border-collapse: collapse; min-width: 600px;">
                                    <thead>
                                        <tr style="background: #f8f9fa;">`;
                
                    dataInfo.columns.forEach(col => {
                        dataHtml += `<th style="padding: 12px; text-align: left; border: 1px solid #dee2e6; font-weight: 600; white-space: nowrap;">${col}</th>`;
                    });
                
                    dataHtml += `
                                        </tr>
                                    </thead>
                                    <tbody>`;
                
                    if (dataInfo.rows.length === 0) {
                        dataHtml += `<tr><td colspan="${dataInfo.columns.length}" style="padding: 20px; text-align: center; color: #666;">暂无数据</td></tr>`;
                    } else {
                        dataInfo.rows.forEach(row => {
                            dataHtml += '<tr>';
                            row.forEach(cell => {
                                dataHtml += `<td style="padding: 12px; border: 1px solid #dee2e6; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${cell}">${cell}</td>`;
                            });
                            dataHtml += '</tr>';
                        });
                    }
                
                    dataHtml += `
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    `;
                
                    // 索引标签页
                    let indexHtml = `
                        <div id="indexes-content" class="table-detail-content" style="flex: 1; overflow-y: auto; padding: 20px; display: none;">`;
                
                    if (structInfo.indexes.length === 0) {
                        indexHtml += '<p style="color: #666; text-align: center; padding: 20px;">该表暂无索引</p>';
                    } else {
                        indexHtml += `
                            <table style="width: 100%; border-collapse: collapse;">
                                <thead>
                                    <tr style="background: #f8f9fa;">
                                        <th style="padding: 12px; text-align: left; border: 1px solid #dee2e6; font-weight: 600;">索引名</th>
                                        <th style="padding: 12px; text-align: left; border: 1px solid #dee2e6; font-weight: 600;">列名</th>
                                        <th style="padding: 12px; text-align: left; border: 1px solid #dee2e6; font-weight: 600;">类型</th>
                                    </tr>
                                </thead>
                                <tbody>`;
                
                        structInfo.indexes.forEach(index => {
                            indexHtml += `
                                <tr>
                                    <td style="padding: 12px; border: 1px solid #dee2e6;">${index.name}</td>
                                    <td style="padding: 12px; border: 1px solid #dee2e6;">${index.column}</td>
                                    <td style="padding: 12px; border: 1px solid #dee2e6;">
                                        ${index.unique ? '<span style="color: #ffc107;">唯一索引</span>' : '普通索引'}
                                    </td>
                                </tr>`;
                        });
                
                        indexHtml += `
                                </tbody>
                            </table>`;
                    }
                
                    indexHtml += '</div>';
                
                    dialog.innerHTML = structHtml + dataHtml + indexHtml;
                    modal.appendChild(dialog);
                    modal.className = 'modal'; // 为关闭按钮提供选择器
                    document.body.appendChild(modal);
                
                    // 点击背景关闭
                    modal.addEventListener('click', (e) => {
                        if (e.target === modal) {
                            modal.remove();
                        }
                    });
                }
                
                // 标签页切换
                function showTableDetailTab(event, tabName) {
                    // 移除所有active状态
                    document.querySelectorAll('.table-detail-tab').forEach(tab => {
                        tab.classList.remove('active');
                        tab.style.borderBottomColor = 'transparent';
                    });
                    
                    document.querySelectorAll('.table-detail-content').forEach(content => {
                        content.style.display = 'none';
                    });
                
                    // 激活当前标签
                    event.target.classList.add('active');
                    event.target.style.borderBottomColor = '#667eea';
                    document.getElementById(tabName + '-content').style.display = 'block';
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
                            filterViews();
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

                // ==================== 事务管理功能 ====================

                // 初始化事务按钮状态
                function initializeTransactionButtons() {
                    console.log('初始化事务按钮状态 - 所有按钮启用');
                    const beginBtn = document.getElementById('begin-btn');
                    const commitBtn = document.getElementById('commit-btn');
                    const rollbackBtn = document.getElementById('rollback-btn');
                    
                    if (beginBtn && commitBtn && rollbackBtn) {
                        // 所有按钮都启用，在执行时再检查状态
                        beginBtn.disabled = false;
                        commitBtn.disabled = false;
                        rollbackBtn.disabled = false;
                        console.log('所有按钮已启用');
                    } else {
                        console.error('无法找到事务按钮元素');
                    }
                }


                // 刷新事务状态
                async function refreshTransactionStatus() {
                    try {
                        console.log('正在刷新事务状态...');
                        const response = await fetch('/api/transaction/status', {
                            method: 'GET',
                            credentials: 'include'
                        });

                        if (!response.ok) {
                            console.error('获取事务状态失败:', response.status, response.statusText);
                            return;
                        }

                        const result = await response.json();
                        console.log('事务状态响应:', result);

                        if (result.success) {
                            const data = result.data;
                            console.log('事务状态数据:', data);
                            
                            document.getElementById('session-id').textContent = data.session_id || '-';
                            document.getElementById('autocommit-status').textContent = data.autocommit ? '是' : '否';
                            document.getElementById('transaction-state').textContent = data.in_transaction ? '进行中' : '未开始';
                            document.getElementById('isolation-level').textContent = data.isolation_level || '-';
                            document.getElementById('current-user-txn').textContent = data.current_user || '-';

                            // 更新自动提交复选框
                            document.getElementById('autocommit-checkbox').checked = data.autocommit;

                            // 更新隔离级别选择
                            const isolationSelect = document.getElementById('isolation-select');
                            isolationSelect.value = data.isolation_level || 'READ COMMITTED';
                        } else {
                            console.error('获取事务状态失败:', result.message);
                        }
                    } catch (error) {
                        console.error('获取事务状态失败:', error);
                    }
                }

                // 开始事务
                async function beginTransaction() {
                    console.log('开始事务按钮被点击');
                    try {
                        // 先检查当前事务状态
                        const statusResponse = await fetch('/api/transaction/status', {
                            method: 'GET',
                            credentials: 'include'
                        });
                        const statusResult = await statusResponse.json();
                        
                        if (statusResult.success && statusResult.data.in_transaction) {
                            showMessage(document.getElementById('transaction-status-info'), '错误：当前已有活动事务，请先提交或回滚', true);
                            return;
                        }

                        const response = await fetch('/api/transaction/begin', {
                            method: 'POST',
                            credentials: 'include'
                        });

                        const result = await response.json();
                        console.log('开始事务响应:', result);

                        if (result.success) {
                            showMessage(document.getElementById('transaction-status-info'), '事务已开始', false);
                            // 刷新事务状态
                            setTimeout(() => {
                                console.log('强制刷新事务状态');
                                refreshTransactionStatus();
                            }, 200);
                        } else {
                            showMessage(document.getElementById('transaction-status-info'), '开始事务失败: ' + result.message, true);
                        }
                    } catch (error) {
                        showMessage(document.getElementById('transaction-status-info'), '开始事务失败: ' + error.message, true);
                    }
                }

                // 提交事务
                async function commitTransaction() {
                    console.log('提交事务按钮被点击');
                    try {
                        // 先检查当前事务状态
                        const statusResponse = await fetch('/api/transaction/status', {
                            method: 'GET',
                            credentials: 'include'
                        });
                        const statusResult = await statusResponse.json();
                        
                        if (statusResult.success && !statusResult.data.in_transaction) {
                            showMessage(document.getElementById('transaction-status-info'), '错误：当前没有活动事务，无法提交', true);
                            return;
                        }

                        const response = await fetch('/api/transaction/commit', {
                            method: 'POST',
                            credentials: 'include'
                        });

                        const result = await response.json();

                        if (result.success) {
                            showMessage(document.getElementById('transaction-status-info'), '事务已提交', false);
                            // 刷新状态
                            setTimeout(() => {
                                refreshTransactionStatus();
                            }, 100);
                        } else {
                            showMessage(document.getElementById('transaction-status-info'), '提交事务失败: ' + result.message, true);
                        }
                    } catch (error) {
                        showMessage(document.getElementById('transaction-status-info'), '提交事务失败: ' + error.message, true);
                    }
                }

                // 回滚事务
                async function rollbackTransaction() {
                    console.log('回滚事务按钮被点击');
                    try {
                        // 先检查当前事务状态
                        const statusResponse = await fetch('/api/transaction/status', {
                            method: 'GET',
                            credentials: 'include'
                        });
                        const statusResult = await statusResponse.json();
                        
                        if (statusResult.success && !statusResult.data.in_transaction) {
                            showMessage(document.getElementById('transaction-status-info'), '错误：当前没有活动事务，无法回滚', true);
                            return;
                        }

                        const response = await fetch('/api/transaction/rollback', {
                            method: 'POST',
                            credentials: 'include'
                        });

                        const result = await response.json();

                        if (result.success) {
                            showMessage(document.getElementById('transaction-status-info'), '事务已回滚', false);
                            // 刷新状态
                            setTimeout(() => {
                                refreshTransactionStatus();
                            }, 100);
                        } else {
                            showMessage(document.getElementById('transaction-status-info'), '回滚事务失败: ' + result.message, true);
                        }
                    } catch (error) {
                        showMessage(document.getElementById('transaction-status-info'), '回滚事务失败: ' + error.message, true);
                    }
                }

                // 设置隔离级别
                async function setIsolationLevel() {
                    const level = document.getElementById('isolation-select').value;
                    
                    try {
                        const response = await fetch('/api/transaction/isolation', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json'
                            },
                            body: JSON.stringify({ level: level }),
                            credentials: 'include'
                        });

                        const result = await response.json();

                        if (result.success) {
                            showMessage(document.getElementById('transaction-status-info'), `隔离级别已设置为: ${level}`, false);
                            refreshTransactionStatus();
                        } else {
                            showMessage(document.getElementById('transaction-status-info'), '设置隔离级别失败: ' + result.message, true);
                        }
                    } catch (error) {
                        showMessage(document.getElementById('transaction-status-info'), '设置隔离级别失败: ' + error.message, true);
                    }
                }

                // 设置自动提交
                async function setAutocommit() {
                    const enabled = document.getElementById('autocommit-checkbox').checked;
                    
                    try {
                        const response = await fetch('/api/transaction/autocommit', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json'
                            },
                            body: JSON.stringify({ enabled: enabled }),
                            credentials: 'include'
                        });

                        const result = await response.json();

                        if (result.success) {
                            showMessage(document.getElementById('transaction-status-info'), `自动提交已${enabled ? '启用' : '禁用'}`, false);
                            refreshTransactionStatus();
                        } else {
                            showMessage(document.getElementById('transaction-status-info'), '设置自动提交失败: ' + result.message, true);
                        }
                    } catch (error) {
                        showMessage(document.getElementById('transaction-status-info'), '设置自动提交失败: ' + error.message, true);
                    }
                }

                // 加载会话列表
                async function loadSessions() {
                    const resultEl = document.getElementById('sessions-list');

                    try {
                        const response = await fetch('/api/sessions', {
                            method: 'GET',
                            credentials: 'include'
                        });

                        const result = await response.json();

                        if (result.success) {
                            let html = '<div class="alert alert-success">会话列表加载成功</div>';
                            
                            if (result.data && result.data.length > 0) {
                                html += '<table class="sessions-table">';
                                html += '<thead><tr><th>会话ID</th><th>自动提交</th><th>事务状态</th><th>隔离级别</th><th>当前会话</th></tr></thead><tbody>';
                                
                                result.data.forEach(session => {
                                    html += '<tr>';
                                    html += `<td>${session.session_id}</td>`;
                                    html += `<td>${session.autocommit ? '是' : '否'}</td>`;
                                    html += `<td>${session.in_txn ? '进行中' : '未开始'}</td>`;
                                    html += `<td>${session.isolation}</td>`;
                                    html += `<td>${session.current ? '是' : '否'}</td>`;
                                    html += '</tr>';
                                });
                                
                                html += '</tbody></table>';
                            } else {
                                html += '<p>暂无会话信息</p>';
                            }
                            
                            resultEl.innerHTML = html;
                        } else {
                            showMessage(resultEl, '获取会话列表失败: ' + result.message, true);
                        }
                    } catch (error) {
                        showMessage(resultEl, '获取会话列表失败: ' + error.message, true);
                    }
                }
                
                // 加载视图列表
                async function loadViews() {
                    const resultEl = document.getElementById('views-result');
                
                    try {
                        const response = await fetch('/api/views', {
                            method: 'GET',
                            credentials: 'include'
                        });
                
                        const result = await response.json();
                
                        if (result.success) {
                            let html = `<div class="alert alert-success">${result.message}</div>`;
                
                            if (result.data && result.data.length > 0) {
                                html += '<div class="result-table"><table>';
                                html += '<thead><tr><th>视图名</th><th>操作</th></tr></thead><tbody>';
                
                                result.data.forEach(view => {
                                    html += `<tr>
                                        <td>${view}</td>
                                        <td>
                                            <button class="btn" onclick="showViewInfo('${view}')" style="margin-right: 5px;">查看详情</button>
                                            <button class="btn btn-secondary" onclick="previewViewData('${view}')">快速预览</button>
                                        </td>
                                    </tr>`;
                                });
                
                                html += '</tbody></table></div>';
                            } else {
                                html += '<p>暂无视图</p>';
                            }
                
                            resultEl.innerHTML = html;
                        } else {
                            showMessage(resultEl, result.message, true);
                        }
                    } catch (error) {
                        showMessage(resultEl, '获取视图列表失败: ' + error.message, true);
                    }
                }
                
                // 显示视图详情
                async function showViewInfo(viewName) {
                    try {
                        // 获取视图信息
                        const infoResponse = await fetch(`/api/views/${viewName}`, {
                            method: 'GET',
                            credentials: 'include'
                        });
                        const infoResult = await infoResponse.json();
                
                        // 获取视图数据
                        const dataResponse = await fetch(`/api/views/${viewName}/data?page=1&page_size=50`, {
                            method: 'GET',
                            credentials: 'include'
                        });
                        const dataResult = await dataResponse.json();
                
                        if (infoResult.success && dataResult.success) {
                            showViewDetailDialog(viewName, infoResult.data, dataResult.data);
                        } else {
                            alert('获取视图信息失败: ' + (infoResult.message || dataResult.message));
                        }
                    } catch (error) {
                        alert('获取视图信息失败: ' + error.message);
                    }
                }
                
                // 显示视图详情对话框
                function showViewDetailDialog(viewName, viewInfo, dataInfo) {
                    const modal = document.createElement('div');
                    modal.style.cssText = `
                        position: fixed; top: 0; left: 0; right: 0; bottom: 0;
                        background: rgba(0,0,0,0.5); z-index: 2000;
                        display: flex; align-items: center; justify-content: center;
                    `;
                
                    const dialog = document.createElement('div');
                    dialog.style.cssText = `
                        background: white; border-radius: 12px; 
                        width: 90%; max-width: 1000px; height: 80%; 
                        box-shadow: 0 10px 30px rgba(0,0,0,0.3);
                        display: flex; flex-direction: column;
                    `;
                
                    let html = `
                        <div style="padding: 20px; border-bottom: 1px solid #e1e5e9;">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <h2>视图: ${viewName}</h2>
                                <button onclick="this.closest('.modal').remove()" 
                                        style="background: #dc3545; color: white; border: none; 
                                               padding: 8px 16px; border-radius: 6px; cursor: pointer;">
                                    关闭
                                </button>
                            </div>
                        </div>
                
                        <div style="padding: 0 20px;">
                            <div class="view-detail-tabs" style="display: flex; border-bottom: 2px solid #e1e5e9;">
                                <div class="view-detail-tab active" onclick="showViewDetailTab(event, 'definition')" 
                                     style="padding: 12px 20px; cursor: pointer; border-bottom: 2px solid #667eea;">
                                    视图定义
                                </div>
                                <div class="view-detail-tab" onclick="showViewDetailTab(event, 'data')" 
                                     style="padding: 12px 20px; cursor: pointer; border-bottom: 2px solid transparent;">
                                    数据内容 (${dataInfo.total}行)
                                </div>
                            </div>
                        </div>
                
                        <!-- 视图定义标签页 -->
                        <div id="definition-content" class="view-detail-content" style="flex: 1; overflow-y: auto; padding: 20px;">
                            <h4>SQL 定义：</h4>
                            <div style="background: #f8f9fa; padding: 15px; border-radius: 6px; border: 1px solid #e9ecef; margin-top: 10px;">
                                <pre style="margin: 0; font-family: 'Courier New', monospace; font-size: 14px; white-space: pre-wrap;">${viewInfo.definition}</pre>
                            </div>
                        </div>
                
                        <!-- 数据内容标签页 -->
                        <div id="data-content" class="view-detail-content" style="flex: 1; overflow-y: auto; padding: 20px; display: none;">
                            <div style="margin-bottom: 15px;">
                                <span style="color: #666;">共 ${dataInfo.total} 行记录</span>
                                ${dataInfo.total_pages > 1 ? `
                                <span style="margin-left: 20px;">
                                    第 ${dataInfo.page} 页，共 ${dataInfo.total_pages} 页
                                </span>` : ''}
                            </div>`;
                
                    if (dataInfo.rows && dataInfo.rows.length > 0) {
                        html += `
                            <div style="overflow-x: auto;">
                                <table style="width: 100%; border-collapse: collapse; min-width: 600px;">
                                    <thead>
                                        <tr style="background: #f8f9fa;">`;
                        
                        dataInfo.columns.forEach(col => {
                            html += `<th style="padding: 12px; text-align: left; border: 1px solid #dee2e6; font-weight: 600; white-space: nowrap;">${col}</th>`;
                        });
                
                        html += `</tr></thead><tbody>`;
                
                        dataInfo.rows.forEach(row => {
                            html += '<tr>';
                            row.forEach(cell => {
                                html += `<td style="padding: 12px; border: 1px solid #dee2e6; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${cell}">${cell}</td>`;
                            });
                            html += '</tr>';
                        });
                
                        html += '</tbody></table></div>';
                    } else {
                        html += '<p style="color: #666; text-align: center; padding: 20px;">暂无数据</p>';
                    }
                
                    html += '</div>';
                
                    dialog.innerHTML = html;
                    modal.appendChild(dialog);
                    modal.className = 'modal';
                    document.body.appendChild(modal);
                
                    modal.addEventListener('click', (e) => {
                        if (e.target === modal) {
                            modal.remove();
                        }
                    });
                }
                
                // 视图详情标签页切换
                function showViewDetailTab(event, tabName) {
                    document.querySelectorAll('.view-detail-tab').forEach(tab => {
                        tab.classList.remove('active');
                        tab.style.borderBottomColor = 'transparent';
                    });
                    
                    document.querySelectorAll('.view-detail-content').forEach(content => {
                        content.style.display = 'none';
                    });
                
                    event.target.classList.add('active');
                    event.target.style.borderBottomColor = '#667eea';
                    document.getElementById(tabName + '-content').style.display = 'block';
                }
                
                function normalizeText(str) {
    return (str || '').toString().trim().toLowerCase();
}

                function filterTables() {
                    const keyword = normalizeText(document.getElementById('table-search')?.value);
                    const tbody = document.querySelector('#tables-result tbody');
                    if (!tbody) return;
                    const rows = tbody.querySelectorAll('tr');
                    rows.forEach(row => {
                        const nameCell = row.querySelector('td:first-child');
                        const name = normalizeText(nameCell?.textContent);
                        const match = !keyword || name.includes(keyword);
                        row.style.display = match ? '' : 'none';
                    });
                }
                
                function filterViews() {
                    const keyword = normalizeText(document.getElementById('view-search')?.value);
                    const tbody = document.querySelector('#views-result tbody');
                    if (!tbody) return;
                    const rows = tbody.querySelectorAll('tr');
                    rows.forEach(row => {
                        const nameCell = row.querySelector('td:first-child');
                        const name = normalizeText(nameCell?.textContent);
                        const match = !keyword || name.includes(keyword);
                        row.style.display = match ? '' : 'none';
                    });
                }

                
                // 快速预览视图数据
                async function previewViewData(viewName) {
                    const resultEl = document.getElementById('views-result');
                    
                    try {
                        const loadingHtml = `<div class="alert alert-info">正在加载视图 ${viewName} 的数据预览...</div>`;
                        resultEl.innerHTML = resultEl.innerHTML + loadingHtml;
                        
                        const response = await fetch(`/api/views/${viewName}/data?page=1&page_size=10`, {
                            method: 'GET',
                            credentials: 'include'
                        });
                
                        const result = await response.json();
                
                        if (result.success) {
                            let html = `<div class="alert alert-success">视图 ${viewName} 数据预览（前10行）</div>`;
                            
                            if (result.data.rows.length > 0) {
                                html += '<div class="result-table"><table>';
                                html += '<thead><tr>';
                                result.data.columns.forEach(col => {
                                    html += `<th>${col}</th>`;
                                });
                                html += '</tr></thead><tbody>';
                                
                                result.data.rows.forEach(row => {
                                    html += '<tr>';
                                    row.forEach(cell => {
                                        html += `<td style="max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${cell}">${cell}</td>`;
                                    });
                                    html += '</tr>';
                                });
                                html += '</tbody></table></div>';
                                
                                if (result.data.total > 10) {
                                    html += `<p><small>显示了前10行，共${result.data.total}行记录。<button class="btn" onclick="showViewInfo('${viewName}')">查看完整数据</button></small></p>`;
                                }
                            } else {
                                html += '<p>该视图暂无数据</p>';
                            }
                            
                            const currentContent = resultEl.innerHTML.replace(/<div class="alert alert-info">.*?<\/div>/, '');
                            resultEl.innerHTML = currentContent + html;
                        } else {
                            showMessage(resultEl, '获取数据失败: ' + result.message, true);
                        }
                    } catch (error) {
                        showMessage(resultEl, '获取数据失败: ' + error.message, true);
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

                    // 页面加载完成后刷新事务状态
                    setTimeout(() => {
                        if (document.getElementById('transaction-status-info')) {
                            refreshTransactionStatus();
                        }
                    }, 1000);
                    
                    // 初始化按钮状态
                    initializeTransactionButtons();
                    
                    // 添加调试信息
                    console.log('页面加载完成，准备刷新事务状态');
                });

                // ====== SQL 输入框自动补全（灰色联想） ======
                (function() {
                    // 创建建议下拉框
                    const suggestDiv = document.createElement('div');
                    suggestDiv.id = 'sql-suggest-list';
                    suggestDiv.style.cssText = 'position: absolute; z-index: 1000; background: white; border: 1px solid #e5e7eb; border-radius: 6px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: none; min-width: 200px; font-size: 14px; max-height: 220px; overflow-y: auto;';
                    document.body.appendChild(suggestDiv);

                    function positionSuggestDiv() {
                        const input = document.getElementById('sql-input');
                        const rect = input.getBoundingClientRect();
                        suggestDiv.style.left = rect.left + window.scrollX + 'px';
                        suggestDiv.style.top = (rect.bottom + window.scrollY) + 'px';
                        suggestDiv.style.width = rect.width + 'px';
                    }

                    let lastPrefix = '';
                    let lastVal = '';
                    let lastSuggestions = [];

                    document.getElementById('sql-input').addEventListener('input', async function(e) {
                        const val = e.target.value;
                        // 取光标前最后一个单词（允许空格/换行分隔）
                        const caret = e.target.selectionStart;
                        const before = val.slice(0, caret);
                        const match = before.match(/([\w.]+)$/);
                        const lastWord = match ? match[1] : '';
                        if (!lastWord) {
                            suggestDiv.style.display = 'none';
                            return;
                        }
                        positionSuggestDiv();
                        try {
                            const resp = await fetch('/api/sql/suggest', {
                                method: 'POST',
                                headers: {'Content-Type': 'application/json'},
                                credentials: 'include',
                                body: JSON.stringify({prefix: lastWord})
                            });
                            const result = await resp.json();
                            if (result.success && result.suggestions.length > 0) {
                                lastSuggestions = result.suggestions;
                                suggestDiv.innerHTML = result.suggestions.map(s => `<div class=\"sql-suggest-item\" style=\"padding: 8px 12px; cursor: pointer;\">${s}</div>`).join('');
                                suggestDiv.style.display = 'block';
                                // 绑定点击事件
                                Array.from(suggestDiv.children).forEach(item => {
                                    item.onclick = () => {
                                        // 替换最后一个词为建议
                                        const input = document.getElementById('sql-input');
                                        const v = input.value;
                                        const c = input.selectionStart;
                                        const before = v.slice(0, c).replace(/([\w.]+)$/, '');
                                        const after = v.slice(c);
                                        input.value = before + item.textContent + ' ' + after;
                                        input.focus();
                                        // 移动光标到补全后
                                        const newPos = (before + item.textContent + ' ').length;
                                        input.setSelectionRange(newPos, newPos);
                                        suggestDiv.style.display = 'none';
                                    };
                                });
                            } else {
                                suggestDiv.style.display = 'none';
                            }
                        } catch {
                            suggestDiv.style.display = 'none';
                        }
                    });

                    // 失焦/ESC 隐藏
                    document.getElementById('sql-input').addEventListener('blur', function() {
                        setTimeout(() => { suggestDiv.style.display = 'none'; }, 200);
                    });
                    document.getElementById('sql-input').addEventListener('keydown', function(e) {
                        if (e.key === 'Escape') suggestDiv.style.display = 'none';
                        // 支持上下键选择建议
                        if ((e.key === 'ArrowDown' || e.key === 'ArrowUp') && suggestDiv.style.display === 'block') {
                            e.preventDefault();
                            const items = Array.from(suggestDiv.children);
                            let idx = items.findIndex(x => x.classList.contains('active'));
                            if (e.key === 'ArrowDown') idx = (idx + 1) % items.length;
                            if (e.key === 'ArrowUp') idx = (idx - 1 + items.length) % items.length;
                            items.forEach(x => x.classList.remove('active'));
                            if (items[idx]) items[idx].classList.add('active');
                        }
                        // 回车选中建议
                        if (e.key === 'Enter' && suggestDiv.style.display === 'block') {
                            const items = Array.from(suggestDiv.children);
                            const active = items.find(x => x.classList.contains('active')) || items[0];
                            if (active) {
                                active.click();
                                e.preventDefault();
                            }
                        }
                    });
                })();
                // ====== 补全下拉选中项阴影样式 ======
                const style = document.createElement('style');
                style.innerHTML = `
                    #sql-suggest-list .sql-suggest-item.active {
                        background: #f1f5fa;
                        box-shadow: 0 2px 8px 0 rgba(30,64,175,0.18), 0 0px 0px 0 rgba(0,0,0,0.00);
                        font-weight: 600;
                        color: #1e40af;
                    }
                `;
                document.head.appendChild(style);
            </script>
        </body>
        </html>'''

        @self.app.route('/api/health', methods=['GET'])
        def health_check():
            """健康检查"""
            return jsonify({
                'status': 'ok',
                'message': 'Database Web API is running',
            })

        @self.app.route('/api/tables/<table_name>/data', methods=['GET'])
        def get_table_data(table_name: str):
            """获取表的实际数据"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                # 获取分页参数
                page = request.args.get('page', 1, type=int)
                page_size = request.args.get('page_size', 100, type=int)

                session_id = self._get_session_id()
                db = self._get_db(session_id)

                # 检查表是否存在
                schema = db.catalog.get_table_schema(table_name)
                if not schema:
                    return jsonify({
                        'success': False,
                        'message': f'表 {table_name} 不存在'
                    }), 404

                # 获取所有记录
                records = db.table_manager.scan_table(table_name)
                total_count = len(records)

                # 分页处理
                start_idx = (page - 1) * page_size
                end_idx = start_idx + page_size
                paged_records = records[start_idx:end_idx]

                # 转换为前端需要的格式
                columns = [col.name for col in schema.columns]
                rows = []

                for record in paged_records:
                    row = []
                    for col_name in columns:
                        value = record.get(col_name)
                        if value is None:
                            row.append('')
                        elif isinstance(value, bool):
                            row.append('是' if value else '否')
                        else:
                            row.append(str(value))
                    rows.append(row)

                return jsonify({
                    'success': True,
                    'data': {
                        'columns': columns,
                        'rows': rows,
                        'total': total_count,
                        'page': page,
                        'page_size': page_size,
                        'total_pages': (total_count + page_size - 1) // page_size
                    }
                })

            except Exception as e:
                logger.error(f"获取表数据错误: {e}")
                return jsonify({
                    'success': False,
                    'message': f'获取表数据失败: {str(e)}'
                }), 500

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
                username = session.get('username')

                # 只清理会话信息，不关闭数据库连接
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
                # 将常见的不可见非中断空格(U+00A0)替换为标准空格，然后去除首尾空白
                sql = sql.replace('\u00A0', ' ').strip()

                if not sql:
                    return jsonify({
                        'success': False,
                        'message': 'SQL语句不能为空'
                    }), 400
                
                # --- 综合修复：自动清理和校正常见的输入问题 ---
                # 1. 将不可见的非中断空格(U+00A0)替换为标准空格
                sql = sql.replace('\u00A0', ' ')
                # 2. 自动将所有中文（全角）分号替换为英文（半角）分号
                sql = sql.replace('；', ';')
                # 3. 最后，清理整个语句块首尾的空白字符
                sql = sql.strip()
                # --- 修复结束 ---

            
                # 按分号分割语句，并过滤空语句
                stmts = [s.strip() for s in sql.split(';') if s.strip()]
                if not stmts:
                    return jsonify({'success': False, 'message': '未找到有效的SQL语句'}), 400

                all_results = []
                session_id = self._get_session_id()
                db = self._get_db(session_id)


                # 检查是否是Shell命令（以反斜杠开头）
                if sql.strip().startswith('\\') or sql.strip().startswith('\\\\'):
                    result = self._handle_shell_command(sql.strip(), db)
                    all_results.append(result)
                else:
                    # 将SQL按分号分割成多个语句
                    stmts = [s.strip() for s in sql.split(';') if s.strip()]
                    if not stmts:
                        result = {"type": "EMPTY", "message": "请输入SQL语句."}
                        all_results.append(result)
                    else:
                        # 循环执行每个语句，通常客户端只显示最后一条语句的结果
                        for stmt in stmts:
                            # 执行器可能需要以分号结尾
                            full_stmt = stmt + ';'
                            result = db.execute_sql(full_stmt)

                            if not isinstance(result, dict):
                                result = {'success': False, 'message': '执行结果格式错误'}

                            if 'success' not in result:
                                result['success'] = True

                            # 格式化SELECT查询结果
                            if (result.get('success') and
                                result.get('type') == 'SELECT' and
                                'data' in result and
                                isinstance(result['data'], list)):
                                result['formatted'] = self._format_select_for_web(result['data'])
                            
                            all_results.append(result)

                # 如果只有一条语句，为保持兼容性，直接返回结果
                if len(all_results) == 1:
                    return jsonify(all_results[0])
                else:
                    # 如果是多条语句，将结果封装后返回
                    return jsonify({
                        'success': True,
                        'message': f'成功执行 {len(all_results)} 条语句。',
                        'results': all_results
                    })
          

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

        @self.app.route('/api/views', methods=['GET'])
        def list_views():
            """获取视图列表"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                session_id = self._get_session_id()
                db = self._get_db(session_id)
                views = db.list_views()

                return jsonify({
                    'success': True,
                    'data': views,
                    'message': f'共{len(views)}个视图'
                })
            except Exception as e:
                logger.error(f"获取视图列表错误: {e}")
                return jsonify({
                    'success': False,
                    'message': f'获取视图列表失败: {str(e)}'
                }), 500

        @self.app.route('/api/views/<view_name>', methods=['GET'])
        def get_view_info(view_name: str):
            """获取视图信息"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                session_id = self._get_session_id()
                db = self._get_db(session_id)
                info = db.get_view_info(view_name)

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
                logger.error(f"获取视图信息错误: {e}")
                return jsonify({
                    'success': False,
                    'message': f'获取视图信息失败: {str(e)}'
                }), 500

        @self.app.route('/api/views/<view_name>/data', methods=['GET'])
        def get_view_data(view_name: str):
            """获取视图数据"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                # 获取分页参数
                page = request.args.get('page', 1, type=int)
                page_size = request.args.get('page_size', 100, type=int)

                session_id = self._get_session_id()
                db = self._get_db(session_id)
                result = db.get_view_data(view_name, page, page_size)

                if not result.get('success'):
                    return jsonify(result), 404

                # 转换为前端需要的格式
                data = result['data']
                if data['rows']:
                    columns = list(data['rows'][0].keys())
                    rows = []
                    for row in data['rows']:
                        formatted_row = []
                        for col in columns:
                            value = row.get(col)
                            if value is None:
                                formatted_row.append('')
                            elif isinstance(value, bool):
                                formatted_row.append('是' if value else '否')
                            else:
                                formatted_row.append(str(value))
                        rows.append(formatted_row)

                    return jsonify({
                        'success': True,
                        'data': {
                            'columns': columns,
                            'rows': rows,
                            'total': data['total'],
                            'page': data['page'],
                            'page_size': data['page_size'],
                            'total_pages': data['total_pages']
                        }
                    })
                else:
                    return jsonify({
                        'success': True,
                        'data': {
                            'columns': [],
                            'rows': [],
                            'total': 0,
                            'page': 1,
                            'page_size': page_size,
                            'total_pages': 0
                        }
                    })

            except Exception as e:
                logger.error(f"获取视图数据错误: {e}")
                return jsonify({
                    'success': False,
                    'message': f'获取视图数据失败: {str(e)}'
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

        @self.app.route('/api/triggers', methods=['GET'])
        def list_triggers():
            """获取触发器列表"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                session_id = self._get_session_id()
                db = self._get_db(session_id)

                # 调用SQL执行器获取触发器列表
                triggers = db.executor.catalog.list_triggers()

                return jsonify({
                    'success': True,
                    'data': triggers,
                    'total': len(triggers)
                })

            except Exception as e:
                logger.error(f"获取触发器列表时发生错误: {str(e)}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'message': '获取触发器列表失败'
                }), 500

        @self.app.route('/api/triggers', methods=['POST'])
        def create_trigger():
            """创建触发器"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                data = request.get_json()
                if not data:
                    return jsonify({
                        'success': False,
                        'message': '请求数据不能为空'
                    }), 400

                # 直接获取名为 'sql' 的字段，其内容是完整的 CREATE TRIGGER 语句
                sql = data.get('sql', '').strip()
                if not sql:
                    return jsonify({
                        'success': False,
                        'message': '缺少必需的 sql 字段'
                    }), 400

                # (可选) 增加一个简单的校验，确保是创建触发器的语句
                if not sql.upper().startswith('CREATE TRIGGER'):
                    return jsonify({
                        'success': False,
                        'message': '此接口只接受 CREATE TRIGGER 语句'
                    }), 400

                session_id = self._get_session_id()
                db = self._get_db(session_id)
                
                # 直接执行这个完整的、由客户端保证语法正确的SQL语句
                result = db.execute_sql(sql)

                return jsonify(result)

            except Exception as e:
                logger.error(f"创建触发器时发生错误: {str(e)}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'message': '创建触发器失败'
                }), 500

        @self.app.route('/api/triggers/<trigger_name>', methods=['DELETE'])
        def drop_trigger(trigger_name: str):
            """删除触发器"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                # 获取查询参数
                if_exists = request.args.get('if_exists', 'false').lower() == 'true'

                # 构造DROP TRIGGER SQL
                if_exists_clause = " IF EXISTS" if if_exists else ""
                sql = f"DROP TRIGGER{if_exists_clause} {trigger_name};"

                session_id = self._get_session_id()
                db = self._get_db(session_id)
                result = db.execute_sql(sql)

                return jsonify(result)

            except Exception as e:
                logger.error(f"删除触发器时发生错误: {str(e)}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'message': '删除触发器失败'
                }), 500

        @self.app.route('/api/triggers/<trigger_name>', methods=['GET'])
        def get_trigger_info(trigger_name: str):
            """获取触发器详细信息"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                session_id = self._get_session_id()
                db = self._get_db(session_id)

                # 获取特定触发器信息
                trigger = db.executor.catalog.get_trigger(trigger_name)
                if not trigger:
                    return jsonify({
                        'success': False,
                        'message': f'触发器 {trigger_name} 不存在'
                    }), 404

                return jsonify({
                    'success': True,
                    'data': trigger
                })

            except Exception as e:
                logger.error(f"获取触发器信息时发生错误: {str(e)}")
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'message': '获取触发器信息失败'
                }), 500

        @self.app.route('/api/cursors/open', methods=['POST'])
        def open_cursor():
            sql = request.json.get('sql')
            try:
                cursor_id = self.executor.open_cursor(sql)
                return jsonify({'success': True, 'cursor_id': cursor_id})
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)})

        @self.app.route('/api/cursors/fetch', methods=['POST'])
        def fetch_cursor():
            cursor_id = request.json.get('cursor_id')
            n = request.json.get('n', 10)
            try:
                res = self.executor.fetch_cursor(int(cursor_id), int(n))
                return jsonify({'success': True, 'rows': res['rows'], 'done': res['done']})
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)})

        @self.app.route('/api/cursors/close', methods=['POST'])
        def close_cursor():
            cursor_id = request.json.get('cursor_id')
            try:
                ok = self.executor.close_cursor(int(cursor_id))
                return jsonify({'success': ok})
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)})

        # ==================== 事务管理API ====================
        
        @self.app.route('/api/transaction/begin', methods=['POST'])
        def begin_transaction():
            """开始事务"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                session_id = self._get_session_id()
                db = self._get_db(session_id)
                result = db.execute_sql("BEGIN")
                return jsonify(result)

            except Exception as e:
                logger.error(f"开始事务失败: {e}")
                return jsonify({'success': False, 'message': f'开始事务失败: {str(e)}'}), 500

        @self.app.route('/api/transaction/commit', methods=['POST'])
        def commit_transaction():
            """提交事务"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                session_id = self._get_session_id()
                db = self._get_db(session_id)
                result = db.execute_sql("COMMIT")
                return jsonify(result)

            except Exception as e:
                logger.error(f"提交事务失败: {e}")
                return jsonify({'success': False, 'message': f'提交事务失败: {str(e)}'}), 500

        @self.app.route('/api/transaction/rollback', methods=['POST'])
        def rollback_transaction():
            """回滚事务"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                session_id = self._get_session_id()
                db = self._get_db(session_id)
                result = db.execute_sql("ROLLBACK")
                return jsonify(result)

            except Exception as e:
                logger.error(f"回滚事务失败: {e}")
                return jsonify({'success': False, 'message': f'回滚事务失败: {str(e)}'}), 500

        @self.app.route('/api/transaction/status', methods=['GET'])
        def get_transaction_status():
            """获取事务状态"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                session_id = self._get_session_id()
                logger.info(f"获取事务状态，会话ID: {session_id}")
                
                db = self._get_db(session_id)
                logger.info(f"数据库连接成功")
                
                # 获取当前会话信息
                sessions = db.list_sessions()
                logger.info(f"所有会话: {sessions}")
                
                current_session = None
                for s in sessions:
                    if s['current']:
                        current_session = s
                        break
                
                if current_session:
                    logger.info(f"当前会话信息: {current_session}")
                    return jsonify({
                        'success': True,
                        'data': {
                            'session_id': current_session['session_id'],
                            'autocommit': current_session['autocommit'],
                            'in_transaction': current_session['in_txn'],  # 保持字段名一致
                            'isolation_level': current_session['isolation'],
                            'current_user': session.get('username', 'unknown')
                        }
                    })
                else:
                    logger.error("未找到当前会话")
                    return jsonify({'success': False, 'message': '无法获取会话状态'}), 500

            except Exception as e:
                logger.error(f"获取事务状态失败: {e}", exc_info=True)
                return jsonify({'success': False, 'message': f'获取事务状态失败: {str(e)}'}), 500

        @self.app.route('/api/transaction/isolation', methods=['POST'])
        def set_isolation_level():
            """设置隔离级别"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                data = request.get_json()
                if not data:
                    return jsonify({'success': False, 'message': '请求数据格式错误'}), 400

                level = data.get('level', '').strip().upper()
                valid_levels = ['READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE']
                
                if level not in valid_levels:
                    return jsonify({
                        'success': False, 
                        'message': f'无效的隔离级别。支持: {", ".join(valid_levels)}'
                    }), 400

                session_id = self._get_session_id()
                db = self._get_db(session_id)
                result = db.execute_sql(f"SET SESSION TRANSACTION ISOLATION LEVEL {level}")
                return jsonify(result)

            except Exception as e:
                logger.error(f"设置隔离级别失败: {e}")
                return jsonify({'success': False, 'message': f'设置隔离级别失败: {str(e)}'}), 500

        @self.app.route('/api/transaction/autocommit', methods=['POST'])
        def set_autocommit():
            """设置自动提交"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                data = request.get_json()
                if not data:
                    return jsonify({'success': False, 'message': '请求数据格式错误'}), 400

                enabled = data.get('enabled', True)
                session_id = self._get_session_id()
                db = self._get_db(session_id)
                result = db.execute_sql(f"SET autocommit = {1 if enabled else 0}")
                return jsonify(result)

            except Exception as e:
                logger.error(f"设置自动提交失败: {e}")
                return jsonify({'success': False, 'message': f'设置自动提交失败: {str(e)}'}), 500

        @self.app.route('/api/sessions', methods=['GET'])
        def list_sessions():
            """列出所有会话"""
            auth_result = self._require_auth()
            if auth_result:
                return auth_result

            try:
                session_id = self._get_session_id()
                db = self._get_db(session_id)
                sessions = db.list_sessions()
                
                return jsonify({
                    'success': True,
                    'data': sessions
                })

            except Exception as e:
                logger.error(f"获取会话列表失败: {e}")
                return jsonify({'success': False, 'message': f'获取会话列表失败: {str(e)}'}), 500

        @self.app.route('/api/sql/suggest', methods=['POST'])
        def sql_suggest():
            auth_result = self._require_auth()
            if auth_result:
                return auth_result
            try:
                data = request.get_json() or {}
                prefix = (data.get('prefix') or '').strip().lower()
                session_id = self._get_session_id()
                db = self._get_db(session_id)
                # 1. 固定建议词
                seed_words = [
                    "help", "tables", "views", "triggers", "stats", "indexes", "describe ", "show ","FROM",
                    "CREATE TABLE ", "CREATE TRIGGER ", "SELECT ", "INSERT INTO ", "UPDATE ", "DELETE FROM ",
                ]
                # 2. 表名、视图名
                try:
                    tables = db.list_tables() or []
                except Exception:
                    tables = []
                try:
                    views = db.list_views() if hasattr(db, "list_views") else []
                except Exception:
                    views = []
                # 3. 合并并过滤
                all_words = seed_words + tables + views
                suggestions = [w for w in all_words if w.lower().startswith(prefix) and w.lower() != prefix]
                return jsonify({'success': True, 'suggestions': suggestions[:10]})
            except Exception as e:
                return jsonify({'success': False, 'suggestions': [], 'error': str(e)})

    def _handle_shell_command(self, command: str, db) -> dict:
        """处理Shell命令"""
        command = command.strip()
        
        # 会话管理命令
        if command.startswith("\\session") or command.startswith("\\\\session"):
            parts = command.split()
            if len(parts) == 1 or parts[1] == "list":
                return self._handle_session_list(db)
            elif parts[1] == "new":
                return self._handle_session_new(db)
            elif parts[1] == "use" and len(parts) > 2:
                session_id = int(parts[2])
                return self._handle_session_use(session_id, db)
            elif parts[1] in ["info", "status"]:
                return self._handle_session_info(db)
            else:
                return {
                    "success": False,
                    "message": "用法: \\session [list|new|use <id>|info]",
                    "data": []
                }
        
        # 表管理命令
        elif command.lower() in ["\\tables", "\\show tables", "\\\\tables", "\\\\show tables"]:
            return self._handle_show_tables(db)
        
        # 视图管理命令
        elif command.lower() in ["\\views", "\\show views", "\\\\views", "\\\\show views"]:
            return self._handle_show_views(db)
        
        # 索引管理命令
        elif command.lower() in ["\\indexes", "\\show indexes", "\\\\indexes", "\\\\show indexes"]:
            return self._handle_show_indexes(db)
        
        # 统计信息命令
        elif command.lower() in ["\\stats", "\\show stats", "\\\\stats", "\\\\show stats"]:
            return self._handle_show_stats(db)
        
        # 用户管理命令
        elif command.lower() in ["\\users", "\\\\users"]:
            return self._handle_show_users(db)
        
        # 当前用户命令
        elif command.lower() in ["\\whoami", "\\\\whoami"]:
            return self._handle_whoami(db)
        
        # 帮助命令
        elif command.lower() in ["\\help", "\\?", "\\\\help", "\\\\?"]:
            return self._handle_help()
        
        else:
            return {
                "success": False,
                "message": f"未知的Shell命令: {command}",
                "data": []
            }
    
    def _handle_session_list(self, db) -> dict:
        """处理 \\session list 命令"""
        try:
            sessions = db.list_sessions()
            return {
                "success": True,
                "message": f"当前会话列表 (共{len(sessions)}个会话)",
                "data": sessions,
                "type": "SHELL_COMMAND"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"获取会话列表失败: {str(e)}",
                "data": []
            }
    
    def _handle_session_new(self, db) -> dict:
        """处理 \\session new 命令"""
        try:
            new_session_id = db.new_session()
            return {
                "success": True,
                "message": f"新会话已创建，会话ID: {new_session_id}",
                "data": [{"new_session_id": new_session_id}],
                "type": "SHELL_COMMAND"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"创建新会话失败: {str(e)}",
                "data": []
            }
    
    def _handle_session_use(self, session_id: int, db) -> dict:
        """处理 \\session use <id> 命令"""
        try:
            # 检查会话是否存在
            sessions = db.list_sessions()
            valid_session_ids = [s['id'] for s in sessions]
            
            if session_id not in valid_session_ids:
                return {
                    "success": False,
                    "message": f"会话 {session_id} 不存在。可用会话: {valid_session_ids}",
                    "data": [],
                    "type": "SHELL_COMMAND"
                }
            
            # 真正切换到目标会话
            web_session_id = self._get_session_id()
            if not hasattr(self, '_user_selected_sessions'):
                self._user_selected_sessions = {}
            self._user_selected_sessions[web_session_id] = session_id
            
            # 真正切换数据库会话
            success = db.use_session(session_id)
            if not success:
                return {
                    "success": False,
                    "message": f"切换到会话 {session_id} 失败",
                    "data": [],
                    "type": "SHELL_COMMAND"
                }
            
            # 获取目标会话的信息
            target_session = next((s for s in sessions if s['id'] == session_id), None)
            
            return {
                "success": True,
                "message": f"已成功切换到会话 {session_id}。现在可以在此会话中开始事务。",
                "data": [{
                    "current_session_id": session_id,
                    "session_info": target_session,
                    "note": "Web环境中的会话切换是受限的，建议使用新标签页获得独立的会话"
                }],
                "type": "SHELL_COMMAND"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"选择会话失败: {str(e)}",
                "data": [],
                "type": "SHELL_COMMAND"
            }
    
    def _handle_session_info(self, db) -> dict:
        """处理 \\session info 命令"""
        try:
            sessions = db.list_sessions()
            current_session = next((s for s in sessions if s["current"]), None)
            
            if current_session:
                return {
                    "success": True,
                    "message": "当前会话信息",
                    "data": [current_session],
                    "type": "SHELL_COMMAND"
                }
            else:
                return {
                    "success": False,
                    "message": "无法获取当前会话信息",
                    "data": []
                }
        except Exception as e:
            return {
                "success": False,
                "message": f"获取会话信息失败: {str(e)}",
                "data": []
            }
    
    def _handle_show_tables(self, db) -> dict:
        """处理 \\tables 命令"""
        try:
            tables = db.list_tables()
            return {
                "success": True,
                "message": f"数据库中的表 (共{len(tables)}个)",
                "data": tables,
                "type": "SHELL_COMMAND"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"获取表列表失败: {str(e)}",
                "data": []
            }
    
    def _handle_show_views(self, db) -> dict:
        """处理 \\views 命令"""
        try:
            views = db.list_views() if hasattr(db, "list_views") else []
            return {
                "success": True,
                "message": f"数据库中的视图 (共{len(views)}个)",
                "data": views,
                "type": "SHELL_COMMAND"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"获取视图列表失败: {str(e)}",
                "data": []
            }
    
    def _handle_show_indexes(self, db) -> dict:
        """处理 \\indexes 命令"""
        try:
            if hasattr(db, 'list_all_indexes'):
                indexes = db.list_all_indexes()
                return {
                    "success": True,
                    "message": f"数据库中的索引 (共{len(indexes)}个)",
                    "data": indexes,
                    "type": "SHELL_COMMAND"
                }
            else:
                return {
                    "success": False,
                    "message": "索引功能不可用",
                    "data": []
                }
        except Exception as e:
            return {
                "success": False,
                "message": f"获取索引列表失败: {str(e)}",
                "data": []
            }
    
    def _handle_show_stats(self, db) -> dict:
        """处理 \\stats 命令"""
        try:
            stats = db.get_database_stats()
            return {
                "success": True,
                "message": "数据库统计信息",
                "data": [stats],
                "type": "SHELL_COMMAND"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"获取统计信息失败: {str(e)}",
                "data": []
            }
    
    def _handle_show_users(self, db) -> dict:
        """处理 \\users 命令"""
        try:
            # 这里需要从catalog获取用户列表
            users = list(db.catalog.users.keys()) if hasattr(db.catalog, 'users') else []
            return {
                "success": True,
                "message": f"数据库用户列表 (共{len(users)}个)",
                "data": users,
                "type": "SHELL_COMMAND"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"获取用户列表失败: {str(e)}",
                "data": []
            }
    
    def _handle_whoami(self, db) -> dict:
        """处理 \\whoami 命令"""
        try:
            current_user = db.get_current_user()
            return {
                "success": True,
                "message": f"当前登录用户: {current_user}",
                "data": [{"current_user": current_user}],
                "type": "SHELL_COMMAND"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"获取当前用户失败: {str(e)}",
                "data": []
            }
    
    def _handle_help(self) -> dict:
        """处理 \\help 命令"""
        help_text = """
MiniSQL Shell 命令帮助:

📋 会话管理:
\\session list                    - 列出所有会话
\\session new                     - 创建新会话
\\session use <id>                - 选择指定会话 (Web环境受限)
\\session info                    - 显示当前会话信息

💡 Web环境提示:
- 每个浏览器标签页有独立的数据库会话
- 要使用不同会话，请打开新标签页
- 会话切换在Web环境中是受限的

📊 数据库管理:
\\tables                          - 列出所有表
\\views                           - 列出所有视图
\\indexes                         - 列出所有索引
\\stats                           - 显示数据库统计信息

👥 用户管理:
\\users                           - 列出所有用户
\\whoami                          - 显示当前用户

❓ 帮助:
\\help 或 \\?                     - 显示此帮助信息

💡 提示: 所有标准SQL语句也支持，如 SELECT, INSERT, UPDATE, DELETE 等
        """
        return {
            "success": True,
            "message": "Shell命令帮助",
            "data": [{"help": help_text.strip()}],
            "type": "SHELL_COMMAND"
        }

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
        try:
            self.db.close()
        except Exception as e:
            logger.warning(f"关闭连接时出错: {e}")
        print("数据库连接已关闭")


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
