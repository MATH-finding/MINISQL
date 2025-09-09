"""
数据库日志器
"""

import os
import time
from datetime import datetime
from typing import Optional
from enum import Enum


class LogLevel(Enum):
    """日志级别"""

    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4
    CRITICAL = 5


class DatabaseLogger:
    """数据库日志器"""

    def __init__(self, db_name: str, log_dir: str = "logs"):
        self.db_name = db_name
        self.log_dir = log_dir
        self.log_file = os.path.join(log_dir, f"{db_name}.log")
        self.min_level = LogLevel.INFO

        # 确保日志目录存在
        os.makedirs(log_dir, exist_ok=True)

        # 启动时记录一条信息
        self._write_startup_info()

    def _write_startup_info(self):
        """写入启动信息"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] [INFO] [SYSTEM] 数据库 {self.db_name} 启动\n")

    def _write_log(self, level: LogLevel, message: str, component: str = "SYSTEM"):
        """写入日志"""
        if level.value < self.min_level.value:
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"[{timestamp}] [{level.name}] [{component}] {message}\n"

        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(log_line)
        except Exception as e:
            print(f"写入日志失败: {e}")

    def debug(self, message: str, component: str = "SYSTEM"):
        self._write_log(LogLevel.DEBUG, message, component)

    def info(self, message: str, component: str = "SYSTEM"):
        self._write_log(LogLevel.INFO, message, component)

    def warning(self, message: str, component: str = "SYSTEM"):
        self._write_log(LogLevel.WARNING, message, component)

    def error(self, message: str, component: str = "SYSTEM"):
        self._write_log(LogLevel.ERROR, message, component)

    def critical(self, message: str, component: str = "SYSTEM"):
        self._write_log(LogLevel.CRITICAL, message, component)

    def set_log_level(self, level: LogLevel):
        self.min_level = level

    def close(self):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] [INFO] [SYSTEM] 数据库 {self.db_name} 关闭\n")
