"""
ロギング機能を提供するモジュール
"""

import os
import logging
from datetime import datetime
from pathlib import Path
from colorama import Fore, Style, init

# Coloramaの初期化
init(autoreset=True)


class ColoredFormatter(logging.Formatter):
    """カラフルなコンソール出力用のフォーマッター"""

    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.RED + Style.BRIGHT,
    }

    def format(self, record):
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{Style.RESET_ALL}"
        return super().format(record)


def setup_logger(name: str, log_level: str = "INFO") -> logging.Logger:
    """
    ロガーのセットアップ

    Args:
        name: ロガー名
        log_level: ログレベル（DEBUG, INFO, WARNING, ERROR, CRITICAL）

    Returns:
        logging.Logger: 設定済みのロガー
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))

    # すでにハンドラーが設定されている場合は追加しない
    if logger.handlers:
        return logger

    # コンソールハンドラー
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = ColoredFormatter(
        '%(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # ファイルハンドラー
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{name}_{timestamp}.log"

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger


def log_exception(logger: logging.Logger, message: str, exc: Exception):
    """
    例外をログに記録

    Args:
        logger: ロガー
        message: エラーメッセージ
        exc: 例外オブジェクト
    """
    logger.error(f"{message}: {str(exc)}")
    logger.debug("詳細なエラー情報:", exc_info=True)
