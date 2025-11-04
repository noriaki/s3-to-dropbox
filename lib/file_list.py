"""
ファイルリスト生成を提供するモジュール
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple
import logging
import humanize


class FileListGenerator:
    """ファイルリスト・README生成を提供するクラス"""

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        ファイルリスト生成の初期化

        Args:
            logger: ロガー（オプション）
        """
        self.logger = logger or logging.getLogger(__name__)

    def generate_tree_structure(self, directory: str, prefix: str = "", max_depth: int = 10,
                                current_depth: int = 0) -> List[str]:
        """
        ディレクトリツリー構造を生成

        Args:
            directory: ディレクトリパス
            prefix: プレフィックス（再帰用）
            max_depth: 最大深度
            current_depth: 現在の深度

        Returns:
            List[str]: ツリー構造の行リスト
        """
        if current_depth >= max_depth:
            return [f"{prefix}...（深すぎるためスキップ）"]

        try:
            entries = []
            items = sorted(os.listdir(directory))

            for i, item in enumerate(items):
                item_path = os.path.join(directory, item)
                is_last = i == len(items) - 1

                # ツリー記号
                connector = "└── " if is_last else "├── "
                extension = "    " if is_last else "│   "

                if os.path.isdir(item_path):
                    entries.append(f"{prefix}{connector}{item}/")
                    # 再帰的にサブディレクトリを処理
                    sub_entries = self.generate_tree_structure(
                        item_path,
                        prefix + extension,
                        max_depth,
                        current_depth + 1
                    )
                    entries.extend(sub_entries)
                else:
                    # ファイルサイズを取得
                    try:
                        size = os.path.getsize(item_path)
                        size_str = humanize.naturalsize(size, binary=True)
                        entries.append(f"{prefix}{connector}{item} ({size_str})")
                    except:
                        entries.append(f"{prefix}{connector}{item}")

            return entries

        except Exception as e:
            self.logger.error(f"ツリー構造の生成に失敗: {str(e)}")
            return []

    def collect_file_info(self, directory: str) -> List[Tuple[str, int, str]]:
        """
        ディレクトリ内の全ファイル情報を収集

        Args:
            directory: ディレクトリパス

        Returns:
            List[Tuple[str, int, str]]: (相対パス, サイズ, 最終更新日時)のリスト
        """
        file_info = []

        try:
            for root, dirs, files in os.walk(directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, directory)

                    try:
                        size = os.path.getsize(file_path)
                        mtime = os.path.getmtime(file_path)
                        mtime_str = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')

                        file_info.append((rel_path, size, mtime_str))
                    except Exception as e:
                        self.logger.warning(f"ファイル情報取得失敗: {file_path} - {str(e)}")

            return sorted(file_info)

        except Exception as e:
            self.logger.error(f"ファイル情報の収集に失敗: {str(e)}")
            return []

    def generate_file_list_md(self, directory: str, bucket_name: str,
                             output_path: str) -> bool:
        """
        file_list.mdを生成

        Args:
            directory: スキャン対象のディレクトリ
            bucket_name: バケット名
            output_path: 出力ファイルパス

        Returns:
            bool: 成功した場合True
        """
        try:
            # ファイル情報を収集
            file_info = self.collect_file_info(directory)

            # 統計情報
            total_files = len(file_info)
            total_size = sum(size for _, size, _ in file_info)
            total_size_str = humanize.naturalsize(total_size, binary=True)

            # ツリー構造を生成
            tree_lines = self.generate_tree_structure(directory)

            # Markdownファイルを生成
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(f"# {bucket_name} ファイル一覧\n\n")
                f.write(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"総ファイル数: {total_files:,}\n")
                f.write(f"総サイズ: {total_size_str}\n\n")

                # ディレクトリ構造
                f.write("## ディレクトリ構造\n\n")
                f.write("```\n")
                f.write(f"{bucket_name}/\n")
                for line in tree_lines:
                    f.write(f"{line}\n")
                f.write("```\n\n")

                # ファイル詳細リスト
                f.write("## ファイル詳細リスト\n\n")
                f.write("| パス | サイズ | 最終更新日時 |\n")
                f.write("|------|--------|-------------|\n")

                for path, size, mtime in file_info:
                    size_str = humanize.naturalsize(size, binary=True)
                    # パスをエスケープ
                    path_escaped = path.replace("|", "\\|")
                    f.write(f"| {path_escaped} | {size_str} | {mtime} |\n")

            self.logger.info(f"ファイルリストを生成しました: {output_path}")
            return True

        except Exception as e:
            self.logger.error(f"ファイルリスト生成に失敗: {str(e)}")
            return False

    def generate_readme_md(self, bucket_name: str, output_path: str,
                          bucket_info: dict, compression_info: dict) -> bool:
        """
        README.mdを生成

        Args:
            bucket_name: バケット名
            output_path: 出力ファイルパス
            bucket_info: バケット情報（region, created_date等）
            compression_info: 圧縮情報（format, files, split等）

        Returns:
            bool: 成功した場合True
        """
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(f"# {bucket_name} バックアップ\n\n")

                # バックアップ情報
                f.write("## バックアップ情報\n\n")
                f.write(f"- **バケット名**: {bucket_name}\n")
                f.write(f"- **バックアップ日時**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"- **元のS3リージョン**: {bucket_info.get('region', 'unknown')}\n")
                f.write(f"- **総ファイル数**: {bucket_info.get('object_count', 0):,}\n")

                original_size = bucket_info.get('original_size', 0)
                compressed_size = compression_info.get('total_size', 0)

                f.write(f"- **元の総サイズ**: {humanize.naturalsize(original_size, binary=True)}\n")
                f.write(f"- **圧縮後サイズ**: {humanize.naturalsize(compressed_size, binary=True)}\n")

                if original_size > 0:
                    compression_ratio = (compressed_size / original_size) * 100
                    f.write(f"- **圧縮率**: {compression_ratio:.1f}%\n")

                f.write("\n")

                # 圧縮ファイル情報
                f.write("## 圧縮ファイル情報\n\n")

                compression_format = compression_info.get('format', 'zip')
                files = compression_info.get('files', [])
                is_split = len(files) > 1

                if is_split:
                    f.write(f"⚠️ **注意**: このバックアップは{len(files)}個のファイルに分割されています。\n\n")
                    f.write("### 分割ファイル一覧\n\n")
                    for i, file_path in enumerate(files, 1):
                        file_name = os.path.basename(file_path)
                        f.write(f"{i}. `{file_name}`\n")
                    f.write("\n")
                else:
                    file_name = os.path.basename(files[0]) if files else "unknown"
                    f.write(f"- **ファイル名**: `{file_name}`\n")
                    f.write(f"- **圧縮形式**: {compression_format}\n\n")

                # 解凍方法
                f.write("## 解凍方法\n\n")

                if is_split:
                    # 分割ファイルの結合・解凍方法
                    base_name = os.path.basename(files[0]).replace('.001', '') if files else "archive"

                    f.write("### 🖥️ macOS / Linux\n\n")
                    f.write("```bash\n")
                    f.write("# 1. 全ての分割ファイルを同じディレクトリに配置\n\n")
                    f.write("# 2. 分割ファイルを結合\n")
                    f.write(f"cat {base_name}.* > {base_name}\n\n")
                    f.write("# 3. 解凍\n")
                    if compression_format == 'zip':
                        f.write(f"unzip {base_name}\n")
                    else:  # tar.gz
                        f.write(f"tar -xzf {base_name}\n")
                    f.write("```\n\n")

                    f.write("### 🪟 Windows (PowerShell)\n\n")
                    f.write("```powershell\n")
                    f.write("# 1. 全ての分割ファイルを同じフォルダに配置\n\n")
                    f.write("# 2. 分割ファイルを結合\n")
                    f.write(f"Get-Content {base_name}.* -Raw | Set-Content {base_name} -Encoding Byte\n\n")
                    f.write("# 3. 解凍（7-Zipが必要）\n")
                    if compression_format == 'zip':
                        f.write(f"7z x {base_name}\n")
                    else:  # tar.gz
                        f.write(f"7z x {base_name}\n")
                    f.write("```\n\n")

                else:
                    # 通常の解凍方法
                    file_name = os.path.basename(files[0]) if files else "archive.zip"

                    f.write("### 🖥️ macOS / Linux\n\n")
                    f.write("```bash\n")
                    if compression_format == 'zip':
                        f.write(f"unzip {file_name}\n")
                    else:  # tar.gz
                        f.write(f"tar -xzf {file_name}\n")
                    f.write("```\n\n")

                    f.write("### 🪟 Windows\n\n")
                    if compression_format == 'zip':
                        f.write("1. ZIPファイルを右クリック\n")
                        f.write("2. 「すべて展開」を選択\n")
                        f.write("3. 展開先を指定して実行\n\n")
                    else:  # tar.gz
                        f.write("7-Zipなどのツールを使用:\n\n")
                        f.write("```cmd\n")
                        f.write(f"7z x {file_name}\n")
                        f.write("```\n\n")

                # その他の情報
                f.write("## その他の情報\n\n")
                f.write("### file_list.md について\n\n")
                f.write("このフォルダには `file_list.md` ファイルが含まれています。\n")
                f.write("このファイルには以下の情報が記載されています:\n\n")
                f.write("- バケット内のディレクトリ構造（ツリー表示）\n")
                f.write("- 全ファイルの詳細リスト（パス、サイズ、最終更新日時）\n\n")

                f.write("### 注意事項\n\n")
                f.write("- 解凍には十分なディスク容量が必要です\n")
                f.write(f"- 推奨空き容量: {humanize.naturalsize(original_size * 1.2, binary=True)} 以上\n")
                if is_split:
                    f.write("- 分割ファイルは全て同じディレクトリに配置してください\n")
                    f.write("- 分割ファイルが1つでも欠けていると解凍できません\n")

                f.write("\n---\n\n")
                f.write("*このバックアップは S3-to-Dropbox ツールで作成されました*\n")

            self.logger.info(f"READMEを生成しました: {output_path}")
            return True

        except Exception as e:
            self.logger.error(f"README生成に失敗: {str(e)}")
            return False
