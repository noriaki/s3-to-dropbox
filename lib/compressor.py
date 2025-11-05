"""
圧縮・分割機能を提供するモジュール
"""

import os
import zipfile
import tarfile
import shutil
from pathlib import Path
from typing import List, Optional, Tuple
import logging


class Compressor:
    """ファイル圧縮・分割機能を提供するクラス"""

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        圧縮機能の初期化

        Args:
            logger: ロガー（オプション）
        """
        self.logger = logger or logging.getLogger(__name__)

    def estimate_compressed_size(self, source_path: str, compression_ratio: float = 0.7) -> int:
        """
        圧縮後のサイズを推定

        Args:
            source_path: 圧縮対象のパス
            compression_ratio: 圧縮率（デフォルト: 0.7 = 70%に圧縮）

        Returns:
            int: 推定圧縮後サイズ（バイト）
        """
        total_size = 0
        for root, dirs, files in os.walk(source_path):
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.exists(file_path):
                    total_size += os.path.getsize(file_path)

        estimated_size = int(total_size * compression_ratio)
        self.logger.debug(f"推定圧縮サイズ: {total_size} -> {estimated_size} bytes")
        return estimated_size

    def compress_to_zip(self, source_path: str, output_path: str,
                       progress_callback: Optional[callable] = None) -> bool:
        """
        ディレクトリをZIPファイルに圧縮

        Args:
            source_path: 圧縮対象のディレクトリパス
            output_path: 出力ZIPファイルパス
            progress_callback: 進捗コールバック関数

        Returns:
            bool: 成功した場合True
        """
        try:
            # 出力ディレクトリの作成
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # ファイル数をカウント
            file_count = sum(len(files) for _, _, files in os.walk(source_path))
            processed = 0

            with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(source_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        # 相対パスを計算
                        arcname = os.path.relpath(file_path, source_path)
                        zipf.write(file_path, arcname)

                        processed += 1
                        if progress_callback:
                            progress_callback(processed, file_count)

            output_size = os.path.getsize(output_path)
            self.logger.info(f"ZIP圧縮完了: {output_path} ({output_size:,} bytes)")
            return True

        except Exception as e:
            self.logger.error(f"ZIP圧縮に失敗: {str(e)}")
            # 失敗した場合は不完全なファイルを削除
            if os.path.exists(output_path):
                os.remove(output_path)
            return False

    def compress_to_tar_gz(self, source_path: str, output_path: str,
                          progress_callback: Optional[callable] = None) -> bool:
        """
        ディレクトリをtar.gzファイルに圧縮

        Args:
            source_path: 圧縮対象のディレクトリパス
            output_path: 出力tar.gzファイルパス
            progress_callback: 進捗コールバック関数

        Returns:
            bool: 成功した場合True
        """
        try:
            # 出力ディレクトリの作成
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # ファイル数をカウント
            file_count = sum(len(files) for _, _, files in os.walk(source_path))
            processed = 0

            with tarfile.open(output_path, 'w:gz') as tar:
                for root, dirs, files in os.walk(source_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        # 相対パスを計算
                        arcname = os.path.relpath(file_path, source_path)
                        tar.add(file_path, arcname=arcname)

                        processed += 1
                        if progress_callback:
                            progress_callback(processed, file_count)

            output_size = os.path.getsize(output_path)
            self.logger.info(f"tar.gz圧縮完了: {output_path} ({output_size:,} bytes)")
            return True

        except Exception as e:
            self.logger.error(f"tar.gz圧縮に失敗: {str(e)}")
            # 失敗した場合は不完全なファイルを削除
            if os.path.exists(output_path):
                os.remove(output_path)
            return False

    def split_file(self, file_path: str, chunk_size: int = 10 * 1024 * 1024 * 1024) -> List[str]:
        """
        ファイルを指定サイズで分割

        Args:
            file_path: 分割対象のファイルパス
            chunk_size: チャンクサイズ（バイト、デフォルト: 10GB）

        Returns:
            List[str]: 分割ファイルのパスリスト
        """
        try:
            file_size = os.path.getsize(file_path)

            # 分割が必要ない場合
            if file_size <= chunk_size:
                self.logger.info(f"ファイルサイズが{chunk_size:,}バイト以下のため、分割不要です")
                return [file_path]

            # 分割ファイルのリスト
            split_files = []

            # ファイルを読み込んで分割
            with open(file_path, 'rb') as f:
                chunk_num = 1
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break

                    # 分割ファイル名: original.zip.001, original.zip.002, ...
                    split_file_path = f"{file_path}.{chunk_num:03d}"
                    with open(split_file_path, 'wb') as chunk_file:
                        chunk_file.write(chunk)

                    split_files.append(split_file_path)
                    self.logger.info(f"分割ファイル作成: {split_file_path} ({len(chunk):,} bytes)")
                    chunk_num += 1

            # 元のファイルを削除
            os.remove(file_path)
            self.logger.info(f"ファイルを{len(split_files)}個に分割しました")

            return split_files

        except Exception as e:
            self.logger.error(f"ファイル分割に失敗: {str(e)}")
            # 失敗した場合は不完全な分割ファイルを削除
            for split_file in split_files:
                if os.path.exists(split_file):
                    os.remove(split_file)
            raise

    def compress_directory(self, source_path: str, output_base_path: str,
                          compression_format: str = 'zip',
                          split_size: int = 10 * 1024 * 1024 * 1024,
                          progress_callback: Optional[callable] = None) -> Tuple[List[str], int]:
        """
        ディレクトリを圧縮し、必要に応じて分割

        Args:
            source_path: 圧縮対象のディレクトリパス
            output_base_path: 出力ファイルのベースパス（拡張子なし）
            compression_format: 圧縮形式（'zip' または 'tar.gz'）
            split_size: 分割サイズ（バイト、デフォルト: 10GB）
            progress_callback: 進捗コールバック関数

        Returns:
            Tuple[List[str], int]: (圧縮ファイルパスのリスト, 圧縮後の総サイズ)
        """
        try:
            # 圧縮形式に応じた拡張子
            if compression_format == 'zip':
                compressed_file = f"{output_base_path}.zip"
                compress_func = self.compress_to_zip
            elif compression_format == 'tar.gz':
                compressed_file = f"{output_base_path}.tar.gz"
                compress_func = self.compress_to_tar_gz
            else:
                raise ValueError(f"サポートされていない圧縮形式: {compression_format}")

            # 圧縮実行
            self.logger.info(f"圧縮開始: {source_path} -> {compressed_file}")
            success = compress_func(source_path, compressed_file, progress_callback)

            if not success:
                raise Exception("圧縮に失敗しました")

            # 圧縮後のサイズを確認
            compressed_size = os.path.getsize(compressed_file)

            # 分割が必要か確認
            if compressed_size > split_size:
                self.logger.info(f"圧縮ファイルが{split_size:,}バイトを超えているため、分割します")
                split_files = self.split_file(compressed_file, split_size)
                total_size = sum(os.path.getsize(f) for f in split_files)
                return split_files, total_size
            else:
                return [compressed_file], compressed_size

        except Exception as e:
            self.logger.error(f"ディレクトリ圧縮に失敗: {str(e)}")
            raise

    def verify_archive(self, archive_path: str, compression_format: str = 'zip') -> bool:
        """
        圧縮ファイルの整合性を確認

        Args:
            archive_path: 圧縮ファイルパス
            compression_format: 圧縮形式（'zip' または 'tar.gz'）

        Returns:
            bool: 整合性が確認できた場合True
        """
        try:
            if compression_format == 'zip':
                with zipfile.ZipFile(archive_path, 'r') as zipf:
                    # テスト実行（CRCチェック）
                    bad_file = zipf.testzip()
                    if bad_file:
                        self.logger.error(f"破損したファイルが見つかりました: {bad_file}")
                        return False
            elif compression_format == 'tar.gz':
                with tarfile.open(archive_path, 'r:gz') as tar:
                    # 全メンバーをチェック
                    tar.getmembers()
            else:
                raise ValueError(f"サポートされていない圧縮形式: {compression_format}")

            self.logger.info(f"圧縮ファイルの整合性確認OK: {archive_path}")
            return True

        except Exception as e:
            self.logger.error(f"圧縮ファイルの整合性確認に失敗: {str(e)}")
            return False

    def check_disk_space(self, path: str, required_space: int) -> Tuple[bool, int]:
        """
        ディスク容量が十分か確認

        Args:
            path: チェック対象のパス
            required_space: 必要な容量（バイト）

        Returns:
            Tuple[bool, int]: (容量が十分な場合True, 利用可能容量)
        """
        try:
            stat = shutil.disk_usage(path)
            available = stat.free

            is_sufficient = available >= required_space

            if not is_sufficient:
                self.logger.warning(
                    f"ディスク容量不足: 必要={required_space:,}バイト, "
                    f"利用可能={available:,}バイト"
                )

            return is_sufficient, available

        except Exception as e:
            self.logger.error(f"ディスク容量の確認に失敗: {str(e)}")
            return False, 0

    def merge_split_files(self, split_files: List[str], output_path: str,
                         progress_callback: Optional[callable] = None) -> bool:
        """
        分割ファイルを結合

        Args:
            split_files: 分割ファイルパスのリスト（順番にソート済み）
            output_path: 出力ファイルパス
            progress_callback: 進捗コールバック関数(current, total)

        Returns:
            bool: 成功した場合True
        """
        try:
            # 分割ファイルの総サイズを計算
            total_size = sum(os.path.getsize(f) for f in split_files)
            processed_size = 0

            # 出力ディレクトリの作成
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            self.logger.info(f"分割ファイル結合開始: {len(split_files)}個 -> {output_path}")

            # 分割ファイルを順番に読み込んで結合
            with open(output_path, 'wb') as output_file:
                for i, split_file in enumerate(split_files, 1):
                    if not os.path.exists(split_file):
                        raise FileNotFoundError(f"分割ファイルが見つかりません: {split_file}")

                    file_size = os.path.getsize(split_file)
                    self.logger.debug(f"結合中 [{i}/{len(split_files)}]: {split_file}")

                    with open(split_file, 'rb') as input_file:
                        # チャンクで読み込んで書き込み（メモリ節約）
                        chunk_size = 10 * 1024 * 1024  # 10MB
                        while True:
                            chunk = input_file.read(chunk_size)
                            if not chunk:
                                break
                            output_file.write(chunk)

                    processed_size += file_size
                    if progress_callback:
                        progress_callback(i, len(split_files))

            output_size = os.path.getsize(output_path)
            self.logger.info(f"ファイル結合完了: {output_path} ({output_size:,} bytes)")

            # サイズ検証
            if output_size != total_size:
                self.logger.warning(
                    f"結合後のサイズが予想と異なります: 期待={total_size:,}, 実際={output_size:,}"
                )

            return True

        except Exception as e:
            self.logger.error(f"ファイル結合に失敗: {str(e)}")
            # 失敗した場合は不完全なファイルを削除
            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except Exception:
                    pass
            return False

    def extract_archive(self, archive_path: str, output_dir: str,
                       compression_format: str = 'zip',
                       progress_callback: Optional[callable] = None) -> bool:
        """
        圧縮ファイルを展開

        Args:
            archive_path: 圧縮ファイルパス
            output_dir: 展開先ディレクトリ
            compression_format: 圧縮形式（'zip' または 'tar.gz'）
            progress_callback: 進捗コールバック関数(current, total)

        Returns:
            bool: 成功した場合True
        """
        try:
            # 出力ディレクトリの作成
            os.makedirs(output_dir, exist_ok=True)

            self.logger.info(f"アーカイブ展開開始: {archive_path} -> {output_dir}")

            if compression_format == 'zip':
                with zipfile.ZipFile(archive_path, 'r') as zipf:
                    members = zipf.namelist()
                    total = len(members)

                    for i, member in enumerate(members, 1):
                        zipf.extract(member, output_dir)
                        if progress_callback:
                            progress_callback(i, total)

            elif compression_format == 'tar.gz':
                with tarfile.open(archive_path, 'r:gz') as tar:
                    members = tar.getmembers()
                    total = len(members)

                    for i, member in enumerate(members, 1):
                        tar.extract(member, output_dir)
                        if progress_callback:
                            progress_callback(i, total)

            else:
                raise ValueError(f"サポートされていない圧縮形式: {compression_format}")

            # 展開されたファイル数を確認
            extracted_count = sum(len(files) for _, _, files in os.walk(output_dir))
            self.logger.info(f"アーカイブ展開完了: {extracted_count}個のファイルを展開")

            return True

        except Exception as e:
            self.logger.error(f"アーカイブ展開に失敗: {str(e)}")
            # 失敗した場合は展開途中のファイルを削除
            if os.path.exists(output_dir):
                try:
                    shutil.rmtree(output_dir)
                except Exception:
                    pass
            return False
