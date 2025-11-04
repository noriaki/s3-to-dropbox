"""
進行状況管理を提供するモジュール
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
import logging


class ProgressManager:
    """進行状況の永続化と管理を提供するクラス"""

    def __init__(self, progress_file: str = "data/migration_progress.json",
                 logger: Optional[logging.Logger] = None):
        """
        進行状況管理の初期化

        Args:
            progress_file: 進行状況ファイルのパス
            logger: ロガー（オプション）
        """
        self.progress_file = Path(progress_file)
        self.logger = logger or logging.getLogger(__name__)

        # 進行状況データの初期化
        self.progress_data = {
            "version": "1.0.0",
            "start_time": None,
            "last_updated": None,
            "current_bucket": None,
            "completed_buckets": [],
            "failed_buckets": [],
            "skipped_buckets": []
        }

        # 既存の進行状況を読み込み
        self._load_progress()

    def _load_progress(self):
        """進行状況ファイルを読み込み"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                    self.progress_data.update(loaded_data)
                    self.logger.info(f"進行状況を読み込みました: {self.progress_file}")
            except Exception as e:
                self.logger.warning(f"進行状況の読み込みに失敗: {str(e)}")
                self.logger.warning("新しい進行状況を開始します")

    def _save_progress(self):
        """進行状況をファイルに保存"""
        try:
            # ディレクトリが存在しない場合は作成
            self.progress_file.parent.mkdir(parents=True, exist_ok=True)

            # 最終更新時刻を記録
            self.progress_data["last_updated"] = datetime.now().isoformat()

            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress_data, f, ensure_ascii=False, indent=2)

            self.logger.debug(f"進行状況を保存しました: {self.progress_file}")

        except Exception as e:
            self.logger.error(f"進行状況の保存に失敗: {str(e)}")

    def start_migration(self):
        """移行プロセスの開始"""
        if not self.progress_data["start_time"]:
            self.progress_data["start_time"] = datetime.now().isoformat()
            self._save_progress()
            self.logger.info("移行プロセスを開始しました")

    def set_current_bucket(self, bucket_name: str):
        """
        現在処理中のバケットを設定

        Args:
            bucket_name: バケット名
        """
        self.progress_data["current_bucket"] = bucket_name
        self._save_progress()
        self.logger.info(f"処理中のバケット: {bucket_name}")

    def mark_bucket_completed(self, bucket_name: str, info: Dict):
        """
        バケットの処理完了を記録

        Args:
            bucket_name: バケット名
            info: バケット情報（オブジェクト数、サイズ等）
        """
        completed_info = {
            "bucket_name": bucket_name,
            "completed_at": datetime.now().isoformat(),
            **info
        }

        self.progress_data["completed_buckets"].append(completed_info)

        # Remove from failed_buckets if this is a retry that succeeded
        self.progress_data["failed_buckets"] = [
            b for b in self.progress_data["failed_buckets"]
            if b["bucket_name"] != bucket_name
        ]

        self.progress_data["current_bucket"] = None
        self._save_progress()

        self.logger.info(f"バケット処理完了: {bucket_name}")

    def mark_bucket_failed(self, bucket_name: str, error: str):
        """
        バケットの処理失敗を記録

        Args:
            bucket_name: バケット名
            error: エラーメッセージ
        """
        failed_info = {
            "bucket_name": bucket_name,
            "failed_at": datetime.now().isoformat(),
            "error": error
        }

        self.progress_data["failed_buckets"].append(failed_info)
        self.progress_data["current_bucket"] = None
        self._save_progress()

        self.logger.error(f"バケット処理失敗: {bucket_name} - {error}")

    def mark_bucket_skipped(self, bucket_name: str, reason: str):
        """
        バケットのスキップを記録

        Args:
            bucket_name: バケット名
            reason: スキップした理由
        """
        skipped_info = {
            "bucket_name": bucket_name,
            "skipped_at": datetime.now().isoformat(),
            "reason": reason
        }

        self.progress_data["skipped_buckets"].append(skipped_info)
        self._save_progress()

        self.logger.info(f"バケットをスキップ: {bucket_name} - {reason}")

    def is_bucket_completed(self, bucket_name: str) -> bool:
        """
        バケットが完了済みか確認

        Args:
            bucket_name: バケット名

        Returns:
            bool: 完了済みの場合True
        """
        completed_names = [b["bucket_name"] for b in self.progress_data["completed_buckets"]]
        return bucket_name in completed_names

    def is_bucket_failed(self, bucket_name: str) -> bool:
        """
        バケットが失敗済みか確認

        Args:
            bucket_name: バケット名

        Returns:
            bool: 失敗済みの場合True
        """
        failed_names = [b["bucket_name"] for b in self.progress_data["failed_buckets"]]
        return bucket_name in failed_names

    def get_completed_buckets(self) -> List[Dict]:
        """
        完了済みバケットのリストを取得

        Returns:
            List[Dict]: 完了済みバケット情報のリスト
        """
        return self.progress_data["completed_buckets"]

    def get_failed_buckets(self) -> List[Dict]:
        """
        失敗したバケットのリストを取得

        Returns:
            List[Dict]: 失敗したバケット情報のリスト
        """
        return self.progress_data["failed_buckets"]

    def get_pending_buckets(self, all_buckets: List[str]) -> List[str]:
        """
        未処理のバケットリストを取得

        Args:
            all_buckets: 全バケット名のリスト

        Returns:
            List[str]: 未処理バケット名のリスト
        """
        completed = [b["bucket_name"] for b in self.progress_data["completed_buckets"]]
        skipped = [b["bucket_name"] for b in self.progress_data["skipped_buckets"]]
        processed = set(completed + skipped)

        pending = [b for b in all_buckets if b not in processed]
        return pending

    def get_summary(self) -> Dict:
        """
        進行状況のサマリーを取得

        Returns:
            Dict: サマリー情報
        """
        completed_count = len(self.progress_data["completed_buckets"])
        failed_count = len(self.progress_data["failed_buckets"])
        skipped_count = len(self.progress_data["skipped_buckets"])

        # 完了したバケットの統計
        total_objects = 0
        total_original_size = 0
        total_compressed_size = 0

        for bucket in self.progress_data["completed_buckets"]:
            total_objects += bucket.get("object_count", 0)
            total_original_size += bucket.get("original_size", 0)
            total_compressed_size += bucket.get("compressed_size", 0)

        return {
            "start_time": self.progress_data.get("start_time"),
            "last_updated": self.progress_data.get("last_updated"),
            "current_bucket": self.progress_data.get("current_bucket"),
            "completed_count": completed_count,
            "failed_count": failed_count,
            "skipped_count": skipped_count,
            "total_objects": total_objects,
            "total_original_size": total_original_size,
            "total_compressed_size": total_compressed_size
        }

    def print_summary(self):
        """進行状況のサマリーをコンソールに出力"""
        summary = self.get_summary()

        print("\n" + "=" * 70)
        print("📊 進行状況サマリー")
        print("=" * 70)

        if summary["start_time"]:
            print(f"開始時刻: {summary['start_time']}")
        if summary["last_updated"]:
            print(f"最終更新: {summary['last_updated']}")

        print(f"\n✅ 完了: {summary['completed_count']}バケット")
        print(f"❌ 失敗: {summary['failed_count']}バケット")
        print(f"⏭️  スキップ: {summary['skipped_count']}バケット")

        if summary["current_bucket"]:
            print(f"\n🔄 処理中: {summary['current_bucket']}")

        if summary["completed_count"] > 0:
            print(f"\n📦 総オブジェクト数: {summary['total_objects']:,}")
            print(f"💾 元のサイズ: {summary['total_original_size']:,} bytes "
                  f"({summary['total_original_size'] / (1024**3):.2f} GB)")
            print(f"📦 圧縮後サイズ: {summary['total_compressed_size']:,} bytes "
                  f"({summary['total_compressed_size'] / (1024**3):.2f} GB)")

            if summary['total_original_size'] > 0:
                compression_ratio = (summary['total_compressed_size'] /
                                   summary['total_original_size']) * 100
                print(f"📉 圧縮率: {compression_ratio:.1f}%")

        print("=" * 70 + "\n")

    def reset_progress(self):
        """進行状況をリセット"""
        self.progress_data = {
            "version": "1.0.0",
            "start_time": None,
            "last_updated": None,
            "current_bucket": None,
            "completed_buckets": [],
            "failed_buckets": [],
            "skipped_buckets": []
        }
        self._save_progress()
        self.logger.info("進行状況をリセットしました")

    def export_report(self, output_file: str):
        """
        詳細レポートをファイルに出力

        Args:
            output_file: 出力ファイルパス
        """
        try:
            summary = self.get_summary()

            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("# S3 to Dropbox 移行レポート\n\n")
                f.write(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

                f.write("## サマリー\n\n")
                f.write(f"- 開始時刻: {summary.get('start_time', 'N/A')}\n")
                f.write(f"- 最終更新: {summary.get('last_updated', 'N/A')}\n")
                f.write(f"- 完了: {summary['completed_count']}バケット\n")
                f.write(f"- 失敗: {summary['failed_count']}バケット\n")
                f.write(f"- スキップ: {summary['skipped_count']}バケット\n\n")

                # 完了したバケット
                if self.progress_data["completed_buckets"]:
                    f.write("## 完了したバケット\n\n")
                    f.write("| バケット名 | オブジェクト数 | 元のサイズ | 圧縮後サイズ | 完了日時 |\n")
                    f.write("|-----------|--------------|-----------|------------|----------|\n")

                    for bucket in self.progress_data["completed_buckets"]:
                        name = bucket["bucket_name"]
                        obj_count = bucket.get("object_count", 0)
                        orig_size = bucket.get("original_size", 0)
                        comp_size = bucket.get("compressed_size", 0)
                        completed_at = bucket.get("completed_at", "N/A")

                        f.write(f"| {name} | {obj_count:,} | {orig_size:,} | "
                               f"{comp_size:,} | {completed_at} |\n")

                    f.write("\n")

                # 失敗したバケット
                if self.progress_data["failed_buckets"]:
                    f.write("## 失敗したバケット\n\n")
                    f.write("| バケット名 | エラー | 失敗日時 |\n")
                    f.write("|-----------|--------|----------|\n")

                    for bucket in self.progress_data["failed_buckets"]:
                        name = bucket["bucket_name"]
                        error = bucket.get("error", "N/A")
                        failed_at = bucket.get("failed_at", "N/A")

                        f.write(f"| {name} | {error} | {failed_at} |\n")

                    f.write("\n")

            self.logger.info(f"レポートを出力しました: {output_file}")

        except Exception as e:
            self.logger.error(f"レポート出力に失敗: {str(e)}")
