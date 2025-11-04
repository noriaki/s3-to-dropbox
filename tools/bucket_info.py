#!/usr/bin/env python3
"""
ツール1: S3バケット情報確認ツール

全S3バケットのサイズ、オブジェクト数、リージョン情報を収集・表示します。
"""

import sys
import os
import json
import argparse
from datetime import datetime
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.logger import setup_logger, log_exception
from lib.aws_client import AWSClient
from dotenv import load_dotenv
import humanize
from tqdm import tqdm


def format_size(size_bytes: int) -> str:
    """バイト数を人間が読みやすい形式に変換"""
    return humanize.naturalsize(size_bytes, binary=True)


def print_table_header():
    """テーブルヘッダーを表示"""
    print("\n" + "=" * 120)
    print(f"{'No.':<5} {'バケット名':<40} {'リージョン':<20} {'作成日':<12} "
          f"{'オブジェクト数':>12} {'サイズ':>15}")
    print("=" * 120)


def print_bucket_row(index: int, bucket_name: str, region: str, created_date: str,
                    object_count: int, size: int):
    """バケット情報の行を表示"""
    size_str = format_size(size)
    print(f"{index:<5} {bucket_name:<40} {region:<20} {created_date:<12} "
          f"{object_count:>12,} {size_str:>15}")


def main():
    """メイン処理"""
    # 引数パーサー
    parser = argparse.ArgumentParser(
        description='S3バケットの情報を収集・表示します',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # 全バケットの情報を表示
  python tools/bucket_info.py

  # 特定のAWSプロファイルを使用
  python tools/bucket_info.py --profile myprofile

  # 詳細ログを出力
  python tools/bucket_info.py --log-level DEBUG
        """
    )

    parser.add_argument('--profile', type=str, help='AWSプロファイル名')
    parser.add_argument('--log-level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='ログレベル（デフォルト: INFO）')
    parser.add_argument('--output', type=str, default='data/bucket_info.json',
                       help='出力JSONファイルパス（デフォルト: data/bucket_info.json）')

    args = parser.parse_args()

    # 環境変数の読み込み
    load_dotenv()

    # ロガーの設定
    logger = setup_logger('bucket_info', args.log_level)

    try:
        print("\n🔍 S3バケット情報確認ツール")
        print("=" * 120)

        # AWSクライアントの初期化
        logger.info("AWSクライアントを初期化しています...")
        aws_client = AWSClient(profile_name=args.profile, logger=logger)

        # バケットリストの取得
        logger.info("バケットリストを取得しています...")
        buckets = aws_client.list_buckets()

        if not buckets:
            print("\n⚠️  バケットが見つかりませんでした。")
            return

        print(f"\n📦 {len(buckets)}個のバケットを検出しました。情報を収集中...\n")

        # バケット情報の収集
        bucket_info_list = []
        total_size = 0
        total_objects = 0
        large_buckets = 0  # 10GB超えるバケットの数
        split_threshold = 10 * 1024 * 1024 * 1024  # 10GB

        # プログレスバー
        with tqdm(total=len(buckets), desc="バケット情報収集", unit="bucket") as pbar:
            for bucket in buckets:
                bucket_name = bucket['Name']
                created_date = bucket['CreationDate'].strftime('%Y-%m-%d')

                pbar.set_description(f"処理中: {bucket_name[:30]}")

                try:
                    # リージョン取得
                    region = aws_client.get_bucket_region(bucket_name)

                    # バージョン管理状態
                    versioning = aws_client.get_bucket_versioning(bucket_name)

                    # サイズとオブジェクト数
                    size, count = aws_client.get_bucket_size_and_count(bucket_name)

                    bucket_info = {
                        'name': bucket_name,
                        'region': region,
                        'created_date': created_date,
                        'object_count': count,
                        'size_bytes': size,
                        'size_human': format_size(size),
                        'versioning_enabled': versioning
                    }

                    bucket_info_list.append(bucket_info)

                    total_size += size
                    total_objects += count

                    if size > split_threshold:
                        large_buckets += 1

                except Exception as e:
                    log_exception(logger, f"バケット {bucket_name} の情報取得に失敗", e)
                    bucket_info_list.append({
                        'name': bucket_name,
                        'region': 'unknown',
                        'created_date': created_date,
                        'object_count': 0,
                        'size_bytes': 0,
                        'size_human': 'N/A',
                        'versioning_enabled': False,
                        'error': str(e)
                    })

                pbar.update(1)

        # サイズ順にソート
        bucket_info_list.sort(key=lambda x: x['size_bytes'], reverse=True)

        # テーブル形式で表示
        print_table_header()

        for i, info in enumerate(bucket_info_list, 1):
            print_bucket_row(
                i,
                info['name'],
                info['region'],
                info['created_date'],
                info['object_count'],
                info['size_bytes']
            )

        print("=" * 120)

        # サマリー表示
        print("\n📊 サマリー")
        print("=" * 120)
        print(f"総バケット数:         {len(bucket_info_list):,}")
        print(f"総オブジェクト数:     {total_objects:,}")
        print(f"総データ量:           {format_size(total_size)} ({total_size:,} bytes)")

        # 圧縮率を仮定して推定
        estimated_compressed = int(total_size * 0.7)  # 70%に圧縮されると仮定
        print(f"圧縮後推定サイズ:     {format_size(estimated_compressed)} （圧縮率70%と仮定）")

        # Dropbox容量との比較
        dropbox_available = 1.5 * 1024 * 1024 * 1024 * 1024  # 1.5TB
        print(f"Dropbox空き容量:      {format_size(dropbox_available)}")

        if estimated_compressed <= dropbox_available:
            print(f"✅ Dropbox容量は十分です（余裕: {format_size(dropbox_available - estimated_compressed)}）")
        else:
            shortage = estimated_compressed - dropbox_available
            print(f"⚠️  Dropbox容量が不足する可能性があります（不足: {format_size(shortage)}）")

        print(f"\n10GB超のバケット:     {large_buckets}個 （分割が必要）")

        print("=" * 120)

        # JSON出力
        output_data = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_buckets': len(bucket_info_list),
                'total_objects': total_objects,
                'total_size_bytes': total_size,
                'total_size_human': format_size(total_size),
                'estimated_compressed_bytes': estimated_compressed,
                'estimated_compressed_human': format_size(estimated_compressed),
                'large_buckets_count': large_buckets
            },
            'buckets': bucket_info_list
        }

        # 出力ディレクトリの作成
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        print(f"\n💾 詳細情報をJSONファイルに保存しました: {output_path}")

        logger.info("バケット情報の収集が完了しました")

    except KeyboardInterrupt:
        print("\n\n⚠️  処理が中断されました。")
        logger.warning("ユーザーによって処理が中断されました")
        sys.exit(1)
    except Exception as e:
        log_exception(logger, "予期しないエラーが発生しました", e)
        print(f"\n❌ エラーが発生しました: {str(e)}")
        print("詳細はログファイルを確認してください。")
        sys.exit(1)


if __name__ == '__main__':
    main()
