#!/usr/bin/env python3
"""
ツール3: バケット削除ツール

Dropboxへの移行が完了したS3バケットを安全に削除します。
"""

import sys
import os
import argparse
import json
from datetime import datetime
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.logger import setup_logger, log_exception
from lib.aws_client import AWSClient
from lib.progress import ProgressManager
from dotenv import load_dotenv
import humanize
from tqdm import tqdm


def print_deletion_preview(buckets_to_delete: list):
    """
    削除予定のバケット情報を表示

    Args:
        buckets_to_delete: 削除予定バケット情報のリスト
    """
    print("\n" + "=" * 120)
    print("🗑️  削除予定のバケット")
    print("=" * 120)
    print(f"{'No.':<5} {'バケット名':<40} {'オブジェクト数':>12} {'元のサイズ':>15} "
          f"{'圧縮後':>15} {'完了日時':<20}")
    print("=" * 120)

    for i, bucket in enumerate(buckets_to_delete, 1):
        name = bucket.get('bucket_name', 'N/A')
        obj_count = bucket.get('object_count', 0)
        orig_size = bucket.get('original_size', 0)
        comp_size = bucket.get('compressed_size', 0)
        completed_at = bucket.get('completed_at', 'N/A')[:19]  # 日時部分のみ

        orig_size_str = humanize.naturalsize(orig_size, binary=True)
        comp_size_str = humanize.naturalsize(comp_size, binary=True)

        print(f"{i:<5} {name:<40} {obj_count:>12,} {orig_size_str:>15} "
              f"{comp_size_str:>15} {completed_at:<20}")

    print("=" * 120)


def confirm_deletion(bucket_count: int, is_dry_run: bool) -> bool:
    """
    削除の確認をユーザーに求める

    Args:
        bucket_count: バケット数
        is_dry_run: ドライランモードの場合True

    Returns:
        bool: ユーザーが承認した場合True
    """
    if is_dry_run:
        print("\n💡 これはドライランです。実際には削除されません。")
        print("   本当に削除する場合は --delete オプションを指定してください。")
        return True

    print("\n" + "=" * 80)
    print("⚠️  ⚠️  ⚠️   重要な警告   ⚠️  ⚠️  ⚠️")
    print("=" * 80)
    print(f"\n{bucket_count}個のS3バケットを完全に削除しようとしています。")
    print("\n⚠️  この操作は取り消せません！")
    print("⚠️  削除する前に、必ずDropboxでバックアップを確認してください！")
    print("\n削除前のチェックリスト:")
    print("  □ Dropboxにバックアップが正しく保存されていることを確認しましたか？")
    print("  □ バックアップファイル（圧縮ファイル、README、file_list.md）が全て揃っていますか？")
    print("  □ 分割ファイルがある場合、全ての分割ファイルが存在しますか？")
    print("  □ 本当にこれらのバケットを削除してよいですか？")
    print("\n" + "=" * 80)

    print("\n本当に削除を実行しますか？")
    print("実行する場合は 'yes' と正確に入力してください（他の入力ではキャンセルされます）")

    response = input("\n入力: ").strip()

    if response == 'yes':
        print("\n最終確認: もう一度 'DELETE' と入力してください")
        final_response = input("\n入力: ").strip()
        return final_response == 'DELETE'

    return False


def delete_bucket_with_progress(aws_client: AWSClient, bucket_name: str, logger) -> dict:
    """
    バケットを削除（プログレス表示付き）

    Args:
        aws_client: AWSクライアント
        bucket_name: バケット名
        logger: ロガー

    Returns:
        dict: 削除結果
    """
    result = {
        'bucket_name': bucket_name,
        'success': False,
        'deleted_objects': 0,
        'error': None
    }

    try:
        print(f"\n🗑️  バケット削除中: {bucket_name}")

        # オブジェクト数を取得
        _, object_count = aws_client.get_bucket_size_and_count(bucket_name)

        if object_count > 0:
            print(f"  オブジェクト削除中... ({object_count:,}個)")

        # バケットを削除
        success = aws_client.delete_bucket(bucket_name, delete_versions=True)

        if success:
            result['success'] = True
            result['deleted_objects'] = object_count
            print(f"  ✅ バケット削除完了: {bucket_name}")
        else:
            result['error'] = "削除に失敗しました"
            print(f"  ❌ バケット削除失敗: {bucket_name}")

    except Exception as e:
        result['error'] = str(e)
        log_exception(logger, f"バケット削除エラー: {bucket_name}", e)
        print(f"  ❌ エラー: {str(e)}")

    return result


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description='移行完了したS3バケットを削除します',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # ドライラン（削除予定を表示のみ）
  python tools/delete_buckets.py

  # 実際に削除を実行
  python tools/delete_buckets.py --delete

  # 特定のバケットのみ削除
  python tools/delete_buckets.py --delete --buckets bucket1 bucket2
        """
    )

    parser.add_argument('--profile', type=str, help='AWSプロファイル名')
    parser.add_argument('--delete', action='store_true',
                       help='実際に削除を実行（指定しない場合はドライラン）')
    parser.add_argument('--buckets', nargs='+',
                       help='削除するバケット名（指定しない場合は移行完了した全バケット）')
    parser.add_argument('--log-level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='ログレベル（デフォルト: INFO）')

    args = parser.parse_args()

    # 環境変数の読み込み
    load_dotenv()

    # ロガーの設定
    logger = setup_logger('delete_buckets', args.log_level)

    try:
        mode = "🗑️  削除実行モード" if args.delete else "👁️  ドライランモード"
        print(f"\n{mode}")
        print("=" * 80)

        # 進行状況管理の初期化
        progress_mgr = ProgressManager(logger=logger)

        # 完了済みバケットを取得
        completed_buckets = progress_mgr.get_completed_buckets()

        if not completed_buckets:
            print("\n⚠️  移行完了したバケットがありません。")
            print("先に migrate_data.py を実行してバケットを移行してください。")
            return

        # 削除対象のバケットを決定
        if args.buckets:
            completed_names = [b['bucket_name'] for b in completed_buckets]
            buckets_to_delete = [
                b for b in completed_buckets
                if b['bucket_name'] in args.buckets
            ]

            if len(buckets_to_delete) != len(args.buckets):
                found = {b['bucket_name'] for b in buckets_to_delete}
                missing = set(args.buckets) - found
                print(f"\n⚠️  以下のバケットは移行完了していないため削除できません: {missing}")

                if not buckets_to_delete:
                    return
        else:
            buckets_to_delete = completed_buckets

        # 削除予定の表示
        print_deletion_preview(buckets_to_delete)

        # サマリー
        total_objects = sum(b.get('object_count', 0) for b in buckets_to_delete)
        total_original_size = sum(b.get('original_size', 0) for b in buckets_to_delete)

        print(f"\n📊 サマリー")
        print(f"  削除対象バケット数: {len(buckets_to_delete)}個")
        print(f"  総オブジェクト数: {total_objects:,}")
        print(f"  総サイズ: {humanize.naturalsize(total_original_size, binary=True)}")

        if not args.delete:
            print("\n💡 本当に削除する場合は、--delete オプションを指定して再実行してください。")
            print("   例: python tools/delete_buckets.py --delete")
            return

        # 削除確認
        if not confirm_deletion(len(buckets_to_delete), args.delete is False):
            print("\n⚠️  削除がキャンセルされました。")
            return

        # AWSクライアントの初期化
        logger.info("AWSクライアントを初期化しています...")
        aws_client = AWSClient(profile_name=args.profile, logger=logger)

        print("\n" + "=" * 80)
        print("🗑️  削除を開始します...")
        print("=" * 80)

        # 削除ログ
        deletion_log = {
            'timestamp': datetime.now().isoformat(),
            'deleted': [],
            'failed': []
        }

        # 各バケットを削除
        for i, bucket_info in enumerate(buckets_to_delete, 1):
            bucket_name = bucket_info['bucket_name']

            print(f"\n[{i}/{len(buckets_to_delete)}] {bucket_name}")

            result = delete_bucket_with_progress(aws_client, bucket_name, logger)

            if result['success']:
                deletion_log['deleted'].append({
                    'bucket_name': bucket_name,
                    'deleted_at': datetime.now().isoformat(),
                    'object_count': result['deleted_objects'],
                    'original_info': bucket_info
                })
            else:
                deletion_log['failed'].append({
                    'bucket_name': bucket_name,
                    'error': result['error'],
                    'attempted_at': datetime.now().isoformat()
                })

        # 最終レポート
        print("\n" + "=" * 80)
        print("📊 削除結果")
        print("=" * 80)

        success_count = len(deletion_log['deleted'])
        failed_count = len(deletion_log['failed'])

        print(f"\n✅ 成功: {success_count}バケット")
        print(f"❌ 失敗: {failed_count}バケット")

        if deletion_log['deleted']:
            print(f"\n✅ 削除されたバケット:")
            for item in deletion_log['deleted']:
                print(f"  - {item['bucket_name']}")

        if deletion_log['failed']:
            print(f"\n❌ 削除に失敗したバケット:")
            for item in deletion_log['failed']:
                print(f"  - {item['bucket_name']}: {item['error']}")

        # 削除ログを保存
        log_path = f"data/deletion_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs('data', exist_ok=True)

        with open(log_path, 'w', encoding='utf-8') as f:
            json.dump(deletion_log, f, ensure_ascii=False, indent=2)

        print(f"\n💾 削除ログを保存しました: {log_path}")

        print("\n" + "=" * 80)
        if failed_count == 0:
            print("🎉 全てのバケットの削除が完了しました！")
        else:
            print("⚠️  一部のバケットの削除に失敗しました。ログを確認してください。")
        print("=" * 80)

        logger.info("バケット削除処理が完了しました")

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
