#!/usr/bin/env python3
"""
ツール2: データ移行ツール（S3 → Dropbox）

S3バケットの全データをDropboxに安全に移行します。
"""

import sys
import os
import shutil
import argparse
from datetime import datetime
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.logger import setup_logger, log_exception
from lib.aws_client import AWSClient
from lib.dropbox_client import DropboxClient
from lib.compressor import Compressor
from lib.progress import ProgressManager
from lib.file_list import FileListGenerator
from dotenv import load_dotenv
import humanize
from tqdm import tqdm


def confirm_migration(bucket_count: int, total_size: int, estimated_time: float) -> bool:
    """
    移行の確認をユーザーに求める

    Args:
        bucket_count: バケット数
        total_size: 総サイズ（バイト）
        estimated_time: 推定時間（時間）

    Returns:
        bool: ユーザーが承認した場合True
    """
    print("\n" + "=" * 80)
    print("⚠️  データ移行を開始する前に、以下の内容を確認してください")
    print("=" * 80)
    print(f"\n📦 移行対象バケット数: {bucket_count}個")
    print(f"💾 総データ量: {humanize.naturalsize(total_size, binary=True)}")
    print(f"⏱️  推定所要時間: 約{estimated_time:.1f}時間")
    print(f"\n⚠️  注意事項:")
    print("  - 移行中はインターネット接続を維持してください")
    print("  - 大量のデータをダウンロード・アップロードします")
    print("  - 一時ファイル用のディスク容量が必要です")
    print("  - 処理が完了するまで時間がかかる場合があります")
    print("\n" + "=" * 80)

    response = input("\n移行を開始しますか？ (yes/no): ").strip().lower()
    return response == 'yes'


def cleanup_temp_files(temp_dir: str, logger):
    """
    一時ファイルをクリーンアップ

    Args:
        temp_dir: 一時ディレクトリ
        logger: ロガー
    """
    try:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            logger.info(f"一時ファイルを削除しました: {temp_dir}")
    except Exception as e:
        log_exception(logger, "一時ファイルの削除に失敗", e)


def migrate_bucket(bucket_name: str, aws_client: AWSClient, dropbox_client: DropboxClient,
                  compressor: Compressor, file_list_gen: FileListGenerator,
                  temp_dir: str, dropbox_base_path: str, compression_format: str,
                  split_size: int, logger) -> dict:
    """
    単一のバケットを移行

    Args:
        bucket_name: バケット名
        aws_client: AWSクライアント
        dropbox_client: Dropboxクライアント
        compressor: 圧縮機能
        file_list_gen: ファイルリスト生成機能
        temp_dir: 一時ディレクトリ
        dropbox_base_path: Dropbox保存先ベースパス
        compression_format: 圧縮形式
        split_size: 分割サイズ
        logger: ロガー

    Returns:
        dict: 移行結果情報
    """
    result = {
        'bucket_name': bucket_name,
        'success': False,
        'object_count': 0,
        'original_size': 0,
        'compressed_size': 0,
        'split_count': 0,
        'error': None
    }

    bucket_temp_dir = os.path.join(temp_dir, bucket_name)
    bucket_dropbox_path = f"{dropbox_base_path}/{bucket_name}"

    try:
        logger.info(f"=" * 80)
        logger.info(f"バケット移行開始: {bucket_name}")
        logger.info(f"=" * 80)

        # ステップ1: バケット情報の取得
        print(f"\n📊 バケット情報を取得中...")
        region = aws_client.get_bucket_region(bucket_name)
        size, count = aws_client.get_bucket_size_and_count(bucket_name)

        result['object_count'] = count
        result['original_size'] = size

        print(f"  リージョン: {region}")
        print(f"  オブジェクト数: {count:,}")
        print(f"  サイズ: {humanize.naturalsize(size, binary=True)}")

        # ディスク容量チェック
        print(f"\n💾 ディスク容量を確認中...")
        required_space = size * 2  # 元データ + 圧縮データ
        is_sufficient, available = compressor.check_disk_space(temp_dir, required_space)

        if not is_sufficient:
            raise Exception(
                f"ディスク容量が不足しています。"
                f"必要: {humanize.naturalsize(required_space, binary=True)}, "
                f"利用可能: {humanize.naturalsize(available, binary=True)}"
            )

        print(f"  ✅ 十分な空き容量があります "
              f"({humanize.naturalsize(available, binary=True)})")

        # ステップ2: S3からダウンロード
        print(f"\n⬇️  S3からダウンロード中...")
        os.makedirs(bucket_temp_dir, exist_ok=True)

        downloaded_files = 0
        with tqdm(total=size, unit='B', unit_scale=True, desc="  ダウンロード") as pbar:
            def download_progress(key, file_size):
                nonlocal downloaded_files
                downloaded_files += 1
                pbar.update(file_size)
                pbar.set_postfix({'ファイル': f'{downloaded_files}/{count}'}, refresh=False)

            success = aws_client.download_bucket(
                bucket_name,
                bucket_temp_dir,
                progress_callback=download_progress
            )

        if not success:
            raise Exception("S3からのダウンロードに失敗しました")

        print(f"  ✅ ダウンロード完了: {downloaded_files}ファイル")

        # ステップ3: ファイルリスト生成
        print(f"\n📝 ファイルリストを生成中...")
        file_list_path = os.path.join(bucket_temp_dir, "file_list.md")
        file_list_gen.generate_file_list_md(bucket_temp_dir, bucket_name, file_list_path)
        print(f"  ✅ ファイルリスト生成完了")

        # ステップ4: データを圧縮
        print(f"\n🗜️  データを圧縮中...")
        output_base = os.path.join(temp_dir, f"{bucket_name}_backup")

        def compress_progress(current, total):
            if total > 0:
                percent = (current / total) * 100
                print(f"  進捗: {current}/{total} ({percent:.1f}%)", end='\r')

        compressed_files, compressed_size = compressor.compress_directory(
            bucket_temp_dir,
            output_base,
            compression_format=compression_format,
            split_size=split_size,
            progress_callback=compress_progress
        )

        result['compressed_size'] = compressed_size
        result['split_count'] = len(compressed_files)

        print(f"\n  ✅ 圧縮完了: {humanize.naturalsize(compressed_size, binary=True)}")
        if len(compressed_files) > 1:
            print(f"  📦 {len(compressed_files)}個のファイルに分割されました")

        # 圧縮ファイルの検証
        print(f"\n🔍 圧縮ファイルを検証中...")
        if len(compressed_files) == 1:
            if not compressor.verify_archive(compressed_files[0], compression_format):
                raise Exception("圧縮ファイルの整合性チェックに失敗しました")
        print(f"  ✅ 整合性チェック完了")

        # ステップ5: README生成
        print(f"\n📄 READMEを生成中...")
        readme_path = os.path.join(temp_dir, f"{bucket_name}_README.md")

        bucket_info = {
            'region': region,
            'object_count': count,
            'original_size': size
        }

        compression_info = {
            'format': compression_format,
            'files': compressed_files,
            'total_size': compressed_size
        }

        file_list_gen.generate_readme_md(
            bucket_name,
            readme_path,
            bucket_info,
            compression_info
        )
        print(f"  ✅ README生成完了")

        # ステップ6: Dropboxにアップロード
        print(f"\n⬆️  Dropboxにアップロード中...")

        # Dropboxフォルダ作成
        dropbox_client.create_folder(dropbox_base_path)
        dropbox_client.create_folder(bucket_dropbox_path)

        # 圧縮ファイルをアップロード
        for i, compressed_file in enumerate(compressed_files, 1):
            file_name = os.path.basename(compressed_file)
            dropbox_path = f"{bucket_dropbox_path}/{file_name}"

            print(f"\n  [{i}/{len(compressed_files)}] {file_name}")

            file_size = os.path.getsize(compressed_file)

            with tqdm(total=file_size, unit='B', unit_scale=True,
                     desc=f"    アップロード") as pbar:

                def upload_progress(uploaded, total):
                    pbar.n = uploaded
                    pbar.refresh()

                success = dropbox_client.upload_file(
                    compressed_file,
                    dropbox_path,
                    progress_callback=upload_progress
                )

                if not success:
                    raise Exception(f"ファイルのアップロードに失敗: {file_name}")

        # file_list.mdをアップロード
        print(f"\n  📝 file_list.mdをアップロード中...")
        dropbox_client.upload_file(
            file_list_path,
            f"{bucket_dropbox_path}/file_list.md"
        )

        # READMEをアップロード
        print(f"  📄 READMEをアップロード中...")
        dropbox_client.upload_file(
            readme_path,
            f"{bucket_dropbox_path}/README.md"
        )

        print(f"\n  ✅ Dropboxアップロード完了")

        # ステップ7: 一時ファイルのクリーンアップ
        print(f"\n🧹 一時ファイルを削除中...")
        if os.path.exists(bucket_temp_dir):
            shutil.rmtree(bucket_temp_dir)
        for compressed_file in compressed_files:
            if os.path.exists(compressed_file):
                os.remove(compressed_file)
        if os.path.exists(readme_path):
            os.remove(readme_path)

        print(f"  ✅ クリーンアップ完了")

        result['success'] = True
        logger.info(f"バケット移行完了: {bucket_name}")

        print(f"\n✅ バケット {bucket_name} の移行が完了しました！")
        print(f"  Dropbox保存先: {bucket_dropbox_path}")

    except Exception as e:
        result['error'] = str(e)
        log_exception(logger, f"バケット {bucket_name} の移行に失敗", e)
        print(f"\n❌ エラーが発生しました: {str(e)}")

        # エラー時のクリーンアップ
        try:
            if os.path.exists(bucket_temp_dir):
                shutil.rmtree(bucket_temp_dir)
        except:
            pass

    return result


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description='S3バケットをDropboxに移行します',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # 全バケットを移行
  python tools/migrate_data.py

  # 特定のバケットのみ移行
  python tools/migrate_data.py --buckets bucket1 bucket2

  # 進行状況をリセット
  python tools/migrate_data.py --reset
        """
    )

    parser.add_argument('--profile', type=str, help='AWSプロファイル名')
    parser.add_argument('--buckets', nargs='+', help='移行するバケット名（指定しない場合は全バケット）')
    parser.add_argument('--reset', action='store_true', help='進行状況をリセット')
    parser.add_argument('--log-level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='ログレベル（デフォルト: INFO）')

    args = parser.parse_args()

    # 環境変数の読み込み
    load_dotenv()

    # ロガーの設定
    logger = setup_logger('migrate_data', args.log_level)

    try:
        print("\n🚀 S3 → Dropbox データ移行ツール")
        print("=" * 80)

        # 設定読み込み
        dropbox_token = os.getenv('DROPBOX_ACCESS_TOKEN')
        dropbox_base_path = os.getenv('DROPBOX_BACKUP_PATH', '/S3_Backup')
        temp_dir = os.getenv('TEMP_DIR', './temp')
        compression_format = os.getenv('COMPRESSION_FORMAT', 'zip')
        split_size = int(os.getenv('SPLIT_SIZE', str(10 * 1024 * 1024 * 1024)))

        # 進行状況管理の初期化
        progress_mgr = ProgressManager(logger=logger)

        # リセット処理
        if args.reset:
            print("\n⚠️  進行状況をリセットします。")
            confirm = input("本当にリセットしますか？ (yes/no): ").strip().lower()
            if confirm == 'yes':
                progress_mgr.reset_progress()
                print("✅ 進行状況をリセットしました。")
            return

        # 進行状況サマリーの表示
        progress_mgr.print_summary()

        # AWSクライアントの初期化
        logger.info("AWSクライアントを初期化しています...")
        aws_client = AWSClient(profile_name=args.profile, logger=logger)

        # Dropboxクライアントの初期化
        logger.info("Dropboxクライアントを初期化しています...")
        dropbox_client = DropboxClient(access_token=dropbox_token, logger=logger)

        # その他のクライアントを初期化
        compressor = Compressor(logger=logger)
        file_list_gen = FileListGenerator(logger=logger)

        # バケットリストの取得
        all_buckets = aws_client.list_buckets()
        bucket_names = [b['Name'] for b in all_buckets]

        # 移行対象のバケットを決定
        if args.buckets:
            target_buckets = [b for b in args.buckets if b in bucket_names]
            if len(target_buckets) != len(args.buckets):
                missing = set(args.buckets) - set(target_buckets)
                logger.warning(f"以下のバケットが見つかりませんでした: {missing}")
        else:
            target_buckets = bucket_names

        # 未処理のバケットを取得
        pending_buckets = progress_mgr.get_pending_buckets(target_buckets)

        if not pending_buckets:
            print("\n✅ 全てのバケットの移行が完了しています。")
            return

        print(f"\n📦 移行対象: {len(pending_buckets)}バケット")

        # 推定時間計算（簡易版）
        estimated_hours = len(pending_buckets) * 0.5  # バケットあたり30分と仮定

        # 確認プロンプト
        total_size = sum(aws_client.get_bucket_size_and_count(b)[0] for b in pending_buckets[:5])
        if not confirm_migration(len(pending_buckets), total_size, estimated_hours):
            print("\n⚠️  移行がキャンセルされました。")
            return

        # 移行開始
        progress_mgr.start_migration()

        # 各バケットを移行
        for i, bucket_name in enumerate(pending_buckets, 1):
            print(f"\n{'=' * 80}")
            print(f"🔄 [{i}/{len(pending_buckets)}] {bucket_name}")
            print(f"{'=' * 80}")

            progress_mgr.set_current_bucket(bucket_name)

            result = migrate_bucket(
                bucket_name,
                aws_client,
                dropbox_client,
                compressor,
                file_list_gen,
                temp_dir,
                dropbox_base_path,
                compression_format,
                split_size,
                logger
            )

            if result['success']:
                progress_mgr.mark_bucket_completed(bucket_name, {
                    'object_count': result['object_count'],
                    'original_size': result['original_size'],
                    'compressed_size': result['compressed_size'],
                    'split_count': result['split_count']
                })
            else:
                progress_mgr.mark_bucket_failed(bucket_name, result['error'])

        # 最終サマリー
        print("\n" + "=" * 80)
        print("🎉 全ての移行処理が完了しました！")
        print("=" * 80)

        progress_mgr.print_summary()

        # レポート出力
        report_path = f"data/migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        progress_mgr.export_report(report_path)
        print(f"\n📊 詳細レポート: {report_path}")

        logger.info("データ移行が完了しました")

    except KeyboardInterrupt:
        print("\n\n⚠️  処理が中断されました。")
        print("進行状況は保存されています。次回実行時に続きから再開できます。")
        logger.warning("ユーザーによって処理が中断されました")
        sys.exit(1)
    except Exception as e:
        log_exception(logger, "予期しないエラーが発生しました", e)
        print(f"\n❌ エラーが発生しました: {str(e)}")
        print("詳細はログファイルを確認してください。")
        sys.exit(1)


if __name__ == '__main__':
    main()
