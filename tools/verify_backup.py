#!/usr/bin/env python3
"""
ツール3: Dropboxバックアップ検証ツール

Dropboxにアップロードされたバックアップファイルの整合性を検証します。
"""

import sys
import os
import json
import argparse
import random
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.logger import setup_logger, log_exception
from lib.aws_client import AWSClient
from lib.dropbox_client import DropboxClient
from lib.compressor import Compressor
from dotenv import load_dotenv
import humanize
from tqdm import tqdm


def select_distributed_sample(items: List[tuple], sample_count: int) -> List[tuple]:
    """
    サイズに基づいて分散サンプリング

    Args:
        items: (name, size) のタプルリスト
        sample_count: サンプリング数

    Returns:
        サンプリングされたアイテムのリスト
    """
    if len(items) <= sample_count:
        return items

    # サイズでソート
    sorted_items = sorted(items, key=lambda x: x[1])

    # 小・中・大の3グループに分割
    third = len(sorted_items) // 3
    small = sorted_items[:third]
    medium = sorted_items[third:third*2]
    large = sorted_items[third*2:]

    # 各グループから均等にサンプリング
    samples_per_group = sample_count // 3
    remainder = sample_count % 3

    selected = []
    selected.extend(random.sample(small, min(samples_per_group + (1 if remainder > 0 else 0), len(small))))
    selected.extend(random.sample(medium, min(samples_per_group + (1 if remainder > 1 else 0), len(medium))))
    selected.extend(random.sample(large, min(samples_per_group, len(large))))

    # 不足分をランダムに追加
    if len(selected) < sample_count:
        remaining = [item for item in sorted_items if item not in selected]
        additional = random.sample(remaining, min(sample_count - len(selected), len(remaining)))
        selected.extend(additional)

    return selected


def get_compression_format_from_filename(filename: str) -> str:
    """
    ファイル名から圧縮形式を判定

    Args:
        filename: ファイル名

    Returns:
        圧縮形式（'zip' または 'tar.gz'）
    """
    if '.tar.gz' in filename:
        return 'tar.gz'
    elif '.zip' in filename:
        return 'zip'
    else:
        return 'zip'  # デフォルト


def verify_bucket_lists(aws_client: AWSClient, dropbox_client: DropboxClient,
                       dropbox_base_path: str, logger) -> Dict:
    """
    S3とDropboxのバケットリストを比較検証

    Args:
        aws_client: AWSクライアント
        dropbox_client: Dropboxクライアント
        dropbox_base_path: Dropboxベースパス
        logger: ロガー

    Returns:
        検証結果の辞書
    """
    result = {
        's3_bucket_count': 0,
        'dropbox_bucket_count': 0,
        's3_buckets': [],
        'dropbox_buckets': [],
        'missing_in_dropbox': [],  # S3にあるがDropboxにない
        'extra_in_dropbox': [],    # DropboxにあるがS3にない
        'match': False
    }

    try:
        print(f"\n{'='*80}")
        print(f"📊 Step 1: バケットリスト整合性チェック")
        print(f"{'='*80}")

        # S3バケットリスト取得
        print(f"\n📦 S3バケットリストを取得中...")
        s3_buckets = aws_client.list_buckets()
        s3_bucket_names = set([b['Name'] for b in s3_buckets])
        result['s3_bucket_count'] = len(s3_bucket_names)
        result['s3_buckets'] = sorted(list(s3_bucket_names))
        print(f"  ✓ S3バケット数: {len(s3_bucket_names)}")

        # Dropboxバケットリスト取得
        print(f"\n📦 Dropboxバケットリストを取得中...")
        dropbox_entries = dropbox_client.list_folder(dropbox_base_path)
        dropbox_bucket_names = set([e.name for e in dropbox_entries if hasattr(e, 'name')])
        result['dropbox_bucket_count'] = len(dropbox_bucket_names)
        result['dropbox_buckets'] = sorted(list(dropbox_bucket_names))
        print(f"  ✓ Dropboxバケット数: {len(dropbox_bucket_names)}")

        # 差分検出
        print(f"\n🔍 バケット名を照合中...")
        missing_in_dropbox = s3_bucket_names - dropbox_bucket_names
        extra_in_dropbox = dropbox_bucket_names - s3_bucket_names

        result['missing_in_dropbox'] = sorted(list(missing_in_dropbox))
        result['extra_in_dropbox'] = sorted(list(extra_in_dropbox))
        result['match'] = (len(missing_in_dropbox) == 0 and len(extra_in_dropbox) == 0)

        # 結果表示
        print(f"\n{'='*80}")
        if result['match']:
            print(f"✅ バケット数: 一致 ({len(s3_bucket_names)}個)")
            print(f"✅ バケット名: 全て一致")
        else:
            print(f"❌ バケット数: S3={len(s3_bucket_names)}, Dropbox={len(dropbox_bucket_names)}")

            if missing_in_dropbox:
                print(f"\n⚠️  移行漏れ (S3にあるがDropboxにない): {len(missing_in_dropbox)}個")
                for bucket in sorted(missing_in_dropbox)[:10]:  # 最初の10個のみ表示
                    print(f"  - {bucket}")
                if len(missing_in_dropbox) > 10:
                    print(f"  ... 他 {len(missing_in_dropbox) - 10}個")

            if extra_in_dropbox:
                print(f"\n⚠️  余分なバケット (DropboxにあるがS3にない): {len(extra_in_dropbox)}個")
                for bucket in sorted(extra_in_dropbox)[:10]:  # 最初の10個のみ表示
                    print(f"  - {bucket}")
                if len(extra_in_dropbox) > 10:
                    print(f"  ... 他 {len(extra_in_dropbox) - 10}個")

        print(f"{'='*80}")

        logger.info(f"バケットリスト整合性チェック完了: 一致={result['match']}")

    except Exception as e:
        log_exception(logger, "バケットリスト整合性チェックに失敗", e)
        print(f"\n❌ エラー: {str(e)}")

    return result


def verify_bucket(bucket_name: str, dropbox_base_path: str,
                 aws_client: AWSClient, dropbox_client: DropboxClient,
                 compressor: Compressor, temp_dir: str,
                 file_sample_count: int, logger) -> Dict:
    """
    単一バケットの検証

    Args:
        bucket_name: バケット名
        dropbox_base_path: Dropboxベースパス
        aws_client: AWSクライアント
        dropbox_client: Dropboxクライアント
        compressor: 圧縮機能
        temp_dir: 一時ディレクトリ
        file_sample_count: サンプリングファイル数
        logger: ロガー

    Returns:
        検証結果の辞書
    """
    result = {
        'bucket_name': bucket_name,
        'success': False,
        'compressed_files': [],
        'sampled_files': [],
        'verified_count': 0,
        'mismatch_count': 0,
        'errors': []
    }

    bucket_dropbox_path = f"{dropbox_base_path}/{bucket_name}"
    bucket_temp_dir = os.path.join(temp_dir, f"verify_{bucket_name}")

    try:
        print(f"\n{'='*80}")
        print(f"📦 バケット検証: {bucket_name}")
        print(f"{'='*80}")

        # ステップ1: Dropboxからファイル一覧取得
        print(f"\n📋 Dropboxからファイル一覧を取得中...")
        entries = dropbox_client.list_folder(bucket_dropbox_path)

        if not entries:
            raise Exception(f"Dropboxにバケットが見つかりません: {bucket_dropbox_path}")

        # 圧縮ファイルを特定
        compressed_files = []
        for entry in entries:
            name = entry.name
            if name.endswith('.zip') or name.endswith('.tar.gz') or name.endswith('.001'):
                compressed_files.append(entry)

        if not compressed_files:
            raise Exception("圧縮ファイルが見つかりません")

        # 分割ファイルかどうか判定
        is_split = any(f.name.endswith('.001') for f in compressed_files)

        if is_split:
            # 分割ファイルをソート
            split_files = sorted([f for f in compressed_files if '.' in f.name.split('_backup')[-1]],
                               key=lambda x: x.name)
            print(f"  ✓ 分割ファイル検出: {len(split_files)}個")
            result['compressed_files'] = [f.name for f in split_files]
        else:
            # 単一ファイル
            archive_file = compressed_files[0]
            print(f"  ✓ 圧縮ファイル: {archive_file.name}")
            result['compressed_files'] = [archive_file.name]

        # ステップ2: ダウンロード
        print(f"\n⬇️  Dropboxからダウンロード中...")
        os.makedirs(bucket_temp_dir, exist_ok=True)

        downloaded_files = []

        if is_split:
            # 分割ファイルをダウンロード
            for i, split_file in enumerate(split_files, 1):
                local_path = os.path.join(bucket_temp_dir, split_file.name)
                file_size = split_file.size

                print(f"\n  [{i}/{len(split_files)}] {split_file.name}")
                with tqdm(total=file_size, unit='B', unit_scale=True, desc="    ダウンロード") as pbar:
                    def progress(downloaded, total):
                        pbar.n = downloaded
                        pbar.refresh()

                    success = dropbox_client.download_file(
                        f"{bucket_dropbox_path}/{split_file.name}",
                        local_path,
                        progress_callback=progress
                    )

                    if not success:
                        raise Exception(f"ダウンロード失敗: {split_file.name}")

                downloaded_files.append(local_path)

            # ステップ3: ファイル結合
            print(f"\n🔗 分割ファイルを結合中...")
            base_name = split_files[0].name.rsplit('.', 1)[0]  # .001を除去
            merged_path = os.path.join(bucket_temp_dir, base_name)

            with tqdm(total=len(downloaded_files), desc="  結合") as pbar:
                def merge_progress(current, total):
                    pbar.n = current
                    pbar.refresh()

                success = compressor.merge_split_files(
                    downloaded_files,
                    merged_path,
                    progress_callback=merge_progress
                )

                if not success:
                    raise Exception("ファイル結合に失敗")

            archive_path = merged_path
            compression_format = get_compression_format_from_filename(base_name)
        else:
            # 単一ファイルをダウンロード
            local_path = os.path.join(bucket_temp_dir, archive_file.name)
            file_size = archive_file.size

            with tqdm(total=file_size, unit='B', unit_scale=True, desc="  ダウンロード") as pbar:
                def progress(downloaded, total):
                    pbar.n = downloaded
                    pbar.refresh()

                success = dropbox_client.download_file(
                    f"{bucket_dropbox_path}/{archive_file.name}",
                    local_path,
                    progress_callback=progress
                )

                if not success:
                    raise Exception(f"ダウンロード失敗: {archive_file.name}")

            archive_path = local_path
            compression_format = get_compression_format_from_filename(archive_file.name)

        # ステップ4: 整合性チェック
        print(f"\n🔍 アーカイブ整合性チェック中...")
        if not compressor.verify_archive(archive_path, compression_format):
            raise Exception("整合性チェックに失敗")
        print(f"  ✓ 整合性OK")

        # ステップ5: 解凍
        print(f"\n📂 アーカイブを解凍中...")
        extract_dir = os.path.join(bucket_temp_dir, "extracted")

        with tqdm(desc="  解凍") as pbar:
            def extract_progress(current, total):
                pbar.total = total
                pbar.n = current
                pbar.refresh()

            success = compressor.extract_archive(
                archive_path,
                extract_dir,
                compression_format,
                progress_callback=extract_progress
            )

            if not success:
                raise Exception("解凍に失敗")

        # ステップ6: ファイルリスト取得
        print(f"\n📝 解凍されたファイルを収集中...")
        extracted_files = []
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                if file == 'file_list.md':
                    continue
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)
                # S3上の相対パスを計算
                rel_path = os.path.relpath(file_path, extract_dir)
                extracted_files.append((rel_path, file_size))

        print(f"  ✓ {len(extracted_files)}個のファイルを検出")

        # ステップ7: ファイルサンプリング
        print(f"\n🎲 ファイルをサンプリング中...")
        sampled_files = select_distributed_sample(extracted_files, file_sample_count)
        print(f"  ✓ {len(sampled_files)}個をサンプリング")

        # ステップ8: S3メタデータと突合
        print(f"\n🔄 S3メタデータと突合中...")

        verified = 0
        mismatches = []

        with tqdm(total=len(sampled_files), desc="  検証") as pbar:
            for rel_path, local_size in sampled_files:
                # S3オブジェクトキーを構築
                s3_key = rel_path.replace('\\', '/')  # Windowsパス対応

                try:
                    # S3からメタデータ取得
                    s3_metadata = aws_client.s3_client.head_object(
                        Bucket=bucket_name,
                        Key=s3_key
                    )
                    s3_size = s3_metadata['ContentLength']

                    # サイズ比較
                    match = (local_size == s3_size)

                    file_result = {
                        'path': s3_key,
                        'local_size': local_size,
                        's3_size': s3_size,
                        'match': match
                    }

                    result['sampled_files'].append(file_result)

                    if match:
                        verified += 1
                    else:
                        mismatches.append(file_result)

                except Exception as e:
                    error_msg = f"ファイル検証エラー: {s3_key} - {str(e)}"
                    result['errors'].append(error_msg)
                    logger.warning(error_msg)

                pbar.update(1)

        result['verified_count'] = verified
        result['mismatch_count'] = len(mismatches)

        # 結果表示
        print(f"\n✅ 検証結果:")
        print(f"  サンプリング数: {len(sampled_files)}")
        print(f"  一致: {verified}")
        print(f"  不一致: {len(mismatches)}")

        if mismatches:
            print(f"\n⚠️  不一致ファイル:")
            for mismatch in mismatches[:10]:  # 最初の10件のみ表示
                print(f"    - {mismatch['path']}")
                print(f"      ローカル: {humanize.naturalsize(mismatch['local_size'], binary=True)}")
                print(f"      S3: {humanize.naturalsize(mismatch['s3_size'], binary=True)}")

        result['success'] = (len(mismatches) == 0)

    except Exception as e:
        error_msg = f"バケット検証エラー: {str(e)}"
        result['errors'].append(error_msg)
        log_exception(logger, f"バケット {bucket_name} の検証に失敗", e)
        print(f"\n❌ エラー: {error_msg}")

    finally:
        # クリーンアップ
        if os.path.exists(bucket_temp_dir):
            try:
                shutil.rmtree(bucket_temp_dir)
                logger.info(f"一時ファイル削除: {bucket_temp_dir}")
            except Exception as e:
                logger.warning(f"一時ファイル削除に失敗: {str(e)}")

    return result


def generate_reports(results: List[Dict], bucket_list_result: Dict, output_dir: str, logger):
    """
    検証レポートを生成

    Args:
        results: 検証結果のリスト
        bucket_list_result: バケットリスト検証結果
        output_dir: 出力ディレクトリ
        logger: ロガー
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # JSONレポート
    json_path = os.path.join(output_dir, f"verification_report_{timestamp}.json")
    report_data = {
        'timestamp': datetime.now().isoformat(),
        'bucket_list_verification': bucket_list_result,
        'summary': {
            'total_buckets': len(results),
            'success_buckets': sum(1 for r in results if r['success']),
            'failed_buckets': sum(1 for r in results if not r['success']),
            'total_verified_files': sum(r['verified_count'] for r in results),
            'total_mismatches': sum(r['mismatch_count'] for r in results),
        },
        'buckets': results
    }

    os.makedirs(output_dir, exist_ok=True)
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(report_data, f, ensure_ascii=False, indent=2)

    logger.info(f"JSONレポート作成: {json_path}")

    # Markdownレポート
    md_path = os.path.join(output_dir, f"verification_report_{timestamp}.md")

    with open(md_path, 'w', encoding='utf-8') as f:
        f.write("# Dropbox バックアップ検証レポート\n\n")
        f.write(f"**生成日時**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # バケットリスト整合性チェック結果
        f.write("## バケットリスト整合性チェック\n\n")
        f.write(f"- **S3バケット数**: {bucket_list_result['s3_bucket_count']}\n")
        f.write(f"- **Dropboxバケット数**: {bucket_list_result['dropbox_bucket_count']}\n")

        if bucket_list_result['match']:
            f.write(f"- **結果**: ✅ 一致\n\n")
        else:
            f.write(f"- **結果**: ❌ 不一致\n\n")

            if bucket_list_result['missing_in_dropbox']:
                f.write(f"### ⚠️ 移行漏れ (S3にあるがDropboxにない): {len(bucket_list_result['missing_in_dropbox'])}個\n\n")
                for bucket in bucket_list_result['missing_in_dropbox']:
                    f.write(f"- `{bucket}`\n")
                f.write("\n")

            if bucket_list_result['extra_in_dropbox']:
                f.write(f"### ⚠️ 余分なバケット (DropboxにあるがS3にない): {len(bucket_list_result['extra_in_dropbox'])}個\n\n")
                for bucket in bucket_list_result['extra_in_dropbox']:
                    f.write(f"- `{bucket}`\n")
                f.write("\n")

        f.write("---\n\n")

        f.write("## サマリー\n\n")
        f.write(f"- **検証バケット数**: {report_data['summary']['total_buckets']}\n")
        f.write(f"- **成功**: {report_data['summary']['success_buckets']}\n")
        f.write(f"- **失敗**: {report_data['summary']['failed_buckets']}\n")
        f.write(f"- **検証ファイル数**: {report_data['summary']['total_verified_files']}\n")
        f.write(f"- **不一致数**: {report_data['summary']['total_mismatches']}\n\n")

        f.write("## バケット別結果\n\n")

        for result in results:
            status = "✅ 成功" if result['success'] else "❌ 失敗"
            f.write(f"### {result['bucket_name']} {status}\n\n")
            f.write(f"- **圧縮ファイル**: {', '.join(result['compressed_files'])}\n")
            f.write(f"- **サンプリング数**: {len(result['sampled_files'])}\n")
            f.write(f"- **一致**: {result['verified_count']}\n")
            f.write(f"- **不一致**: {result['mismatch_count']}\n")

            if result['errors']:
                f.write(f"\n**エラー**:\n\n")
                for error in result['errors']:
                    f.write(f"- {error}\n")

            if result['mismatch_count'] > 0:
                f.write(f"\n**不一致ファイル**:\n\n")
                mismatches = [item for item in result['sampled_files'] if not item['match']]
                for mismatch in mismatches[:20]:  # 最初の20件
                    f.write(f"- `{mismatch['path']}`\n")
                    f.write(f"  - ローカル: {humanize.naturalsize(mismatch['local_size'], binary=True)}\n")
                    f.write(f"  - S3: {humanize.naturalsize(mismatch['s3_size'], binary=True)}\n")

            f.write("\n---\n\n")

    logger.info(f"Markdownレポート作成: {md_path}")

    return json_path, md_path


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(
        description='Dropboxバックアップファイルを検証します',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # デフォルト設定で検証（5バケット、各50ファイル）
  python tools/verify_backup.py

  # サンプリング数を指定
  python tools/verify_backup.py --bucket-count 3 --file-count 20

  # 特定のバケットのみ検証
  python tools/verify_backup.py --buckets bucket1 bucket2
        """
    )

    parser.add_argument('--profile', type=str, help='AWSプロファイル名')
    parser.add_argument('--buckets', nargs='+', help='検証するバケット名（指定しない場合はサンプリング）')
    parser.add_argument('--bucket-count', type=int, default=5,
                       help='サンプリングするバケット数（デフォルト: 5）')
    parser.add_argument('--file-count', type=int, default=50,
                       help='各バケットでサンプリングするファイル数（デフォルト: 50）')
    parser.add_argument('--log-level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='ログレベル（デフォルト: INFO）')
    parser.add_argument('--output-dir', type=str, default='data',
                       help='レポート出力先ディレクトリ（デフォルト: data）')

    args = parser.parse_args()

    # 環境変数の読み込み
    load_dotenv()

    # ロガーの設定
    logger = setup_logger('verify_backup', args.log_level)

    try:
        print("\n🔍 Dropbox バックアップ検証ツール")
        print("=" * 80)

        # 設定読み込み
        dropbox_app_key = os.getenv('DROPBOX_APP_KEY')
        dropbox_app_secret = os.getenv('DROPBOX_APP_SECRET')
        dropbox_refresh_token = os.getenv('DROPBOX_REFRESH_TOKEN')
        dropbox_base_path = os.getenv('DROPBOX_BACKUP_PATH', '/S3_Backup')
        temp_dir = os.getenv('TEMP_DIR', './temp')

        # AWSクライアントの初期化
        logger.info("AWSクライアントを初期化しています...")
        aws_client = AWSClient(profile_name=args.profile, logger=logger)

        # Dropboxクライアントの初期化
        logger.info("Dropboxクライアントを初期化しています...")
        dropbox_client = DropboxClient(
            app_key=dropbox_app_key,
            app_secret=dropbox_app_secret,
            oauth2_refresh_token=dropbox_refresh_token,
            logger=logger
        )

        # 圧縮機能の初期化
        compressor = Compressor(logger=logger)

        # バケットリスト整合性チェック
        bucket_list_result = verify_bucket_lists(
            aws_client,
            dropbox_client,
            dropbox_base_path,
            logger
        )

        # バケット選択
        print(f"\n{'='*80}")
        print(f"📊 Step 2: サンプリング検証")
        print(f"{'='*80}")

        if args.buckets:
            target_buckets = args.buckets
            print(f"\n📦 指定されたバケット: {len(target_buckets)}個")
        else:
            # Dropboxからバケット一覧取得
            print(f"\n📋 Dropboxからバケット一覧を取得中...")
            entries = dropbox_client.list_folder(dropbox_base_path)
            bucket_folders = [e.name for e in entries if hasattr(e, 'name')]

            print(f"  ✓ {len(bucket_folders)}個のバケットを検出")

            # サイズ情報を取得
            print(f"\n📊 バケットサイズを取得中...")
            bucket_sizes = []
            for bucket_name in tqdm(bucket_folders, desc="  取得中"):
                try:
                    size, _ = aws_client.get_bucket_size_and_count(bucket_name)
                    bucket_sizes.append((bucket_name, size))
                except Exception as e:
                    logger.warning(f"バケット {bucket_name} のサイズ取得に失敗: {str(e)}")

            # 分散サンプリング
            sampled = select_distributed_sample(bucket_sizes, args.bucket_count)
            target_buckets = [name for name, size in sampled]

            print(f"\n🎲 サンプリング結果:")
            for name, size in sampled:
                print(f"  - {name}: {humanize.naturalsize(size, binary=True)}")

        # 検証実行
        results = []
        for i, bucket_name in enumerate(target_buckets, 1):
            print(f"\n{'='*80}")
            print(f"🔄 [{i}/{len(target_buckets)}] {bucket_name}")
            print(f"{'='*80}")

            result = verify_bucket(
                bucket_name,
                dropbox_base_path,
                aws_client,
                dropbox_client,
                compressor,
                temp_dir,
                args.file_count,
                logger
            )

            results.append(result)

        # レポート生成
        print(f"\n{'='*80}")
        print("📊 レポート生成中...")
        print(f"{'='*80}")

        json_path, md_path = generate_reports(results, bucket_list_result, args.output_dir, logger)

        print(f"\n✅ 検証完了！")
        print(f"\n📄 レポート:")
        print(f"  - JSON: {json_path}")
        print(f"  - Markdown: {md_path}")

        # サマリー表示
        success_count = sum(1 for r in results if r['success'])
        total_verified = sum(r['verified_count'] for r in results)
        total_mismatches = sum(r['mismatch_count'] for r in results)

        print(f"\n📊 最終結果:")
        print(f"  成功: {success_count}/{len(results)} バケット")
        print(f"  検証ファイル数: {total_verified}")
        print(f"  不一致: {total_mismatches}")

        if total_mismatches == 0 and success_count == len(results):
            print(f"\n🎉 全てのバケットの検証に成功しました！")
        else:
            print(f"\n⚠️  一部のバケットで問題が検出されました。レポートを確認してください。")

        logger.info("バックアップ検証が完了しました")

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
