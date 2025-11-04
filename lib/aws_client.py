"""
AWS S3クライアントを提供するモジュール
"""

import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from typing import Optional, Dict, List, Tuple
from datetime import datetime
import logging


class AWSClient:
    """AWS S3操作を提供するクライアント"""

    def __init__(self, profile_name: Optional[str] = None, logger: Optional[logging.Logger] = None):
        """
        AWSクライアントの初期化

        Args:
            profile_name: AWSプロファイル名（オプション）
            logger: ロガー（オプション）
        """
        self.logger = logger or logging.getLogger(__name__)
        self.profile_name = profile_name

        try:
            # セッションの作成
            if profile_name:
                self.session = boto3.Session(profile_name=profile_name)
            else:
                self.session = boto3.Session()

            # S3クライアントの作成
            self.s3_client = self.session.client('s3')
            self.s3_resource = self.session.resource('s3')

            # 認証確認
            self._verify_credentials()

        except (NoCredentialsError, PartialCredentialsError) as e:
            self.logger.error("AWS認証情報が見つかりません")
            self._print_setup_instructions()
            raise
        except Exception as e:
            self.logger.error(f"AWSクライアントの初期化に失敗しました: {str(e)}")
            raise

    def _verify_credentials(self):
        """認証情報の確認"""
        try:
            # STSを使って認証情報を確認
            sts = self.session.client('sts')
            identity = sts.get_caller_identity()
            self.logger.info(f"AWS認証成功: Account={identity['Account']}, ARN={identity['Arn']}")
        except Exception as e:
            self.logger.error(f"AWS認証の確認に失敗しました: {str(e)}")
            self._print_setup_instructions()
            raise

    def _print_setup_instructions(self):
        """AWS認証設定の手順を表示"""
        instructions = """
╔════════════════════════════════════════════════════════════════════════╗
║                    AWS認証情報の設定方法                                 ║
╚════════════════════════════════════════════════════════════════════════╝

AWS認証情報が正しく設定されていません。以下の方法で設定してください。

【方法1: AWS CLI設定ファイルを使用（推奨）】

  1. AWS CLIがインストールされているか確認:
     $ aws --version

  2. AWS CLIで認証情報を設定:
     $ aws configure

     以下の情報を入力してください:
     - AWS Access Key ID: あなたのアクセスキーID
     - AWS Secret Access Key: あなたのシークレットアクセスキー
     - Default region name: ap-northeast-1（または使用するリージョン）
     - Default output format: json

  3. 設定ファイルの場所:
     - macOS/Linux: ~/.aws/credentials
     - Windows: C:\\Users\\USERNAME\\.aws\\credentials

【方法2: 環境変数を使用】

  以下の環境変数を設定してください:

  $ export AWS_ACCESS_KEY_ID="your_access_key"
  $ export AWS_SECRET_ACCESS_KEY="your_secret_key"
  $ export AWS_DEFAULT_REGION="ap-northeast-1"

【方法3: .envファイルを使用】

  プロジェクトルートに .env ファイルを作成し、以下を記載:

  AWS_ACCESS_KEY_ID=your_access_key
  AWS_SECRET_ACCESS_KEY=your_secret_key
  AWS_DEFAULT_REGION=ap-northeast-1

【既存設定の確認方法】

  $ aws configure list
  $ aws sts get-caller-identity

【IAMユーザーの作成（必要な場合）】

  1. AWS マネジメントコンソールにログイン
  2. IAM サービスを開く
  3. 「ユーザー」→「ユーザーを追加」
  4. アクセスキーを作成
  5. 必要な権限: AmazonS3FullAccess（または適切なカスタムポリシー）

詳細は README.md を参照してください。
"""
        print(instructions)

    def list_buckets(self) -> List[Dict[str, any]]:
        """
        全バケットのリストを取得

        Returns:
            List[Dict]: バケット情報のリスト
        """
        try:
            response = self.s3_client.list_buckets()
            buckets = response.get('Buckets', [])
            self.logger.info(f"{len(buckets)}個のバケットを検出しました")
            return buckets
        except ClientError as e:
            self.logger.error(f"バケットリストの取得に失敗しました: {str(e)}")
            raise

    def get_bucket_region(self, bucket_name: str) -> str:
        """
        バケットのリージョンを取得

        Args:
            bucket_name: バケット名

        Returns:
            str: リージョン名
        """
        try:
            response = self.s3_client.get_bucket_location(Bucket=bucket_name)
            region = response.get('LocationConstraint')
            # us-east-1の場合はNoneが返される
            return region if region else 'us-east-1'
        except ClientError as e:
            self.logger.warning(f"バケット {bucket_name} のリージョン取得に失敗: {str(e)}")
            return "unknown"

    def get_bucket_versioning(self, bucket_name: str) -> bool:
        """
        バケットのバージョン管理状態を取得

        Args:
            bucket_name: バケット名

        Returns:
            bool: バージョン管理が有効な場合True
        """
        try:
            response = self.s3_client.get_bucket_versioning(Bucket=bucket_name)
            status = response.get('Status', 'Disabled')
            return status == 'Enabled'
        except ClientError as e:
            self.logger.warning(f"バケット {bucket_name} のバージョン管理状態取得に失敗: {str(e)}")
            return False

    def get_bucket_size_and_count(self, bucket_name: str) -> Tuple[int, int]:
        """
        バケットのサイズとオブジェクト数を取得

        Args:
            bucket_name: バケット名

        Returns:
            Tuple[int, int]: (総サイズ(バイト), オブジェクト数)
        """
        try:
            total_size = 0
            object_count = 0

            bucket = self.s3_resource.Bucket(bucket_name)

            for obj in bucket.objects.all():
                total_size += obj.size
                object_count += 1

            return total_size, object_count

        except ClientError as e:
            self.logger.error(f"バケット {bucket_name} のサイズ取得に失敗: {str(e)}")
            return 0, 0

    def download_bucket(self, bucket_name: str, download_path: str,
                       progress_callback: Optional[callable] = None) -> bool:
        """
        バケットの全オブジェクトをダウンロード

        Args:
            bucket_name: バケット名
            download_path: ダウンロード先パス
            progress_callback: 進捗コールバック関数

        Returns:
            bool: 成功した場合True
        """
        try:
            bucket = self.s3_resource.Bucket(bucket_name)
            os.makedirs(download_path, exist_ok=True)

            for obj in bucket.objects.all():
                # S3のディレクトリマーカー（キーが / で終わるオブジェクト）はスキップ
                if obj.key.endswith('/'):
                    self.logger.debug(f"ディレクトリマーカーをスキップ: {obj.key}")
                    continue

                target_path = os.path.join(download_path, obj.key)

                # ディレクトリの作成
                os.makedirs(os.path.dirname(target_path), exist_ok=True)

                # ファイルのダウンロード
                self.s3_client.download_file(bucket_name, obj.key, target_path)

                if progress_callback:
                    progress_callback(obj.key, obj.size)

            return True

        except ClientError as e:
            self.logger.error(f"バケット {bucket_name} のダウンロードに失敗: {str(e)}")
            return False

    def delete_bucket(self, bucket_name: str, delete_versions: bool = True) -> bool:
        """
        バケットを削除（オブジェクトとバージョンも削除）

        Args:
            bucket_name: バケット名
            delete_versions: バージョンも削除する場合True

        Returns:
            bool: 成功した場合True
        """
        try:
            bucket = self.s3_resource.Bucket(bucket_name)

            # バージョン管理されたオブジェクトを削除
            if delete_versions:
                bucket.object_versions.delete()
            else:
                bucket.objects.delete()

            # バケット本体を削除
            bucket.delete()

            self.logger.info(f"バケット {bucket_name} を削除しました")
            return True

        except ClientError as e:
            self.logger.error(f"バケット {bucket_name} の削除に失敗: {str(e)}")
            return False
