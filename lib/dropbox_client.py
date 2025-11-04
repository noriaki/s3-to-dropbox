"""
Dropboxクライアントを提供するモジュール
"""

import os
import dropbox
from dropbox.exceptions import ApiError, AuthError
from dropbox.files import WriteMode
from typing import Optional
import logging


class DropboxClient:
    """Dropbox操作を提供するクライアント"""

    # チャンクサイズ: 150MB（Dropbox APIの推奨値）
    CHUNK_SIZE = 150 * 1024 * 1024

    def __init__(self, access_token: str, logger: Optional[logging.Logger] = None):
        """
        Dropboxクライアントの初期化

        Args:
            access_token: Dropboxアクセストークン
            logger: ロガー（オプション）
        """
        self.logger = logger or logging.getLogger(__name__)

        if not access_token:
            self.logger.error("Dropboxアクセストークンが指定されていません")
            self._print_setup_instructions()
            raise ValueError("Dropboxアクセストークンが必要です")

        try:
            self.dbx = dropbox.Dropbox(access_token)

            # 認証確認
            self._verify_credentials()

        except AuthError as e:
            self.logger.error("Dropbox認証に失敗しました")
            self._print_setup_instructions()
            raise
        except Exception as e:
            self.logger.error(f"Dropboxクライアントの初期化に失敗しました: {str(e)}")
            raise

    def _verify_credentials(self):
        """認証情報の確認"""
        try:
            account = self.dbx.users_get_current_account()
            self.logger.info(f"Dropbox認証成功: {account.name.display_name} ({account.email})")

            # ストレージ容量の確認
            space_usage = self.dbx.users_get_space_usage()
            used = space_usage.used
            allocated = space_usage.allocation.get_individual().allocated
            available = allocated - used

            used_gb = used / (1024 ** 3)
            allocated_gb = allocated / (1024 ** 3)
            available_gb = available / (1024 ** 3)

            self.logger.info(f"Dropboxストレージ: {used_gb:.2f}GB / {allocated_gb:.2f}GB 使用中")
            self.logger.info(f"利用可能容量: {available_gb:.2f}GB")

        except Exception as e:
            self.logger.error(f"Dropbox認証の確認に失敗しました: {str(e)}")
            raise

    def _print_setup_instructions(self):
        """Dropbox認証設定の手順を表示"""
        instructions = """
╔════════════════════════════════════════════════════════════════════════╗
║                 Dropbox APIトークンの取得・設定方法                       ║
╚════════════════════════════════════════════════════════════════════════╝

Dropboxアクセストークンが正しく設定されていません。
以下の手順でトークンを取得してください。

【ステップ1: Dropbox App Consoleにアクセス】

  1. ブラウザで以下のURLにアクセス:
     https://www.dropbox.com/developers/apps

  2. Dropboxアカウントでログイン

【ステップ2: アプリケーションを作成】

  1. 「Create app」ボタンをクリック

  2. 以下の設定を選択:
     - API: Scoped access
     - Type of access: Full Dropbox
     - Name: 任意の名前（例: S3-to-Dropbox-Backup）

  3. 「Create app」をクリック

【ステップ3: 権限（Permissions）を設定】

  1. 「Permissions」タブを開く

  2. 以下の権限にチェックを入れる:
     ✓ files.metadata.write
     ✓ files.content.write
     ✓ files.content.read
     ✓ account_info.read

  3. 「Submit」ボタンをクリック

【ステップ4: アクセストークンを生成】

  1. 「Settings」タブに戻る

  2. 「OAuth 2」セクションを見つける

  3. 「Generated access token」の下にある
     「Generate」ボタンをクリック

  4. 表示されたトークンをコピー
     （このトークンは一度しか表示されません！）

【ステップ5: トークンを設定】

  方法1: 環境変数を使用
    $ export DROPBOX_ACCESS_TOKEN="your_token_here"

  方法2: .envファイルを使用
    プロジェクトルートに .env ファイルを作成し、以下を記載:

    DROPBOX_ACCESS_TOKEN=your_token_here

【トークンの確認方法】

  $ echo $DROPBOX_ACCESS_TOKEN

【セキュリティに関する注意】

  ⚠️  アクセストークンは秘密情報です！
  - Gitにコミットしない（.gitignoreに.envが含まれていることを確認）
  - 他人と共有しない
  - 定期的に再生成することを推奨

【トラブルシューティング】

  Q: トークンが無効と表示される
  A: トークンが正しくコピーされているか確認してください。
     前後にスペースが入っていないか注意してください。

  Q: 権限エラーが発生する
  A: Permissions タブで必要な権限が全て有効になっているか確認してください。

  Q: トークンを忘れた・紛失した
  A: 新しいトークンを生成してください。古いトークンは無効化されます。

詳細は README.md を参照してください。
"""
        print(instructions)

    def get_available_space(self) -> int:
        """
        利用可能なストレージ容量を取得（バイト単位）

        Returns:
            int: 利用可能容量（バイト）
        """
        try:
            space_usage = self.dbx.users_get_space_usage()
            used = space_usage.used
            allocated = space_usage.allocation.get_individual().allocated
            return allocated - used
        except Exception as e:
            self.logger.error(f"ストレージ容量の取得に失敗: {str(e)}")
            return 0

    def upload_file(self, local_path: str, dropbox_path: str,
                   progress_callback: Optional[callable] = None) -> bool:
        """
        ファイルをDropboxにアップロード（大容量ファイル対応）

        Args:
            local_path: ローカルファイルパス
            dropbox_path: Dropbox上のパス
            progress_callback: 進捗コールバック関数

        Returns:
            bool: 成功した場合True
        """
        try:
            file_size = os.path.getsize(local_path)

            with open(local_path, 'rb') as f:
                if file_size <= self.CHUNK_SIZE:
                    # 小さいファイルは一括アップロード
                    self.dbx.files_upload(
                        f.read(),
                        dropbox_path,
                        mode=WriteMode('overwrite')
                    )
                    if progress_callback:
                        progress_callback(file_size, file_size)
                else:
                    # 大きいファイルはチャンクアップロード
                    upload_session_start_result = self.dbx.files_upload_session_start(
                        f.read(self.CHUNK_SIZE)
                    )
                    session_id = upload_session_start_result.session_id
                    cursor = dropbox.files.UploadSessionCursor(
                        session_id=session_id,
                        offset=f.tell()
                    )

                    if progress_callback:
                        progress_callback(cursor.offset, file_size)

                    # 残りのチャンクをアップロード
                    while f.tell() < file_size:
                        remaining = file_size - f.tell()
                        chunk_size = min(self.CHUNK_SIZE, remaining)
                        chunk = f.read(chunk_size)

                        if remaining <= self.CHUNK_SIZE:
                            # 最後のチャンク
                            commit = dropbox.files.CommitInfo(
                                path=dropbox_path,
                                mode=WriteMode('overwrite')
                            )
                            self.dbx.files_upload_session_finish(
                                chunk,
                                cursor,
                                commit
                            )
                        else:
                            # 中間のチャンク
                            self.dbx.files_upload_session_append_v2(
                                chunk,
                                cursor
                            )
                            cursor.offset = f.tell()

                        if progress_callback:
                            progress_callback(cursor.offset, file_size)

            self.logger.info(f"アップロード完了: {dropbox_path}")
            return True

        except ApiError as e:
            self.logger.error(f"ファイルアップロードに失敗: {local_path} -> {dropbox_path}")
            self.logger.error(f"エラー詳細: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"予期しないエラー: {str(e)}")
            return False

    def create_folder(self, folder_path: str) -> bool:
        """
        フォルダを作成

        Args:
            folder_path: フォルダパス

        Returns:
            bool: 成功した場合True
        """
        try:
            self.dbx.files_create_folder_v2(folder_path)
            self.logger.info(f"フォルダ作成: {folder_path}")
            return True
        except ApiError as e:
            # フォルダが既に存在する場合はエラーを無視
            if e.error.is_path() and e.error.get_path().is_conflict():
                self.logger.debug(f"フォルダは既に存在します: {folder_path}")
                return True
            else:
                self.logger.error(f"フォルダ作成に失敗: {folder_path}")
                self.logger.error(f"エラー詳細: {str(e)}")
                return False

    def file_exists(self, dropbox_path: str) -> bool:
        """
        ファイルが存在するか確認

        Args:
            dropbox_path: Dropbox上のパス

        Returns:
            bool: 存在する場合True
        """
        try:
            self.dbx.files_get_metadata(dropbox_path)
            return True
        except ApiError:
            return False

    def list_folder(self, folder_path: str) -> list:
        """
        フォルダ内のファイル一覧を取得

        Args:
            folder_path: フォルダパス

        Returns:
            list: ファイル・フォルダのリスト
        """
        try:
            result = self.dbx.files_list_folder(folder_path)
            entries = result.entries

            # ページネーション対応
            while result.has_more:
                result = self.dbx.files_list_folder_continue(result.cursor)
                entries.extend(result.entries)

            return entries
        except ApiError as e:
            self.logger.error(f"フォルダ一覧の取得に失敗: {folder_path}")
            self.logger.error(f"エラー詳細: {str(e)}")
            return []
