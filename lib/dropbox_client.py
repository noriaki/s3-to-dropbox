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

    def __init__(self, app_key: str, app_secret: str, oauth2_refresh_token: str,
                 logger: Optional[logging.Logger] = None):
        """
        Dropboxクライアントの初期化

        Args:
            app_key: Dropbox App Key
            app_secret: Dropbox App Secret
            oauth2_refresh_token: Dropbox OAuth2 Refresh Token
            logger: ロガー（オプション）
        """
        self.logger = logger or logging.getLogger(__name__)

        if not app_key or not app_secret or not oauth2_refresh_token:
            self.logger.error("Dropbox認証情報が不足しています")
            self._print_setup_instructions()
            raise ValueError("Dropbox App Key, App Secret, Refresh Tokenが必要です")

        try:
            self.dbx = dropbox.Dropbox(
                oauth2_refresh_token=oauth2_refresh_token,
                app_key=app_key,
                app_secret=app_secret
            )

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
║           Dropbox OAuth2 Refresh Tokenの取得・設定方法                   ║
╚════════════════════════════════════════════════════════════════════════╝

Dropbox認証情報が正しく設定されていません。
長時間実行するバッチ処理では、リフレッシュトークンが必要です。

【ステップ1: Dropbox App Consoleにアクセス】

  1. ブラウザで以下のURLにアクセス:
     https://www.dropbox.com/developers/apps

  2. 既存のアプリを選択（または新規作成）

【ステップ2: 権限（Permissions）を確認】

  1. 「Permissions」タブを開く

  2. 以下の権限にチェックが入っているか確認:
     ✓ files.metadata.write
     ✓ files.content.write
     ✓ files.content.read
     ✓ account_info.read

  3. 変更した場合は「Submit」ボタンをクリック

【ステップ3: App KeyとApp Secretを確認】

  1. 「Settings」タブを開く

  2. App keyをコピー

  3. App secretの「Show」ボタンをクリックしてコピー

【ステップ4: 認証コードを取得】

  1. 以下のURLにアクセス（YOUR_APP_KEYを実際のApp Keyに置き換え）:

     https://www.dropbox.com/oauth2/authorize?client_id=YOUR_APP_KEY&token_access_type=offline&response_type=code

  2. アクセスを許可

  3. リダイレクトされたURLから認証コード（code=以降の文字列）をコピー

【ステップ5: リフレッシュトークンを取得】

  以下のcurlコマンドを実行（各値を実際の値に置き換え）:

  curl https://api.dropbox.com/oauth2/token \\
    -d code=YOUR_AUTHORIZATION_CODE \\
    -d grant_type=authorization_code \\
    -d client_id=YOUR_APP_KEY \\
    -d client_secret=YOUR_APP_SECRET

  レスポンスのJSONから "refresh_token" の値をコピー

【ステップ6: 環境変数を設定】

  .envファイルに以下を記載:

  DROPBOX_APP_KEY=your_app_key_here
  DROPBOX_APP_SECRET=your_app_secret_here
  DROPBOX_REFRESH_TOKEN=your_refresh_token_here

【セキュリティに関する注意】

  ⚠️  認証情報は秘密情報です！
  - Gitにコミットしない（.gitignoreに.envが含まれていることを確認）
  - 他人と共有しない
  - リフレッシュトークンは有効期限がありません

【トラブルシューティング】

  Q: 認証エラーが発生する
  A: App Key, App Secret, Refresh Tokenが正しく設定されているか確認してください。

  Q: 権限エラーが発生する
  A: Permissionsタブで必要な権限が全て有効になっているか確認してください。
     権限を変更した場合は、リフレッシュトークンを再取得してください。

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

    def download_file(self, dropbox_path: str, local_path: str,
                     progress_callback: Optional[callable] = None) -> bool:
        """
        Dropboxからファイルをダウンロード（大容量ファイル対応）

        Args:
            dropbox_path: Dropbox上のパス
            local_path: ローカル保存先パス
            progress_callback: 進捗コールバック関数(downloaded_bytes, total_bytes)

        Returns:
            bool: 成功した場合True
        """
        try:
            # ファイルメタデータを取得してサイズを確認
            metadata = self.dbx.files_get_metadata(dropbox_path)
            file_size = metadata.size

            # 保存先ディレクトリを作成
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            # ダウンロード実行
            metadata, response = self.dbx.files_download(dropbox_path)

            with open(local_path, 'wb') as f:
                # ストリーミングでダウンロード（メモリ効率的）
                downloaded = 0
                chunk_size = 10 * 1024 * 1024  # 10MB chunks for progress updates

                # response.content を取得してチャンクで書き込み
                content = response.content
                total_size = len(content)

                # チャンク単位で書き込み
                for i in range(0, total_size, chunk_size):
                    chunk = content[i:i + chunk_size]
                    f.write(chunk)
                    downloaded += len(chunk)

                    if progress_callback:
                        progress_callback(downloaded, file_size)

            self.logger.info(f"ダウンロード完了: {dropbox_path} -> {local_path}")
            return True

        except ApiError as e:
            self.logger.error(f"ファイルダウンロードに失敗: {dropbox_path} -> {local_path}")
            self.logger.error(f"エラー詳細: {str(e)}")
            # 失敗した場合は部分ダウンロードファイルを削除
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except Exception:
                    pass
            return False
        except Exception as e:
            self.logger.error(f"予期しないエラー: {str(e)}")
            # 失敗した場合は部分ダウンロードファイルを削除
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except Exception:
                    pass
            return False

    def get_file_metadata(self, dropbox_path: str) -> Optional[dict]:
        """
        ファイルのメタデータを取得

        Args:
            dropbox_path: Dropbox上のパス

        Returns:
            dict: メタデータ（name, size, path_display等）、失敗時はNone
        """
        try:
            metadata = self.dbx.files_get_metadata(dropbox_path)
            return {
                'name': metadata.name,
                'path_display': metadata.path_display,
                'size': metadata.size if hasattr(metadata, 'size') else 0,
                'is_folder': isinstance(metadata, dropbox.files.FolderMetadata),
                'client_modified': metadata.client_modified if hasattr(metadata, 'client_modified') else None,
                'server_modified': metadata.server_modified if hasattr(metadata, 'server_modified') else None,
            }
        except ApiError as e:
            self.logger.error(f"メタデータの取得に失敗: {dropbox_path}")
            self.logger.error(f"エラー詳細: {str(e)}")
            return None
