# S3 to Dropbox バックアップ＆削除システム

AWS S3バケットをDropboxにバックアップし、安全に削除するためのツールセットです。

## 📋 目次

- [概要](#概要)
- [機能](#機能)
- [システム要件](#システム要件)
- [セットアップ手順](#セットアップ手順)
  - [1. 依存関係のインストール](#1-依存関係のインストール)
  - [2. AWS認証情報の設定](#2-aws認証情報の設定)
  - [3. Dropbox APIトークンの取得・設定](#3-dropbox-apiトークンの取得設定)
  - [4. 環境変数の設定](#4-環境変数の設定)
- [使用方法](#使用方法)
  - [ツール1: バケット情報確認](#ツール1-バケット情報確認)
  - [ツール2: データ移行](#ツール2-データ移行)
  - [ツール3: バックアップ検証](#ツール3-バックアップ検証)
  - [ツール4: バケット削除](#ツール4-バケット削除)
- [ワークフロー](#ワークフロー)
- [データ保存形式](#データ保存形式)
- [トラブルシューティング](#トラブルシューティング)
- [FAQ](#faq)
- [注意事項](#注意事項)

---

## 概要

このツールセットは、AWS S3の複数のバケットを効率的にDropboxにバックアップし、その後安全に削除するために開発されました。

### 主な特徴

- ✅ 全自動バックアップ＆削除プロセス
- ✅ 大容量ファイル対応（10GB超のファイルは自動分割）
- ✅ 途中中断・再開機能
- ✅ 詳細なプログレス表示
- ✅ 圧縮によるストレージ節約
- ✅ 安全な削除確認プロセス

---

## 機能

### ツール1: バケット情報確認 (`bucket_info.py`)

- 全S3バケットのリストを取得
- 各バケットのサイズ、オブジェクト数、リージョンを表示
- Dropbox容量との比較
- JSON形式での詳細情報出力

### ツール2: データ移行 (`migrate_data.py`)

- S3からのデータダウンロード
- データの圧縮（ZIP or tar.gz）
- 10GB超過時の自動分割
- ファイル一覧（file_list.md）の生成
- 解凍方法を含むREADME.mdの生成
- Dropboxへのアップロード
- 進行状況の永続化（中断・再開可能）

### ツール3: バックアップ検証 (`verify_backup.py`)

- Dropboxバックアップファイルの整合性検証
- 圧縮ファイルのダウンロードと解凍テスト
- S3メタデータとの突合検証
- 分散サンプリング（小・中・大サイズのバケット/ファイル）
- JSON/Markdownレポート生成

### ツール4: バケット削除 (`delete_buckets.py`)

- 移行完了したバケットのみ削除
- ドライランモード（削除予定の確認）
- 二重確認プロセス
- 詳細な削除ログ

---

## システム要件

- **Python**: 3.8以上
- **OS**: macOS, Linux, Windows
- **ディスク容量**: 最大バケットサイズの2倍以上の空き容量を推奨
- **ネットワーク**: 安定したインターネット接続

---

## セットアップ手順

### 1. 依存関係のインストール

#### Python仮想環境の作成

```bash
# 仮想環境を作成
python3 -m venv venv
```

#### 依存パッケージのインストール

**⚠️ 重要**: macOS（特にZSH使用時）では、`source venv/bin/activate`が構文エラーになる場合があります。
その場合は以下の方法で**activateせずに**直接インストールしてください：

```bash
# activateせずに、venv/bin/pip を直接使用（推奨）
venv/bin/pip install -r requirements.txt
```

<details>
<summary>💡 従来の方法（activate を使用）でインストールしたい場合</summary>

```bash
# macOS/Linux (Bash)
source venv/bin/activate
pip install -r requirements.txt

# Windows
venv\Scripts\activate
pip install -r requirements.txt
```

**注意**: ZSHでは`source venv/bin/activate`が以下のエラーで失敗することがあります：
```
venv/bin/activate:4: defining function based on alias `deactivate'
```

この場合は上記の「activateせずに直接インストール」する方法を使用してください。

</details>

---

### 2. AWS認証情報の設定

AWS認証情報を設定する方法は3つあります。**方法1（AWS CLI）を推奨します。**

#### 方法1: AWS CLIを使用（推奨）

##### ステップ1: AWS CLIのインストール確認

```bash
aws --version
```

AWS CLIがインストールされていない場合は、[公式ドキュメント](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)を参照してインストールしてください。

##### ステップ2: AWS認証情報の設定

```bash
aws configure
```

以下の情報を入力してください:

```
AWS Access Key ID [None]: YOUR_ACCESS_KEY_ID
AWS Secret Access Key [None]: YOUR_SECRET_ACCESS_KEY
Default region name [None]: ap-northeast-1
Default output format [None]: json
```

##### ステップ3: 設定の確認

```bash
# 設定内容の確認
aws configure list

# 認証テスト
aws sts get-caller-identity
```

成功すると、以下のような出力が表示されます:

```json
{
    "UserId": "AIDAI...",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/your-username"
}
```

#### 方法2: 環境変数を使用

```bash
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_DEFAULT_REGION="ap-northeast-1"
```

#### 方法3: .envファイルを使用

プロジェクトルートに `.env` ファイルを作成:

```bash
cp .env.example .env
```

`.env` ファイルを編集:

```
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=ap-northeast-1
```

#### 必要なIAM権限

以下の権限が必要です:

- `s3:ListAllMyBuckets`
- `s3:ListBucket`
- `s3:GetObject`
- `s3:GetBucketLocation`
- `s3:GetBucketVersioning`
- `s3:DeleteObject`
- `s3:DeleteObjectVersion`
- `s3:DeleteBucket`

#### トラブルシューティング

**Q: 認証エラーが発生する**

```bash
# 設定ファイルの場所を確認
# macOS/Linux
ls -la ~/.aws/

# 認証情報ファイルの内容を確認
cat ~/.aws/credentials
```

**Q: 複数のAWSアカウントを使い分けたい**

プロファイルを使用します:

```bash
# プロファイルを指定して設定
aws configure --profile myprofile

# ツール実行時にプロファイルを指定
venv/bin/python tools/bucket_info.py --profile myprofile
```

---

### 3. Dropbox OAuth2 Refresh Tokenの取得・設定

**⚠️ 重要**: 長時間実行するデータ移行では、短期トークン（4時間有効）ではなく、**リフレッシュトークン**を使用する必要があります。

#### ステップ1: Dropbox App Consoleにアクセス

1. ブラウザで以下のURLにアクセス:
   ```
   https://www.dropbox.com/developers/apps
   ```

2. Dropboxアカウントでログイン

3. 既存のアプリを選択（または新規作成）

#### ステップ2: アプリケーションを作成（新規の場合のみ）

1. **「Create app」** ボタンをクリック

2. 以下の設定を選択:

   - **Choose an API**: `Scoped access`
   - **Choose the type of access you need**: `Full Dropbox`
   - **Name your app**: 任意の名前（例: `S3-to-Dropbox-Backup`）

3. **「Create app」** をクリック

#### ステップ3: 権限（Permissions）を設定

1. **「Permissions」** タブを開く

2. 以下の権限にチェックを入れる:

   - ☑️ `files.metadata.write`
   - ☑️ `files.content.write`
   - ☑️ `files.content.read`
   - ☑️ `account_info.read`

3. **「Submit」** ボタンをクリック

#### ステップ4: App KeyとApp Secretを確認

1. **「Settings」** タブを開く

2. **App key** をコピーして安全な場所に保存

3. **App secret** の **「Show」** ボタンをクリックしてコピー

#### ステップ5: 認証コードを取得

1. 以下のURLにブラウザでアクセス（`YOUR_APP_KEY`を実際のApp Keyに置き換え）:

   ```
   https://www.dropbox.com/oauth2/authorize?client_id=YOUR_APP_KEY&token_access_type=offline&response_type=code
   ```

   > 💡 **重要**: `token_access_type=offline` パラメータを必ず含めてください

2. Dropboxアカウントでログインし、アクセスを許可

3. リダイレクトされたURL（`https://localhost/?code=XXXXX...`）から **認証コード**（`code=` 以降の文字列）をコピー

#### ステップ6: リフレッシュトークンを取得

以下のcurlコマンドを実行（各値を実際の値に置き換え）:

```bash
curl https://api.dropbox.com/oauth2/token \
  -d code=YOUR_AUTHORIZATION_CODE \
  -d grant_type=authorization_code \
  -d client_id=YOUR_APP_KEY \
  -d client_secret=YOUR_APP_SECRET
```

**レスポンス例**:
```json
{
  "access_token": "sl.xxxxxxxxxx",
  "token_type": "bearer",
  "expires_in": 14400,
  "refresh_token": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
  "scope": "...",
  "uid": "...",
  "account_id": "..."
}
```

レスポンスから **`refresh_token`** の値をコピーして保存してください。

#### ステップ7: 環境変数を設定

`.env` ファイルに以下を設定:

```bash
DROPBOX_APP_KEY=your_app_key_here
DROPBOX_APP_SECRET=your_app_secret_here
DROPBOX_REFRESH_TOKEN=your_refresh_token_here
```

#### ステップ8: 接続テスト

```bash
# データ移行ツールを実行して認証をテスト
venv/bin/python tools/migrate_data.py --reset
```

成功すると、Dropbox認証成功のメッセージが表示されます。

#### トラブルシューティング

**Q: 認証エラーが発生する**

- App Key, App Secret, Refresh Tokenが正しく設定されているか確認
- `.env`ファイルの値に前後のスペースがないか確認
- 認証コードは1回しか使用できません。エラーの場合は新しい認証コードを取得

**Q: 権限エラーが発生する**

- Permissions タブで必要な権限が全て有効になっているか確認
- **権限を変更した場合は、新しいリフレッシュトークンを取得する必要があります**

**Q: "expired_access_token" エラーが発生する**

- リフレッシュトークンではなく、短期トークンを使用している可能性があります
- 認証URL に `token_access_type=offline` パラメータが含まれているか確認
- 正しいリフレッシュトークンを取得し直してください

**Q: リフレッシュトークンの有効期限は？**

- リフレッシュトークンは**有効期限がありません**
- ユーザーがアプリのアクセス許可を取り消すまで永続的に使用できます

---

### 4. 環境変数の設定

`.env.example` を `.env` にコピーし、必要に応じて編集します:

```bash
cp .env.example .env
```

`.env` ファイルの主な設定項目:

```bash
# Dropbox設定
DROPBOX_APP_KEY=your_app_key_here
DROPBOX_APP_SECRET=your_app_secret_here
DROPBOX_REFRESH_TOKEN=your_refresh_token_here
DROPBOX_BACKUP_PATH=/S3_Backup

# 一時ファイル保存先
TEMP_DIR=./temp

# ログレベル
LOG_LEVEL=INFO

# 圧縮設定
COMPRESSION_FORMAT=zip
SPLIT_SIZE=10737418240  # 10GB
```

---

## 使用方法

**⚠️ 仮想環境を使用している場合の注意**

ツールを実行する際は、以下のいずれかの方法を使用してください：

**方法1: venv/bin/python を直接使用（推奨）**
```bash
# activateせずに実行
venv/bin/python tools/bucket_info.py
venv/bin/python tools/migrate_data.py
venv/bin/python tools/delete_buckets.py
```

**方法2: 仮想環境をactivateして実行**
```bash
# 仮想環境をactivate（ZSHでエラーになる場合は方法1を使用）
source venv/bin/activate
python tools/bucket_info.py
# 終了時
deactivate
```

以下の例では簡潔にするため `python` と記載していますが、仮想環境を使用している場合は適宜 `venv/bin/python` に読み替えてください。

---

### ツール1: バケット情報確認

全S3バケットの情報を収集・表示します。

#### 基本的な使用方法

```bash
venv/bin/python tools/bucket_info.py
# または（activate済みの場合）
python tools/bucket_info.py
```

#### オプション

```bash
# 特定のAWSプロファイルを使用
venv/bin/python tools/bucket_info.py --profile myprofile

# 詳細ログを出力
venv/bin/python tools/bucket_info.py --log-level DEBUG

# 出力ファイルを指定
venv/bin/python tools/bucket_info.py --output my_buckets.json
```

#### 出力例

```
🔍 S3バケット情報確認ツール
================================================================================
📦 67個のバケットを検出しました。情報を収集中...

No.   バケット名                              リージョン            作成日        オブジェクト数        サイズ
==================================================================================================================
1     my-large-bucket                         ap-northeast-1        2015-03-20            15,234      45.2 GB
2     assets-production                       us-east-1             2014-06-15             8,921      23.1 GB
...

📊 サマリー
================================================================================
総バケット数:         67
総オブジェクト数:     234,567
総データ量:           1.2 TB
圧縮後推定サイズ:     840 GB （圧縮率70%と仮定）
Dropbox空き容量:      1.5 TB
✅ Dropbox容量は十分です（余裕: 660 GB）

10GB超のバケット:     12個 （分割が必要）
================================================================================
```

---

### ツール2: データ移行

S3バケットをDropboxに移行します。

#### 基本的な使用方法

```bash
venv/bin/python tools/migrate_data.py
# または（activate済みの場合）
python tools/migrate_data.py
```

#### オプション

```bash
# 特定のバケットのみ移行
venv/bin/python tools/migrate_data.py --buckets bucket1 bucket2

# 特定のAWSプロファイルを使用
venv/bin/python tools/migrate_data.py --profile myprofile

# 進行状況をリセット
venv/bin/python tools/migrate_data.py --reset
```

#### 処理フロー

各バケットについて以下の処理を実行します:

1. ✅ **バケット情報取得** - リージョン、サイズ、オブジェクト数を取得
2. ✅ **ディスク容量確認** - 十分な空き容量があるか確認
3. ✅ **S3からダウンロード** - 全オブジェクトをローカルにダウンロード
4. ✅ **ファイルリスト生成** - `file_list.md`を生成
5. ✅ **データ圧縮** - ZIP形式で圧縮（10GB超の場合は自動分割）
6. ✅ **整合性確認** - 圧縮ファイルの検証
7. ✅ **README生成** - 解凍方法を含む`README.md`を生成
8. ✅ **Dropboxアップロード** - 圧縮ファイル、file_list.md、READMEをアップロード
9. ✅ **クリーンアップ** - 一時ファイルを削除

#### 途中中断・再開

処理は途中で中断しても、次回実行時に続きから再開できます:

```bash
# 処理中に Ctrl+C で中断

# 再実行すると、完了済みのバケットはスキップされます
venv/bin/python tools/migrate_data.py
```

---

### ツール3: バックアップ検証

Dropboxにアップロードされたバックアップファイルの整合性を検証します。

#### 基本的な使用方法

```bash
venv/bin/python tools/verify_backup.py
# または（activate済みの場合）
python tools/verify_backup.py
```

#### オプション

```bash
# サンプリング数を指定（デフォルト: 5バケット、各50ファイル）
venv/bin/python tools/verify_backup.py --bucket-count 3 --file-count 20

# 特定のバケットのみ検証
venv/bin/python tools/verify_backup.py --buckets bucket1 bucket2

# 特定のAWSプロファイルを使用
venv/bin/python tools/verify_backup.py --profile myprofile

# レポート出力先を指定
venv/bin/python tools/verify_backup.py --output-dir reports
```

#### 検証フロー

各バケットについて以下の検証を実行します:

1. ✅ **Dropboxからファイル一覧取得** - 圧縮ファイルの存在確認
2. ✅ **圧縮ファイルダウンロード** - 分割ファイルにも対応
3. ✅ **分割ファイル結合** - 必要に応じて結合
4. ✅ **整合性チェック** - CRC検証
5. ✅ **アーカイブ解凍** - 実際に解凍できるか確認
6. ✅ **ファイルサンプリング** - 小・中・大サイズから分散選択
7. ✅ **S3メタデータ突合** - ファイル名・サイズをS3と比較
8. ✅ **レポート生成** - JSON/Markdown形式で出力

#### サンプリング方法

- **バケット選択**: 全バケットを小・中・大サイズに分類し、各グループから均等に選択
- **ファイル選択**: 各バケット内のファイルも小・中・大サイズに分類し、均等に選択
- これにより、偏りのない代表的なサンプルを検証できます

#### 出力レポート

```
data/
├── verification_report_20251105_123456.json    # 詳細データ
└── verification_report_20251105_123456.md      # 人間向けレポート
```

---

### ツール4: バケット削除

移行完了したバケットを削除します。

#### ドライラン（削除予定の確認）

```bash
venv/bin/python tools/delete_buckets.py
# または（activate済みの場合）
python tools/delete_buckets.py
```

実際には削除せず、削除予定のバケット情報のみを表示します。

#### 実際に削除

```bash
venv/bin/python tools/delete_buckets.py --delete
# または（activate済みの場合）
python tools/delete_buckets.py --delete
```

二重確認プロセスが実行されます:

1. 削除予定バケットの一覧表示
2. `yes` の入力要求
3. `DELETE` の入力要求（最終確認）

#### オプション

```bash
# 特定のバケットのみ削除
venv/bin/python tools/delete_buckets.py --delete --buckets bucket1 bucket2

# 特定のAWSプロファイルを使用
venv/bin/python tools/delete_buckets.py --delete --profile myprofile
```

---

## ワークフロー

推奨される実行順序:

**注意**: 以下の例では簡潔にするため `python` と記載していますが、仮想環境を使用している場合は `venv/bin/python` を使用してください。

```
1️⃣  バケット情報確認
    ↓
    venv/bin/python tools/bucket_info.py
    ↓
    ・全バケットの情報を確認
    ・Dropbox容量が十分か確認
    ・分割が必要なバケット数を確認

2️⃣  データ移行
    ↓
    venv/bin/python tools/migrate_data.py
    ↓
    ・S3 → Dropboxへバックアップ
    ・途中で中断しても再開可能
    ・進行状況は自動保存

3️⃣  バックアップ検証
    ↓
    venv/bin/python tools/verify_backup.py
    ↓
    ・Dropboxから圧縮ファイルをダウンロード
    ・実際に解凍してS3メタデータと突合
    ・JSON/Markdownレポート生成
    ・問題があれば詳細レポートで確認

4️⃣  削除予定確認（ドライラン）
    ↓
    venv/bin/python tools/delete_buckets.py
    ↓
    ・削除予定のバケットを確認

5️⃣  バケット削除
    ↓
    venv/bin/python tools/delete_buckets.py --delete
    ↓
    ・確認プロンプトに従って削除実行
```

---

## データ保存形式

### Dropbox上の構造

```
/S3_Backup/
├── bucket-name-1/
│   ├── bucket-name-1.zip
│   ├── file_list.md
│   └── README.md
├── bucket-name-2/
│   ├── bucket-name-2.zip.001  # 分割ファイル
│   ├── bucket-name-2.zip.002
│   ├── bucket-name-2.zip.003
│   ├── file_list.md
│   └── README.md
└── ...
```

### file_list.md の内容

- ディレクトリ構造（ツリー表示）
- 全ファイルの詳細リスト（パス、サイズ、最終更新日時）

### README.md の内容

- バケット情報（リージョン、オブジェクト数、サイズ）
- 圧縮ファイル情報
- 解凍方法（OS別の手順）
- 分割ファイルの結合方法（分割された場合）

---

## トラブルシューティング

### 仮想環境のactivateエラー（ZSH）

**エラー**: `venv/bin/activate:4: defining function based on alias 'deactivate'`

**原因**: ZSHで Bash用のactivateスクリプトを実行しようとしている

**解決方法**:

**方法1: activateせずに直接実行（推奨）**
```bash
# venv/bin/pip を直接使用
venv/bin/pip install -r requirements.txt

# ツール実行時も venv/bin/python を使用
venv/bin/python tools/bucket_info.py
```

**方法2: Bashシェルで実行**
```bash
# Bashに切り替え
bash

# activateして作業
source venv/bin/activate
pip install -r requirements.txt
python tools/bucket_info.py

# 終了時
deactivate
exit  # ZSHに戻る
```

### Python externally-managed-environment エラー

**エラー**: `error: externally-managed-environment`

**原因**: Homebrew管理のPython 3.14では、PEP 668により直接インストールが制限されています

**解決方法**:

仮想環境を使用してください（activateは不要）：
```bash
# 仮想環境を作成
python3 -m venv venv

# activateせずにインストール
venv/bin/pip install -r requirements.txt

# ツールを実行
venv/bin/python tools/bucket_info.py
```

### AWS認証エラー

**エラー**: `NoCredentialsError` または `PartialCredentialsError`

**解決方法**:

```bash
# 認証情報を確認
aws configure list

# 認証テスト
aws sts get-caller-identity
```

[AWS認証情報の設定](#2-aws認証情報の設定)を参照してください。

### Dropbox認証エラー

**エラー**: `AuthError` または `expired_access_token`

**解決方法**:

1. リフレッシュトークンが正しく設定されているか確認:
   ```bash
   echo $DROPBOX_APP_KEY
   echo $DROPBOX_APP_SECRET
   echo $DROPBOX_REFRESH_TOKEN
   ```

2. `.env`ファイルの値に前後のスペースがないか確認

3. 権限が正しく設定されているか確認（Dropbox App Consoleで確認）
   - 権限を変更した場合は、新しいリフレッシュトークンを取得する必要があります

4. `expired_access_token`エラーの場合:
   - 短期トークンではなく、リフレッシュトークンを使用しているか確認
   - [Dropbox OAuth2 Refresh Tokenの取得](#3-dropbox-oauth2-refresh-tokenの取得設定)を参照して、正しくリフレッシュトークンを取得してください

### ディスク容量不足

**エラー**: `ディスク容量が不足しています`

**解決方法**:

1. 不要なファイルを削除してディスク容量を確保

2. `TEMP_DIR`を別のドライブに変更:
   ```bash
   # .envファイルを編集
   TEMP_DIR=/path/to/large/drive/temp
   ```

### ネットワークエラー

**エラー**: タイムアウトや接続エラー

**解決方法**:

1. インターネット接続を確認

2. リトライ回数を増やす:
   ```bash
   # .envファイルを編集
   MAX_RETRIES=5
   ```

3. 処理は自動的に保存されるので、再実行すれば続きから再開されます

### 圧縮エラー

**エラー**: 圧縮処理に失敗

**解決方法**:

1. ディスク容量を確認

2. 破損したファイルがないか確認

3. 圧縮形式を変更:
   ```bash
   # .envファイルを編集
   COMPRESSION_FORMAT=tar.gz
   ```

---

## FAQ

### Q: 移行にかかる時間はどのくらいですか？

**A**: バケットのサイズとオブジェクト数によりますが、目安として:

- 1GBのバケット: 約10〜20分
- 10GBのバケット: 約1〜2時間
- 100GBのバケット: 約10〜20時間

ネットワーク速度にも依存します。

### Q: 移行中にS3にファイルが追加されたらどうなりますか？

**A**: 移行開始時点のスナップショットがバックアップされます。移行開始後に追加されたファイルは含まれません。

### Q: 一部のファイルが失敗した場合は？

**A**: バケット単位で管理しています。1つのバケットが失敗しても、他のバケットは継続して処理されます。失敗したバケットは進行状況ファイルに記録され、エラー内容がログに出力されます。

### Q: コストはどのくらいかかりますか？

**A**: 主なコスト:

- **AWS S3**: データ転送料（S3 → インターネット）
  - 最初の1GBは無料、以降は$0.114/GB（東京リージョン）
- **Dropbox**: 容量が不足する場合はアップグレードが必要

### Q: 圧縮ファイルが分割された場合、どうやって結合しますか？

**A**: 各バケットフォルダの `README.md` に詳細な手順が記載されています。

macOS/Linux:
```bash
cat bucket-name.zip.* > bucket-name.zip
unzip bucket-name.zip
```

Windows (PowerShell):
```powershell
Get-Content bucket-name.zip.* -Raw | Set-Content bucket-name.zip -Encoding Byte
7z x bucket-name.zip
```

### Q: 圧縮後のファイルサイズは元のサイズと比べてどのくらいですか？

**A**: ファイルの種類によりますが、一般的に:

- テキスト・ログファイル: 60〜80%圧縮（20〜40%に削減）
- 画像・動画: ほとんど圧縮されない（すでに圧縮済みのため）
- 平均: 約70%のサイズ（30%削減）

### Q: 処理を途中で中断した場合、どうなりますか？

**A**: 進行状況は自動的に保存されます。次回実行時に、完了済みのバケットはスキップされ、続きから再開されます。

### Q: バケットを削除した後、復元できますか？

**A**: いいえ、削除は**不可逆**です。必ずDropboxでバックアップを確認してから削除してください。

---

## 注意事項

### ⚠️ 重要な警告

1. **バックアップの確認**
   - バケットを削除する前に、必ずDropboxでバックアップを確認してください
   - 圧縮ファイル、README.md、file_list.mdが全て揃っているか確認

2. **削除は不可逆**
   - 一度削除したバケットは復元できません
   - ドライランで削除予定を確認してから実行してください

3. **ディスク容量**
   - 最大バケットサイズの2倍以上の空き容量を推奨
   - 容量不足の場合、処理が失敗する可能性があります

4. **ネットワーク環境**
   - 安定したインターネット接続が必要
   - モバイル回線よりも有線・Wi-Fiを推奨

5. **認証情報の保護**
   - `.env`ファイルは絶対にGitにコミットしないでください
   - アクセストークンは他人と共有しないでください

### ベストプラクティス

1. **段階的な実行**
   - まず小規模なバケットでテストしてください
   - 問題がなければ本格的に実行してください

2. **定期的な確認**
   - 移行中は定期的にログを確認してください
   - エラーが発生していないか確認してください

3. **バックアップの検証**
   - 移行後、いくつかのファイルを実際に解凍して確認することを推奨
   - file_list.mdで内容を確認してください

4. **削除前のチェックリスト**
   - ✅ Dropboxにバックアップが存在する
   - ✅ 圧縮ファイルが完全である
   - ✅ README.mdとfile_list.mdが存在する
   - ✅ 分割ファイルが全て揃っている
   - ✅ いくつかのファイルを解凍して確認した

---

## ライセンス

MIT License

---

## 開発者向け情報

### プロジェクト構造

```
s3-to-dropbox/
├── tools/              # 4つのメインツール
│   ├── bucket_info.py
│   ├── migrate_data.py
│   ├── verify_backup.py
│   └── delete_buckets.py
├── lib/                # 共通ライブラリ
│   ├── aws_client.py
│   ├── dropbox_client.py
│   ├── compressor.py
│   ├── progress.py
│   ├── file_list.py
│   └── logger.py
├── logs/               # ログファイル
├── data/               # 進行状況・レポートファイル
├── temp/               # 一時ファイル
├── requirements.txt
├── .env.example
└── README.md
```

### 技術スタック

- **Python**: 3.14.0
- **AWS SDK**: boto3
- **Dropbox API**: dropbox
- **その他**: tqdm, humanize, colorama, python-dotenv

---

## サポート

問題が発生した場合:

1. ログファイル（`logs/`）を確認
2. 進行状況ファイル（`data/migration_progress.json`）を確認
3. このREADMEのトラブルシューティングセクションを参照

---

**最終更新**: 2025-11-04
