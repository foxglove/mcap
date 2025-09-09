# フロントエンドエンジニア向け開発ガイド

このドキュメントは、MCAPプロジェクトに貢献するフロントエンドエンジニア向けのガイドです。
プロジェクトの概要、技術スタック、開発の進め方について説明します。

## 1. プロジェクト概要

MCAPは、Pub/Sub（出版/購読型）メッセージを記録・再生するためのコンテナフォーマットです。主にロボティクスの分野で利用されますが、汎用的なフォーマットであり、Webベースの可視化ツールなどでも活用されています。

フロントエンド開発者は、主に以下の2つの領域に関わることになります。

1.  **TypeScriptライブラリ**: ブラウザやNode.js環境でMCAPファイルを読み書きするためのライブラリ群です。
2.  **ドキュメンテーションサイト**: Docusaurusで構築された公式サイト（[mcap.dev](https://mcap.dev/)）の開発とメンテナンス。

## 2. 技術スタック

本リポジトリはYarn Workspacesを利用したモノリポ構成になっています。

### TypeScriptライブラリ (`typescript/`)

-   **@mcap/core**: MCAPファイルの読み書きに関する低レベルな機能を提供するコアライブラリ。
-   **@mcap/browser**: ブラウザ環境で動作させるためのラッパー。
-   **@mcap/nodejs**: Node.js環境で動作させるためのラッパー。
-   **@mcap/support**: 有名な圧縮形式などをサポートするためのライブラリ。
-   **TypeScript**: 主要な開発言語です。
-   **Jest**: テストフレームワークとして利用します。

### ドキュメンテーションサイト (`website/`)

-   **Docusaurus**: 静的サイトジェネレーター。
-   **React**: UIライブラリ。
-   **TypeScript**: DocusaurusのコンポーネントやページはTypeScriptで記述されています。

## 3. ディレクトリ構成

フロントエンドエンジニアが主に関わるディレクトリは以下の通りです。

```
.
├── typescript/      # TypeScriptライブラリのソースコード
│   ├── core/        # @mcap/core パッケージ
│   ├── browser/     # @mcap/browser パッケージ
│   ├── nodejs/      # @mcap/nodejs パッケージ
│   └── ...
├── website/         # ドキュメンテーションサイトのソースコード
│   ├── src/
│   │   ├── pages/   # ページコンポーネント
│   │   └── css/
│   └── docs/        # Markdownで記述されたドキュメント
└── package.json     # ルートのpackage.json (Yarn Workspacesの設定)
```

## 4. 開発環境のセットアップ

### 前提条件

-   Node.js
-   Yarn

### 手順

1.  **リポジトリをクローンします。**
    ```bash
    git clone https://github.com/foxglove/mcap.git
    cd mcap
    ```

2.  **依存関係をインストールします。**
    リポジトリのルートで以下のコマンドを実行すると、すべてのワークスペース（TypeScriptライブラリ、Webサイトなど）の依存関係がインストールされます。
    ```bash
    yarn install
    ```

3.  **開発サーバーを起動します。**
    ドキュメンテーションサイトのローカル開発サーバーを起動するには、以下のコマンドを実行します。
    ```bash
    yarn start
    # もしくは yarn workspace website start
    ```
    ブラウザで `http://localhost:3000` が開きます。ファイルの変更はライブで反映されます。

4.  **TypeScriptライブラリをビルドします。**
    TypeScriptライブラリに変更を加えた場合は、以下のコマンドでビルドできます。
    ```bash
    yarn typescript:build
    ```

## 5. テスト

TypeScriptライブラリのテストはJestで書かれています。以下のコマンドで、すべてのTypeScript関連のテストを実行できます。

```bash
yarn typescript:test
```
