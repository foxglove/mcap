# `McapWriter` 詳細ドキュメント

`McapWriter`は、MCAPファイルを生成するための主要なクラスです。インスタンス化からファイルの完成まで、一連のメソッド呼び出しを通じてMCAPファイルを構築します。

## コンストラクタ

`McapWriter`をインスタンス化するには、コンストラクタにオプションオブジェクトを渡します。

```typescript
import { McapWriter, IWritable } from "@mcap/core";

const writable: IWritable = ...; // IWritableを実装したオブジェクト
const writer = new McapWriter({ writable });
```

### 解説

-   **ソースコード**: [`typescript/core/src/McapWriter.ts`, line 105](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L105)
-   **必須パラメータ**: `options`オブジェクトには、`writable`プロパティが必須です。これは`IWritable`インターフェースを実装したオブジェクトで、ライターが生成したバイナリデータを実際に書き込む先の抽象を提供します。
-   **オプション**: `writable`以外にも、`McapWriterOptions` 型で定義された多数のオプションを指定することで、ライターの挙動を細かく制御できます。

---

### `McapWriterOptions`

コンストラクタに渡すことができるオプションです。

-   **ソースコード**: [`typescript/core/src/McapWriter.ts`, line 32](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L32)

#### 主なオプション

-   `writable: IWritable` (必須)
    -   書き込み先のオブジェクト。`position(): bigint` と `write(data: Uint8Array): Promise<void>` の2つのメソッドを持つ必要があります。

-   `useChunks?: boolean` (デフォルト: `true`)
    -   `true`の場合、メッセージはチャンクにまとめて書き込まれます。これにより、インデックスを利用した効率的な読み取りが可能になります。

-   `useStatistics?: boolean` (デフォルト: `true`)
    -   `true`の場合、ファイル全体の統計情報（メッセージ数、時間範囲など）が計算され、ファイル末尾のサマリーセクションに書き込まれます。

-   `compressChunk?: (chunkData: Uint8Array) => { compression: string; compressedData: Uint8Array }`
    -   チャンクを圧縮するためのコールバック関数を指定します。圧縮アルゴリズム名（例："zstd"）と圧縮後のデータを返す必要があります。指定しない場合、チャンクは圧縮されません。
    -   **根拠**: コンストラクタ内で、このオプションは`this.#compressChunk`に代入されます ([line 136](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L136))。その後、`#finalizeChunk`メソッド内で、この関数が存在する場合に呼び出され、チャンクデータが圧縮されます ([line 546](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L546))。

-   `chunkSize?: number` (デフォルト: `1024 * 1024`)
    -   1チャンクあたりのおおよその最大サイズ（バイト単位）。`addMessage`メソッドの呼び出し後、チャンクのサイズがこの値を超えると、チャンクがファイルに書き込まれます。
    -   **根拠**: `addMessage`メソッドの最後で、チャンクビルダーの現在のサイズ (`this.#chunkBuilder.byteLength`) がこの`chunkSize`と比較され、超えていれば`#finalizeChunk`が呼び出されます ([line 527](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L527))。

---

## `start()`

MCAPファイルの書き込みを開始し、ファイルの先頭にヘッダー情報を記録します。

```typescript
await writer.start({
  profile: "my-profile",
  library: "my-library",
});
```

### 解説

-   **ソースコード**: [`typescript/core/src/McapWriter.ts`, line 231](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L231)
-   **処理内容**:
    1.  書き込みが追記モード (`appendMode`) でないことを確認します ([line 232](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L232))。
    2.  データセクションのCRC計算を初期化します ([line 235](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L235))。
    3.  MCAPのマジックナンバー (`\x89MCAP0\r\n`) を書き込みます ([line 236](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L236))。
    4.  引数で渡された`Header`レコードを書き込みます ([line 237](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L237))。
    5.  書き込んだデータを`writable`オブジェクトにフラッシュし、CRCを更新します ([lines 239-241](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L239-L241))。

---

## `registerSchema()` と `registerChannel()`

メッセージを書き込む前に、そのメッセージの構造（スキーマ）と、どのトピックに属するか（チャンネル）を登録する必要があります。これらのメソッドは、登録したスキーマとチャンネルに一意のIDを払い出し、そのIDを返します。

```typescript
const schemaId = await writer.registerSchema({
  name: "Example",
  encoding: "json",
  data: new TextEncoder().encode(JSON.stringify({ type: "object" })),
});

const channelId = await writer.registerChannel({
  schemaId,
  topic: "/example",
  messageEncoding: "json",
  metadata: new Map(),
});
```

### `registerSchema()` の解説

-   **ソースコード**: [`typescript/core/src/McapWriter.ts`, line 443](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L443)
-   **処理内容**:
    1.  新しいスキーマIDをインクリメントして生成します (`this.#nextSchemaId++`) ([line 444](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L444))。
    2.  引数で受け取ったスキーマ情報と生成したIDを、インスタンス内の`#schemas` Mapに保存します ([line 445](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L445))。
    3.  統計情報が有効な場合、スキーマ数をインクリメントします ([lines 446-448](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L446-L448))。
    4.  生成したIDを返します。

### `registerChannel()` の解説

-   **ソースコード**: [`typescript/core/src/McapWriter.ts`, line 455](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L455)
-   **処理内容**:
    1.  新しいチャンネルIDをインクリメントして生成します (`this.#nextChannelId++`) ([line 456](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L456))。
    2.  引数で受け取ったチャンネル情報と生成したIDを、インスタンス内の`#channels` Mapに保存します ([line 457](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L457))。
    3.  統計情報が有効な場合、チャンネル数をインクリメントします ([lines 458-460](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L458-L460))。
    4.  生成したIDを返します。

---

## `addMessage()`

登録されたチャンネルに新しいメッセージを書き込みます。

```typescript
await writer.addMessage({
  channelId,
  sequence: 1,
  logTime: 100n,
  publishTime: 100n,
  data: new TextEncoder().encode(JSON.stringify({ greeting: "hello" })),
});
```

### 解説

-   **ソースコード**: [`typescript/core/src/McapWriter.ts`, line 465](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L465)
-   **処理内容**:
    1.  **統計情報の更新**: 統計情報が有効な場合、メッセージ数、全体の開始・終了時刻、チャンネルごとのメッセージ数などを更新します ([lines 466-486](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L466-L486))。
    2.  **スキーマとチャンネルの遅延書き込み**: このメッセージが属するチャンネル (`channelId`) の情報がまだファイルに書き込まれていない場合、このタイミングで`Channel`レコードと、必要であれば関連する`Schema`レコードを書き込みます。これにより、`registerSchema`や`registerChannel`を呼び出した順序に関わらず、必要な情報がメッセージの前に記録されることが保証されます ([lines 489-518](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L489-L518))。
    3.  **メッセージの追加**: メッセージレコードを生成し、チャンクが有効な場合はチャンクビルダーに、無効な場合は直接レコードライターに追加します ([lines 520-525](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L520-L525))。
    4.  **チャンクの確定**: チャンクが有効で、現在のチャンクのサイズがコンストラクタで指定された`chunkSize`を超えた場合、`#finalizeChunk()`を呼び出して現在のチャンクをファイルに書き込み、新しいチャンクを開始します ([lines 527-529](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L527-L529))。

---

## `end()`

すべてのメッセージの書き込みが完了した後に呼び出し、MCAPファイルを完成させます。このメソッドは、ファイルの末尾にサマリーセクションとフッターを書き込みます。

```typescript
await writer.end();
```

### 解説

-   **ソースコード**: [`typescript/core/src/McapWriter.ts`, line 244](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L244)
-   **処理内容**:
    1.  **最終チャンクの確定**: `addMessage`でまだファイルに書き込まれていない、最後のチャンクを確定して書き込みます (`#finalizeChunk()`) ([line 245](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L245))。
    2.  **DataEndの書き込み**: データセクションの終わりを示す`DataEnd`レコードを書き込みます。これにはデータセクション全体のCRCが含まれます ([lines 252-257](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L252-L257))。
    3.  **サマリーセクションの書き込み**: ファイルのインデックス情報を含むサマリーセクションを書き込みます。これには、オプションに応じて以下のレコードが含まれます。
        -   `Schema` ([lines 264-274](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L264-L274))
        -   `Channel` ([lines 276-286](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L276-L286))
        -   `Statistics` ([lines 288-298](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L288-L298))
        -   `MetadataIndex` ([lines 302-312](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L302-L312))
        -   `AttachmentIndex` ([lines 314-324](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L314-L324))
        -   `ChunkIndex` ([lines 326-336](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L326-L336))
    4.  **SummaryOffsetの書き込み**: 各サマリーセクションの各項目へのオフセットを指す`SummaryOffset`レコードを書き込みます ([lines 342-348](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L342-L348))。
    5.  **フッターの書き込み**: サマリーセクションの開始位置と、サマリーセクション自体のCRCを含む`Footer`レコードを書き込みます ([lines 352-368](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L352-L368))。
    6.  **末尾のマジックナンバー**: ファイルの終端を示す、最後のマジックナンバーを書き込みます ([line 370](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapWriter.ts#L370))。
