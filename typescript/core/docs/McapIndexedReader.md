# `McapIndexedReader` 詳細ドキュメント

`McapIndexedReader`は、ファイル全体にランダムアクセス可能な（シーク可能な）ソースからMCAPファイルを効率的に読み込むためのクラスです。ファイルシステム上のファイルなどが典型的なユースケースです。

ストリーミングリーダーとは異なり、最初にファイルのインデックス情報を読み込むことで、ファイル全体をスキャンすることなく、特定のメッセージに高速にアクセスできます。

## `Initialize()` (静的メソッド)

`McapIndexedReader`のインスタンスは、コンストラクタではなく、非同期の静的メソッド `Initialize` を通じて生成します。このメソッドは、MCAPファイルのインデックスを読み取り、リーダーを完全に準備した状態で返します。

```typescript
import { McapIndexedReader, IReadable } from "@mcap/core";

const readable: IReadable = ...; // IReadableを実装したオブジェクト
const reader = await McapIndexedReader.Initialize({ readable });
```

### 解説

-   **ソースコード**: [`typescript/core/src/McapIndexedReader.ts`, line 108](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapIndexedReader.ts#L108)
-   **パラメータ**:
    -   `readable: IReadable`: 読み込み対象のソース。`size(): Promise<bigint>`と`read(offset: bigint, size: bigint): Promise<Uint8Array>`の2つのメソッドを持つ必要があります。
    -   `decompressHandlers?: DecompressHandlers`: 圧縮チャンクの解凍ハンドラ。
-   **処理内容**:
    `Initialize`メソッドは、効率的なアクセスのために、ファイルのメタデータとインデックス情報を事前に読み込んで解析します。
    1.  **ヘッダーの読み込み**: ファイルの先頭から`Header`レコードを読み込み、検証します ([lines 121-150](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapIndexedReader.ts#L121-L150))。
    2.  **フッターの読み込み**: ファイルの末尾から`Footer`レコードと末尾のマジックナンバーを読み込み、検証します ([lines 156-210](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapIndexedReader.ts#L156-L210))。`Footer`には、サマリーセクションの開始位置 (`summaryStart`) が記録されています。
    3.  **サマリーセクションの読み込み**: `Footer`の情報をもとに、`DataEnd`レコードからフッターの直前までのサマリーセクション全体を一度に読み込みます ([lines 229-233](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapIndexedReader.ts#L229-L233))。
    4.  **サマリーのCRC検証**: サマリーセクションのCRC（巡回冗長検査）を計算し、`Footer`に記録されている値と一致するかを検証します ([lines 234-245](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapIndexedReader.ts#L234-L245))。
    5.  **インデックスレコードの解析**: サマリーセクション内の各レコード（`Schema`, `Channel`, `ChunkIndex`など）を解析し、インスタンスのプロパティ（`channelsById`, `chunkIndexes`など）に格納します ([lines 254-307](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapIndexedReader.ts#L254-L307))。
    6.  **インスタンスの生成**: すべてのインデックス情報を解析後、それらを引数としてプライベートコンストラクタを呼び出し、準備完了状態の`McapIndexedReader`インスタンスを生成して返します ([lines 309-322](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapIndexedReader.ts#L309-L322))。

---

## `readMessages()`

ファイルに記録されているメッセージを非同期で読み込むためのジェネレータメソッドです。`for-await-of`構文と共に使用します。インデックス情報を活用するため、特定のトピックや時間範囲でメッセージを効率的にフィルタリングできます。

```typescript
// readerは初期化済みのMcapIndexedReaderインスタンス

// すべてのメッセージを読み込む
for await (const message of reader.readMessages()) {
  console.log(message.data);
}

// トピックと時間範囲でフィルタリング
for await (const message of reader.readMessages({
  topics: ["/imu"],
  startTime: 1640995200000000000n, // BigIntナノ秒
  endTime: 1640995260000000000n,   // BigIntナノ秒
})) {
  // ...
}
```

### 解説

-   **ソースコード**: [`typescript/core/src/McapIndexedReader.ts`, line 325](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapIndexedReader.ts#L325)
-   **パラメータ**: オプションオブジェクト
    -   `topics?: string[]`: 読み込むトピックのリスト。指定しない場合はすべてのトピックが対象。
    -   `startTime?: bigint`: メッセージのログ時刻 (`logTime`) の下限（ナノ秒）。
    -   `endTime?: bigint`: メッセージのログ時刻 (`logTime`) の上限（ナノ秒）。
    -   `reverse?: boolean`: `true`にするとメッセージを降順で返す。
-   **処理内容**:
    1.  **チャンネルのフィルタリング**: `topics`が指定された場合、該当するトピックを持つチャンネルIDのセットを作成します ([lines 339-346](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapIndexedReader.ts#L339-L346))。
    2.  **チャンクの選別**: `startTime`と`endTime`の範囲に収まるメッセージを含むチャンクのみを選別します。この際、各チャンクのメタデータ（`messageStartTime`, `messageEndTime`）が利用されます ([lines 349-357](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapIndexedReader.ts#L349-L357))。
    3.  **ヒープ構造の利用**: 選別されたチャンクを、メッセージの開始時刻が最も早いものが頂点に来るように、最小ヒープ（min-heap）に追加します。これにより、複数のチャンクにまたがるメッセージを時系列順に効率的に処理できます ([line 348](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapIndexedReader.ts#L348))。
    4.  **遅延読み込みとイテレーション**:
        -   ループのたびに、ヒープの頂点にあるチャンク（次に読み込むべきメッセージが含まれるチャンク）からメッセージを1つ取り出します ([line 371](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapIndexedReader.ts#L371))。
        -   そのチャンクのデータ（`Chunk`レコード）やメッセージインデックスがまだメモリに読み込まれていない場合は、このタイミングで初めて`readable`から読み込みます ([lines 364, 374](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapIndexedReader.ts#L364-L374))。これにより、不要なデータの読み込みを最小限に抑えます。
        -   取り出したメッセージを`yield`で返します ([line 396](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapIndexedReader.ts#L396))。
        -   チャンクにまだ読み込むべきメッセージが残っていれば、ヒープを再構成します。なければ、ヒープからそのチャンクを取り除きます ([lines 398-406](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapIndexedReader.ts#L398-L406))。
