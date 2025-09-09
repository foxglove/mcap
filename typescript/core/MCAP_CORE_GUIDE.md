# `@mcap/core` 詳細ガイド

`@mcap/core`は、TypeScript環境でMCAPファイルを低レベルで操作するためのライブラリです。MCAPファイルの書き込み、ストリーミング読み込み、インデックスを利用した効率的な読み込みの3つの主要機能を提供します。

このドキュメントでは、それぞれの機能について、その目的と具体的な使用方法を解説します。

## 主な機能

-   **`McapWriter`**: MCAPファイルをゼロから構築するためのクラスです。スキーマ、チャネル、メッセージなどを順次書き込み、最終的にインデックスを含んだ完全なMCAPファイルを生成します。
-   **`McapStreamReader`**: ネットワーク経由のデータなど、データ全体が一度に利用できないストリーミング形式のMCAPを読み込むためのクラスです。受け取ったデータを逐次解析します。
-   **`McapIndexedReader`**: ファイルシステム上のファイルなど、ランダムアクセスが可能なMCAPファイルを効率的に読み込むためのクラスです。最初にファイル末尾のインデックス情報を読み込むことで、特定のメッセージに高速にアクセスできます。

---

*このガイドの以降のセクションでは、各クラスの具体的な使い方をコード例と共に詳述します。*

## `McapWriter` を使用したファイルの書き込み

`McapWriter` は、MCAPファイルを生成するためのクラスです。`IWritable` インターフェースを満たす書き込み可能なオブジェクト（例：Node.jsの`fs.WriteStream`や自作のバッファクラス）を渡して使います。

### 主なステップ

1.  **`McapWriter`インスタンスの作成**: 書き込み先となる`writable`オブジェクトを指定します。
2.  **`start()`**: ファイルのヘッダー情報を書き込み、書き込み処理を開始します。
3.  **`registerSchema()`**: メッセージの構造を定義するスキーマを登録し、`schemaId` を取得します。
4.  **`registerChannel()`**: メッセージが属するチャンネル（トピックやエンコーディング情報など）を登録し、`channelId` を取得します。
5.  **`addMessage()`**: 実際のメッセージデータを、対応する`channelId`と共に書き込みます。
6.  **`end()`**: すべてのメッセージの書き込みが終わったら、フッターやサマリー情報（各種インデックスなど）を書き込み、ファイルを完成させます。

### コード例

以下の例は、インメモリのバッファに簡単なMCAPファイルを書き込む方法を示しています。

```typescript
import { McapWriter } from "@mcap/core";
import { IWritable } from "@mcap/core";

// 1. IWritableインターフェースを実装した書き込み先クラスを定義
class MemoryWriter implements IWritable {
  buffer: Uint8Array = new Uint8Array();

  position(): bigint {
    return BigInt(this.buffer.length);
  }

  async write(data: Uint8Array): Promise<void> {
    const newBuffer = new Uint8Array(this.buffer.length + data.length);
    newBuffer.set(this.buffer);
    newBuffer.set(data, this.buffer.length);
    this.buffer = newBuffer;
  }
}

async function writeExample() {
  const memoryWriter = new MemoryWriter();

  // 2. McapWriterのインスタンスを作成
  const writer = new McapWriter({ writable: memoryWriter });

  // 3. ヘッダーを書き込み、ライブラリ情報などを指定
  await writer.start({
    profile: "",
    library: "mcap-core-example",
  });

  // 4. スキーマを登録
  const schemaId = await writer.registerSchema({
    name: "Example",
    encoding: "json",
    data: new TextEncoder().encode(JSON.stringify({ type: "object" })),
  });

  // 5. チャンネルを登録
  const channelId = await writer.registerChannel({
    schemaId,
    topic: "/example",
    messageEncoding: "json",
    metadata: new Map(),
  });

  const textEncoder = new TextEncoder();

  // 6. メッセージを追加
  await writer.addMessage({
    channelId,
    sequence: 1,
    logTime: 100n,
    publishTime: 100n,
    data: textEncoder.encode(JSON.stringify({ greeting: "hello" })),
  });

  await writer.addMessage({
    channelId,
    sequence: 2,
    logTime: 101n,
    publishTime: 101n,
    data: textEncoder.encode(JSON.stringify({ greeting: "world" })),
  });

  // 7. フッターとインデックスを書き込み、ファイルを完成させる
  await writer.end();

  console.log(`MCAP file written to buffer, size: ${memoryWriter.buffer.length} bytes`);
  // memoryWriter.buffer にMCAPファイルのバイナリデータが格納されている
}

writeExample();
```

## `McapStreamReader` を使用したストリーミング読み込み

`McapStreamReader` は、データが断片的に到着するようなストリーミング環境でMCAPを読み込むためのクラスです。例えば、ネットワーク越しに受信したデータや、大きなファイルを少しずつ読み込む場合に使用します。

### 主なステップ

1.  **`McapStreamReader`インスタンスの作成**: 必要に応じて、解凍ハンドラなどのオプションを指定します。
2.  **`append()`**: 新しいデータチャンク（`Uint8Array`）が到着するたびに、このメソッドを呼び出してリーダーの内部バッファに追加します。
3.  **`nextRecord()`**: `append()`を呼び出した後、このメソッドをループで呼び出します。解析可能なレコードがバッファにあれば、それを返します。レコードがなければ `undefined` を返します。

### コード例

以下の例は、MCAPファイルのバイナリデータ（例えば、前の`McapWriter`の例で生成したもの）を、小さなチャンクに分割してストリームリーダーに供給する方法を示しています。

```typescript
import { McapStreamReader, McapTypes } from "@mcap/core";

function readStreamExample(mcapBuffer: Uint8Array) {
  // 1. McapStreamReaderのインスタンスを作成
  const reader = new McapStreamReader();

  let bytesRead = 0;
  const chunkSize = 10;

  // 2. データを小さなチャンクに分割して、順次リーダーに供給
  while (bytesRead < mcapBuffer.length) {
    const chunk = mcapBuffer.subarray(bytesRead, bytesRead + chunkSize);
    reader.append(chunk);
    bytesRead += chunk.length;

    console.log(`Appended ${chunk.length} bytes to the stream reader.`);

    // 3. レコードが解析可能になるまでnextRecord()を呼び出す
    let record: McapTypes.TypedMcapRecord | undefined;
    while ((record = reader.nextRecord())) {
      console.log(`Read record: ${record.type}`);
      if (record.type === "Message") {
        // 必要であれば、メッセージのペイロードをデコード
        const messageData = new TextDecoder().decode(record.data);
        console.log(`  Message content: ${messageData}`);
      }
    }
  }

  console.log("Finished reading stream.");
  // reader.done() が true になる
}

// McapWriterの例で生成したmcapBufferを渡すことを想定
// readStreamExample(memoryWriter.buffer);
```

## `McapIndexedReader` を使用したインデックス読み込み

`McapIndexedReader` は、ファイル全体にランダムアクセス可能な場合に、MCAPファイルを効率的に読み込むためのクラスです。最初にファイルのインデックス情報を読み込むため、ファイル全体をスキャンすることなく、特定のトピックや時間範囲のメッセージに高速にアクセスできます。

`IReadable` インターフェース（`size()`と`read()`メソッドを持つ）を実装したオブジェクトを必要とします。

### 主なステップ

1.  **`McapIndexedReader.Initialize()`**: `IReadable`オブジェクトを渡して、非同期でリーダーを初期化します。この静的メソッドは、ファイルのヘッダー、フッター、そしてサマリー（インデックス）部分を読み込み、リーダーインスタンスを返します。
2.  **`readMessages()`**: メッセージを読み込むための非同期ジェネレータメソッドを呼び出します。オプションで`topics`、`startTime`、`endTime`を指定して、結果をフィルタリングできます。
3.  **`for-await-of`ループ**: 非同期ジェネレータからメッセージを一つずつ受け取って処理します。

### コード例

以下の例は、`McapWriter`の例で生成したMCAPファイルのバッファを、`IReadable`インターフェースを介してインデックスリーダーで読み込む方法を示しています。

```typescript
import { McapIndexedReader } from "@mcap/core";
import { IReadable, McapTypes } from "@mcap/core";

// 1. IReadableインターフェースを実装した読み込み元クラスを定義
class MemoryReader implements IReadable {
  constructor(private buffer: Uint8Array) {}

  async size(): Promise<bigint> {
    return BigInt(this.buffer.length);
  }

  async read(offset: bigint, size: bigint): Promise<Uint8Array> {
    return this.buffer.subarray(Number(offset), Number(offset + size));
  }
}


async function readIndexedExample(mcapBuffer: Uint8Array) {
  const memoryReader = new MemoryReader(mcapBuffer);

  // 2. 静的メソッドでリーダーを初期化
  const reader = await McapIndexedReader.Initialize({ readable: memoryReader });

  // 3. 全てのメッセージを読み込んで表示
  console.log("Reading all messages:");
  for await (const message of reader.readMessages()) {
    console.log(
      `  Message on topic ${reader.channelsById.get(message.channelId)!.topic}:`,
      JSON.parse(new TextDecoder().decode(message.data))
    );
  }

  // 4. 特定のトピックのメッセージをフィルタリングして読み込み
  console.log("\nReading messages on topic '/example':");
  for await (const message of reader.readMessages({ topics: ["/example"] })) {
     console.log(
      `  Message on topic ${reader.channelsById.get(message.channelId)!.topic}:`,
      JSON.parse(new TextDecoder().decode(message.data))
    );
  }
}

// McapWriterの例で生成したmcapBufferを渡すことを想定
// readIndexedExample(memoryWriter.buffer);
```

## 補助的な型と定数

`@mcap/core`は、主要なクラスに加えて、便利な型定義と定数をエクスポートしています。

-   **`McapTypes`**:
    `McapTypes.TypedMcapRecord` は、MCAPファイル内に存在しうるすべてのレコード（`Header`, `Channel`, `Message`, `Chunk`など）の型を含む共用体型です。また、`McapTypes.Channel` や `McapTypes.Message` のように、個別のレコード型にアクセスすることもできます。リーダーから返されるレコードの型を扱う際に便利です。

-   **`McapConstants`**:
    MCAP仕様で定義されている定数を含みます。例えば、`McapConstants.Opcode.MESSAGE` のように、各レコードタイプに対応するオペコードにアクセスできます。低レベルな処理やデバッグ時に役立ちます。

```typescript
import { McapTypes, McapConstants } from "@mcap/core";

function printRecordInfo(record: McapTypes.TypedMcapRecord) {
  if (record.type === "Message") {
    // recordは Message 型として扱える
    console.log(`Message record found on channel ${record.channelId}`);
  } else if (record.type === "Header") {
    console.log(`Header record found with profile: ${record.profile}`);
  }
}
```
