# `McapStreamReader` 詳細ドキュメント

`McapStreamReader`は、データが断片的に到着するストリーミング環境でMCAPファイルを読み込むためのクラスです。ネットワーク経由のデータや、大きなファイルを少しずつ読み込む場合に最適です。内部にバッファを持ち、十分なデータが揃うとレコードを解析して返します。

## コンストラクタ

`McapStreamReader`をインスタンス化するには、オプションオブジェクトを渡します。オプションはすべて省略可能です。

```typescript
import McapStreamReader from "@mcap/core";

const reader = new McapStreamReader({ validateCrcs: true });
```

### 解説

-   **ソースコード**: [`typescript/core/src/McapStreamReader.ts`, line 81](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L81)
-   **パラメータ**: `McapReaderOptions` 型のオブジェクトを受け取ります。このオブジェクトですべての挙動を制御します。引数を渡さない場合、すべてのオプションはデフォルト値に設定されます。

---

### `McapReaderOptions`

コンストラクタに渡すことができるオプションです。

-   **ソースコード**: [`typescript/core/src/McapStreamReader.ts`, line 9](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L9)

#### 主なオプション

-   `includeChunks?: boolean` (デフォルト: `false`)
    -   `true`に設定すると、`nextRecord()`が`Chunk`レコードそのものを返すようになります。`false`の場合、`Chunk`レコードは内部で処理され、その中に含まれる`Schema`, `Channel`, `Message`レコードのみが返されます。
    -   **根拠**: この値はコンストラクタで`this.#includeChunks`に保存されます ([line 82](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L82))。`#read`ジェネレータ内で`Chunk`レコードに遭遇した際、このフラグが`true`の場合のみ`yield record`で`Chunk`が返されます ([line 271](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L271))。

-   `decompressHandlers?: DecompressHandlers` (デフォルト: `{}`)
    -   圧縮されたチャンクを解凍（デコンプレス）するためのハンドラ関数を、圧縮形式名をキーとしたオブジェクトとして提供します。
    -   **根拠**: `#read`ジェネレータ内で圧縮された`Chunk`に遭遇すると、`record.compression`文字列をキーとしてこのオブジェクトからハンドラを取得しようとします。ハンドラが存在しない場合、エラーがスローされます ([line 276](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L276))。

-   `validateCrcs?: boolean` (デフォルト: `true`)
    -   `true`の場合、チャンクのCRC（巡回冗長検査）を検証します。パフォーマンス向上のために`false`に設定することもできますが、データの完全性が保証されなくなります。
    -   **根拠**: `#read`ジェネレータ内でチャンクを処理する際、このフラグが`true`で、かつチャンクのCRCが0でない場合に、CRCの計算と検証が行われます ([line 282](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L282))。

-   `noMagicPrefix?: boolean` (デフォルト: `false`)
    -   `true`の場合、リーダーはストリームの先頭にあるはずのMCAPマジックナンバーをチェックしません。MCAPファイルの一部断片を読み込む場合などに使用します。
    -   **根拠**: `#read`ジェネレータの冒頭で、このフラグが`false`の場合にのみマジックナンバーの解析ループが実行されます ([line 241](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L241))。

---

## `append()` と `nextRecord()`

この2つのメソッドは、`McapStreamReader`の核となるインターフェースです。`append()`でデータを供給し、`nextRecord()`で解析済みのレコードを取り出します。

```typescript
// readerはMcapStreamReaderのインスタンス
// dataChunkはストリームから受け取ったUint8Array

reader.append(dataChunk);

let record;
while ((record = reader.nextRecord())) {
  // レコードを処理する
  console.log(record.type);
}
```

### `append()` の解説

-   **ソースコード**: [`typescript/core/src/McapStreamReader.ts`, line 111](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L111)
-   **処理内容**:
    1.  リーダーが既に完了状態 (`doneReading`) でないかチェックします ([line 112](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L112))。
    2.  `#appendOrShift`プライベートメソッドを呼び出し、新しいデータを内部バッファに追加します ([line 115](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L115))。
    3.  `#appendOrShift` ([line 118](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L118)) は、効率的なバッファ管理を行います。既存のバッファに空きがあればデータを追記し、なければ不要なデータを破棄してバッファを詰めたり、必要であればより大きな新しいバッファを確保してデータをコピーします。

### `nextRecord()` の解説

-   **ソースコード**: [`typescript/core/src/McapStreamReader.ts`, line 185](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L185)
-   **処理内容**:
    1.  リーダーが完了状態 (`doneReading`) であれば、即座に `undefined` を返します ([line 186](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L186))。
    2.  内部の`#generator` (`#read`メソッド) の`next()`を呼び出して、次のレコードの解析を試みます ([line 188](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L188))。
    3.  `#read`ジェネレータは、内部バッファに完全なレコードを解析するのに十分なデータがある場合、そのレコードを`yield`します。データが不十分な場合は、何も返さずに次のデータが`append()`されるのを待ちます。
    4.  レコードが`Channel`や`Message`の場合、一貫性チェック（例：未知のチャンネルIDを持つメッセージがないか）を行います ([lines 190-203](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L190-L203))。
    5.  ジェネレータが完了した場合（`Footer`と末尾のマジックナンバーを読み取った場合）、`#doneReading`フラグを`true`に設定します ([line 205](https://github.com/foxglove/mcap/blob/main/typescript/core/src/McapStreamReader.ts#L205))。
    6.  解析されたレコード、または解析できるレコードがない場合は`undefined`を返します。
