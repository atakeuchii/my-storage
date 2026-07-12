# my-storage

LSM-tree（Log-Structured Merge-tree）ベースの Key-Value ストレージエンジン。Clojure 製、スクラッチ実装。

LevelDB / RocksDB と同じ**組み込み型ストレージエンジン**であり、DB サーバではない。アプリのプロセス内にライブラリとして同居し、指定ディレクトリにデータを永続化する。

## 特徴

- **Write-Ahead Log** によるクラッシュ回復（末尾破損の検出・切り捨て込み）
- **Memtable**（`sorted-map`）→ 閾値超過で **SSTable** へ flush
- **スパースインデックス** + **Bloom filter** 付き SSTable
- **Tombstone** による削除、**size-tiered コンパクション**で物理削除
- **レンジスキャン**（k-way マージによる全レイヤー統合）
- **fsync ポリシー**（耐久性と書き込み性能のトレードオフを設定可能）
- `test.check` によるプロパティベーステスト（素の `sorted-map` との等価性検証）

---

## アーキテクチャ

```
            put / get / delete / scan
                       │
                       ▼
            ┌───────────────────┐
            │  WAL (append-only) │  ← 追記 + fsync（未flush分の保険）
            └───────────────────┘
                       │
                       ▼
            ┌───────────────────┐
            │  Memtable          │  ← (atom (sorted-map))
            └───────────────────┘
                       │ 閾値超えで flush
                       ▼
            ┌───────────────────┐
            │  Immutables        │  ← flush 待ちの控え室
            └───────────────────┘
                       │ SSTable 書き出し
                       ▼
   ┌──────────┬──────────┬──────────┐
   │ SSTable  │ SSTable  │ SSTable  │  ← 不変・ソート済み・Bloom filter 付き
   └──────────┴──────────┴──────────┘
                       │ size-tiered で自動マージ
                       ▼
              Compaction（k-way merge / tombstone 物理削除）

  MANIFEST.edn … 有効な SSTable 集合の台帳（唯一の真実）

  read path: memtable → immutables(新→古) → SSTable(新→古, Bloom で枝刈り)
             最初に当たったものが勝つ（newest wins）。tombstone なら「無い」。
```

### 名前空間

| ns | 役割 | 依存 |
|---|---|---|
| `encoding` | レコードのバイト直列化、tombstone センチネル | なし |
| `bloom` | Bloom filter（BitSet + ダブルハッシュ） | なし |
| `wal` | WAL の追記 / fsync / リプレイ / ローテート | encoding |
| `sstable` | SSTable の書き出し / 読み取り / 範囲スキャン | encoding, bloom |
| `manifest` | 有効 SSTable 集合の EDN 台帳（原子的更新） | なし |
| `merge` | k-way マージ（scan と compaction で共有） | なし |
| `compaction` | マージ実行 + size-tiered 戦略 | sstable, merge |
| `core` | 公開 API、状態管理、flush / compaction の統括 | 全部 |

---

## 使い方

```clojure
(require '[my-storage.core :as db])

(def store (db/open "/var/data/mystore"
                    {:flush-threshold      10000   ; memtable の件数閾値
                     :compaction-threshold 4       ; SSTable が4枚たまったらマージ
                     :wal-fsync            100}))  ; 100件ごとに fsync

(db/put    store "user:1001" "aki")
(db/get    store "user:1001")                ;=> "aki"
(db/scan   store "user:1000" "user:2000")    ;=> ([k v] ...) ソート順
(db/delete store "user:1001")
(db/compact! store)                          ; 手動フルコンパクション
(db/stats  store)                            ; {:reads :skips :gets :read-amp}
(db/close  store)
```

### オプション

| キー | 既定 | 意味 |
|---|---|---|
| `:flush-threshold` | 1000 | memtable の件数がこれを超えると SSTable へ flush |
| `:compaction-threshold` | （未設定=無効） | 似たサイズの SSTable がこの枚数たまるとマージ |
| `:compaction-size-ratio` | 1.5 | 「サイズが近い」とみなす倍率 |
| `:wal-fsync` | `:always` | `:always` / N（N件ごと） / `:never` |
| `:bloom-fpp` | 0.01 | Bloom filter の目標偽陽性率 |
| `:index-interval` | 128 | スパースインデックスの間隔（N件ごと） |

---

## 形式仕様

### WAL レコード

```
┌─────────┬──────────┬─────────┬────────────┬─────────┐
│ keyLen  │ key      │ valLen  │ value      │ crc32   │
├─────────┼──────────┼─────────┼────────────┼─────────┤
│ int 4B  │ keyLen B │ int 4B  │ valLen B   │ int 4B  │
│ (BE)    │ (UTF-8)  │ (BE)    │ (UTF-8)    │ (BE)    │
└─────────┴──────────┴─────────┴────────────┴─────────┘
                       └ valLen = -1 → tombstone（value 部なし）
   crc32 = keyLen〜value の全バイトに対する CRC32
```

例: `put "ab" "xyz"` は 17 バイト。`delete "ab"` は 14 バイト。

破損検出は三段構え：
1. 長さ不足（途中で落ちた）→ `BufferUnderflowException`
2. 長さフィールドが負 → 明示的に弾く
3. 長さは正常だが中身が化けた → **CRC 不一致**

WAL は追記専用なので「壊れるとしたら末尾だけ」。最初に壊れたレコードに当たったら、それ以降を切り捨てる。

### SSTable

```
┌─────────────────────────────────────────┐
│ データ部                                  │
│   [keyLen][key][valLen][value] をソート順に連続
│   （tombstone は valLen = -1）             │
├─────────────────────────────────────────┤
│ インデックス部（スパース）                 │
│   [count]                                 │
│   [keyLen][key][offset(8B)] × count       │  ← index-interval 件ごとに1つ
├─────────────────────────────────────────┤
│ Bloom filter 部                           │
│   [m(8B)][k(4B)][bitsLen(4B)][bits]       │
├─────────────────────────────────────────┤
│ フッター（固定長 28B）                     │
│   [bloomOffset(8B)]                       │
│   [indexOffset(8B)]                       │
│   [entryCount(4B)]                        │
│   [magic(8B) = "MYSSTBL1"]                │
└─────────────────────────────────────────┘
```

**末尾から辿れる**設計。読むときは、フッター28B → `indexOffset` へシーク → インデックスをメモリにロード → 目的キーの直前のオフセットへ飛んで線形スキャン。

### MANIFEST.edn

```clojure
{:sstables ["sstable-...-000000000.db"     ; 古い
            "sstable-...-000000001.db"]}   ; 新しい
```

**有効な SSTable 集合の唯一の真実。** ディレクトリスキャンではなくこれを読む。更新は temp 書き込み → fsync → 原子的 rename。

---

## 設計判断とトレードオフ

### なぜ削除が tombstone なのか

SSTable は不変なので、古いファイルにある実体を物理削除できない。そこで **「消えた」という墓石を上に積み**、読み取り時に新しい順に見て最初に当たったのが墓石なら「無い」を返す。

代償は、**削除しても容量が減らない**（墓石と実体の両方が残るので、一時的に増える）。両方を本当に消すのはコンパクション時。これが実際の DB で「削除したのに容量が減らない」現象の正体。

### なぜ size-tiered なのか

似たサイズの SSTable が N 枚たまったらマージする方式。**書き込みに有利・空間効率は劣る**。対する leveled（各レベルにサイズ上限、あふれた分だけ下と重なる範囲をマージ）は読み込み・空間に有利だが書き込み増幅が大きい。学習目的で実装が素直な size-tiered を選んだ。

部分マージ時は、**最古の SSTable を含むときだけ** tombstone を物理削除する（下に古い実体が残るなら、墓石はそれを隠し続ける必要があるため）。

### なぜ `swap-vals!` なのか

`swap!` の更新関数は **CAS 競合でリトライされる**ため、中に副作用を置くと複数回実行され得る。`swap-vals!` は CAS 成功時の `[old new]` を返り値でくれるので、更新関数を純粋に保ったまま、**副作用を CAS ループの外で1回だけ**実行できる。

### なぜ WAL は flush 後に空になるのか

flush 済みデータは SSTable に固定されたので、WAL が守る対象がなくなる。ローテートで空に戻すことで、**WAL は常に「未 flush の尻尾」だけ**になり、起動時のリプレイが軽く保たれる。

### クラッシュ安全性の順序

flush: **① SSTable を書く → ② マニフェスト更新 → ③ WAL ローテート**

compaction: **① 新 SSTable を書く → ② マニフェスト差し替え → ③ 古いファイル削除**

どこで落ちても、マニフェストに載る前なら旧状態が有効（新ファイルは孤児として無視）、載った後なら新状態が有効。**データが消えるより二重になる方が安全**という判断（二重は newest-wins が吸収する）。

---

## ベンチマーク結果

20,000 件 put → 3,000 回ランダム get（`lein with-profile +bench run -m my-storage.bench`）。

### WAL fsync ポリシー（書き込み性能を支配する）

| `:wal-fsync` | 書き込み tps | 対 `:always` |
|---|---|---|
| `:always` | 283 | 1× |
| `10` | 973 | 3.4× |
| `100` | 4,395 | 15.5× |
| `1000` | 8,772 | 31× |
| `:never` | 9,537 | **34×**（天井） |

fsync 1回に **3.5ms**。`:always` は天井の 3% しか出せていない = **書き込み性能の 97% を fsync が食っていた**。グループコミット（N=1000）で天井の 92% を回復。代償は「クラッシュ時に直近 N-1 件を失い得る」こと。MySQL の `innodb_flush_log_at_trx_commit` と同じトレードオフ。

### コンパクション（書き込みと読み込みのトレードオフ）

| `:compaction-threshold` | 書き込み tps | SSTable 枚数 | get 平均 |
|---|---|---|---|
| 2 | 3,421 | 2 | 82.2 µs |
| 4 | 4,999 | 4 | 101.1 µs |
| 無効 | 6,437 | 10 | 131.8 µs |

**書き込みスループットを半分払って、読み込みを 1.6 倍速くしている。** コンパクションは「書き込みの余力を、読み込みの速さに変換する装置」。

### Bloom filter（読み込み増幅）

| `:bloom-fpp` | 存在しないキーの read-amp | ファイルサイズ |
|---|---|---|
| 0.5 | 2.00（4枚中2枚も無駄読み） | 476 KB |
| 0.1 | 0.41 | 484 KB |
| 0.01 | 0.04 | 496 KB |
| 0.001 | **0.00**（1枚も読まない） | 508 KB |

偽陽性率を下げるほど無駄読みが消え、代わりにファイルが太る。既定 0.01 で、存在しないキーの 96% が **SSTable を1枚も読まずに** nil を返す。

### スパースインデックス間隔

| `:index-interval` | get 平均 | ファイルサイズ |
|---|---|---|
| 16 | 88.1 µs | 520 KB |
| 128 | 95.3 µs | 496 KB |
| 1024 | 204.0 µs | 493 KB |

間隔を広げるとインデックスは小さくなるが、ブロック内の線形スキャンが長くなる。

> **注**: 時間（µs）はウォームアップなし・1回測定のためノイズが乗る。**カウンタ（read-amp / files / bytes）は決定的で信用できる。** 信頼できるレイテンシが必要なら criterium を使うこと。

---

## テスト

```
lein test
```

- **例ベース**（39本）: 各コンポーネントの単体・結合テスト
- **クラッシュ回復**（6本）: flush / compaction の途中で落ちた中間状態からの復旧
  - SSTable は書けたがマニフェスト更新前 → 孤児として無視、WAL から復元
  - マニフェスト更新後・WAL ローテート前 → 二重になるが newest-wins が吸収
  - compaction の新ファイルは書けたがマニフェスト差し替え前 → 旧状態が有効
  - マニフェスト差し替え後・古いファイル削除前 → 新状態が有効、旧は孤児
  - 書きかけの壊れた .db が残る → マニフェストに無いので一度も開かれない
  - 削除が compaction 途中クラッシュを跨いでも resurrect しない
- **プロパティベース**（`test.check`）: `put` / `delete` / `compact` / `restart` のランダム操作列を、自作エンジンと素の `sorted-map` の両方に流し、**任意時点の get / scan が一致すること**を検証（700ケース通過）

---

## 既知の制約

- **コンパクションが全データをメモリに載せる**。`sstable-scan` がファイル全体を読み、`compact!` が結果を `vec` で materialize するため、GB 級のデータで **OOM のリスク**がある。対処は「`sstable-scan` の遅延イテレータ化 + `write-sstable!` の1パス化（Bloom は入力 entry-count の合計で見積もり）」。
- **トランザクションなし**。複数キーの原子的更新も隔離レベルもない。
- **単一プロセス・単一スレッド想定**。atom ベースで、複数スレッドからの同時書き込みは未検証。
- **値は文字列限定**。バイト列への一般化は未実装。
- **leveled compaction は未実装**（size-tiered のみ）。上位 tier のマージにサイズ上限がないため、大きなファイルのマージが重い。
- **セカンダリインデックスなし**。キーでしか引けない。

---

## 参考

- Kleppmann, *Designing Data-Intensive Applications*, 第3章 Storage and Retrieval
- O'Neil et al., *The Log-Structured Merge-Tree (LSM-Tree)*, 1996
- Kirsch & Mitzenmacher, *Less Hashing, Same Performance*（ダブルハッシュ）
- LevelDB / RocksDB のドキュメント
