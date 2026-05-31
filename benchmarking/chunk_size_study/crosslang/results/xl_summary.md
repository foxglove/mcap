### small messages, zstd

| language | write MB/s | read MB/s | read/write |
| --- | --- | --- | --- |
| C++ | 346 | 1095 | 3.2× |
| Rust | 194 | 329 | 1.7× |
| Go | 328 | 1112 | 3.4× |
| Python | 45 | 19 | 0.4× |

### small messages, none

| language | write MB/s | read MB/s | read/write |
| --- | --- | --- | --- |
| C++ | 294 | 1263 | 4.3× |
| Rust | 328 | 808 | 2.5× |
| Go | 736 | 1151 | 1.6× |
| Python | 44 | 19 | 0.4× |
| TypeScript | 91 | 78 | 0.8× |

### large messages, zstd

| language | write MB/s | read MB/s | read/write |
| --- | --- | --- | --- |
| C++ | 1741 | 10531 | 6.0× |
| Rust | 1686 | 7777 | 4.6× |
| Go | 2977 | 6171 | 2.1× |
| Python | 1079 | 4697 | 4.4× |

### large messages, none

| language | write MB/s | read MB/s | read/write |
| --- | --- | --- | --- |
| C++ | 718 | 7365 | 10.3× |
| Rust | 2469 | 7962 | 3.2× |
| Go | 2082 | 3782 | 1.8× |
| Python | 709 | 3206 | 4.5× |
| TypeScript | 405 | 780 | 1.9× |
