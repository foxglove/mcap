## Compression ratio (zstd, payload / file size)

| class | 256K | 768K | 1M | 4M | 8M | 16M | 32M |
| --- | --- | --- | --- | --- | --- | --- | --- |
| small | 1.35 | 1.37 | 1.37 | 1.37 | 1.37 | 1.37 | 1.37 |
| jpeg | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 |
| pointcloud | 2.15 | 2.15 | 2.15 | 2.15 | 2.15 | 2.15 | 2.15 |
| mixed | 1.95 | 1.95 | 1.95 | 1.94 | 1.93 | 1.93 | 1.93 |

## Point-read bytes fetched (zstd, single message)

| class | 256K | 768K | 1M | 4M | 8M | 16M | 32M |
| --- | --- | --- | --- | --- | --- | --- | --- |
| small | 106 KiB | 334 KiB | 445 KiB | 1.7 MiB | 3.5 MiB | 6.9 MiB | 13.9 MiB |
| jpeg | 147 KiB | 733 KiB | 879 KiB | 3.9 MiB | 7.9 MiB | 15.9 MiB | 31.9 MiB |
| pointcloud | 679 KiB | 679 KiB | 679 KiB | 1.3 MiB | 3.3 MiB | 7.3 MiB | 14.6 MiB |
| mixed | 679 KiB | 679 KiB | 679 KiB | 1.8 MiB | 3.6 MiB | 7.9 MiB | 17.0 MiB |
