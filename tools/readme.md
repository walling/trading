# tools

## dataset tool

Run it:

```bash
./tools/dataset  # default it fetches trades for kraken:btc/eur
./tools/dataset --since 2020-11-05
./tools/dataset --market kraken:ada/btc
./tools/dataset --market kraken:ada/btc  --since 2020-11-05
./tools/dataset --help
```

Todo:

-   Add more commands: fetch and save datasets
-   Add dataset query API (Python)
