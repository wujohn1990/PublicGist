# PublicGist

Gist, Notes, Snippets

## tor_proxy.py

use tor as a proxy pool

## kucoin_data.py

kucoin streaming data recorder utilizing the thread-safe logging module and zstandard/gzip compression

## kucoin_stream_bokeh.py

streaming plot using bokeh with kucoin data

```bash
$ bokeh serve kucoin_stream_bokeh.py
```

## MergeSortedFiles

merge multiple sorted csv files according to specified column (it uses [csv-parser](https://github.com/vincentlaucsb/csv-parser) for parsing csv files)
