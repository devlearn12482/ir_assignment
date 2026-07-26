# Vendored simdjson

- Upstream: `https://github.com/simdjson/simdjson`
- Release tag: `v3.6.4`
- Commit: `88bf5cb6d95324751fc916475c77a53ffba0f1a8`
- Imported files: `singleheader/simdjson.h`, `singleheader/simdjson.cpp`, and `LICENSE`

SHA-256:

```text
simdjson.h   e04ca5b5e45855f4bc0a53ce387c28ad008d1602fe85464bf85700ac2f3290cb
simdjson.cpp 38d8efacdddcaa23f9500d41b5ad028374d7b4db8b78a4f3275049624e1e326e
LICENSE      ceb701e33448c7d8429be0f64745027d78700a64440b773297a65793b96dec5d
```

The dependency contract is compiled and executed by `tests/unit_tests`.
Changing this version requires the API and byte-faithfulness test to pass.
