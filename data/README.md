# Data

This project does not ship a static dataset. The live source is the public GitHub REST API, read by the Fabric notebooks.

## What is committed

Unit tests use small synthetic payloads in `tests/fixtures/github/`. Those files are shaped like GitHub responses and are safe to commit.

## What is not committed

- GitHub tokens and `.env` files
- Local API caches
- Exported API dumps

The Fabric path reads the token from Azure Key Vault. A local experiment can put a token in an uncommitted `.env` file. See `.env.example`.

## Directory

```text
data/
└── README.md
```

`data/cache/` and `data/landing/` are gitignored for local experiments. The Phase 1 notebooks do not write them. Lakehouse tables live in OneLake, not in this folder.
