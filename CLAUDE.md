# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Kafka Connect PoC Mono-Repo

## Repo structure

```
/
├── CLAUDE.md          ← this file (repo-level rules and routing)
├── pocs/
│   └── <poc-name>/    ← each PoC is fully self-contained here
└── ...
```

## PoC isolation model

Every proof-of-concept lives in its own directory under `/pocs/<poc-name>/`.
Each PoC directory is **fully self-contained**: its own docker-compose, configs,
scripts, documentation, and CLAUDE.md.

The repo root is **generic infrastructure only** — it must never contain
PoC-specific logic, configs, or documentation.

## Rules for Claude

### Routing rule
When asked to work on a PoC, identify the target directory under `/pocs/`.
If no PoC is specified or the target is ambiguous, **stop and ask** which
`/pocs/<poc-name>/` directory to use before doing any work.

### Hard rule — no PoC logic at root
Never place PoC-specific files at the repo root. This includes but is not
limited to: docker-compose files, connector configs, SQL scripts, runbooks,
or any technology-specific setup. All of that belongs inside `/pocs/<poc-name>/`.

### Active PoC context
When working inside a PoC directory, read its local `CLAUDE.md` first — it
contains PoC-specific instructions, architecture decisions, and constraints
that override nothing here but add specificity.

## Adding a new PoC

1. Create `/pocs/<poc-name>/`.
2. Add a `CLAUDE.md` inside it describing the PoC scope, stack, and rules.
3. All implementation files go inside that directory — nothing at root.
4. The PoC name should be short, lowercase, and hyphen-separated.
