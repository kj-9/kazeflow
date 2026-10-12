# Verification

2026-10-11にrootが統合確認した。

- `make test`: Python 3.13で321 tests passed（file adapter専用42件）。
- `make ci-check`: format、Ruff、tyが成功。
- `make docs-check`: strict buildとDocs検証が成功。
- Docs example: 別プロセスでseed成功、failは期待したexit 1、retry成功。producerは後続2回でmaterialized。
- `uv build --offline`、wheel metadata検証、clean core-only installed-wheel smoke成功。wheelは現行14 Python source filesと一致。
- `openspec doctor`、`openspec validate --all --strict`: 成功。
- Python 3.10–3.13 CI matrixを維持。ローカル実行は3.13。

## Scenario evidence

- 明示activation、typed falsey/date keys、unsupported output/key: 専用codec/plan/identity tests。
- Fresh process/copied root、変更identity、corruption/schema/codec、old record: 専用subprocess/manifest/load tests。
- Atomic publication: generation書き込み失敗、初回/既存reference置換失敗、同時writer/reader tests。
- 下流失敗後の再開: fresh-process executor testと3-stage example。

rootと独立review agentがmanifest validation、publication境界、trusted root下のpath handling、protocol互換性を確認し、未解決のcritical findingなし。commit、push、GitHub Issue更新は行っていない。
