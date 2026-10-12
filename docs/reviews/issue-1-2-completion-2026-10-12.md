# Issue #1・#2 の実装完了確認

2026-10-12時点のread-only Issue本文・コメントauditとローカル統合検証。
GitHub上の両IssueはOpenで、push・PR・CI・merge・完了コメント・closeは未実行。

## Issue #1

- flow内の共有resource concurrencyとplan表示: `add-flow-scoped-resource-concurrency`
- rolling rate limitとslotを占有しない待機: `add-flow-scoped-resource-rate-limit`
- bounded retry、shared cooldown、invocation履歴: `add-resource-retry-cooldown`
- 明示circuit open、pending/retry skipと原因追跡: `add-resource-circuit-breaker`
- terminal failed/circuit-skipped partition key取得: `add-partition-outcome-queries`
- resource実行・成否・retry・観測admission wait統計: `add-resource-execution-statistics`

元Issueの再構成に従い、独立したStopPendingPartitionsではなくcircuit openを使用。
統計のwaitは最初に観測したresource admission拒否からbody開始/terminal skipまでの
合計であり、dependency待ちや未検査のglobal枠待ちは含まない。重複時間と後続の
scheduler遅延を含み得る。live result用で、portable履歴から推測しない。

## Issue #2

- 明示version/input ID、compatible committed outputの再利用、commit後の下流公開:
  `add-partition-materialization-reuse`
- 明示codec、atomic generation/reference、fresh process再利用:
  `add-file-materialization-adapter`
- executed/materializedとIDのresult/event/CLI/SQLite観測:
  `record-materialization-provenance`
- exact identityの無効化、epoch ledger、コピー後の旧artifact復活防止、inspection:
  `add-materialization-invalidation`
- 同一keyのasset列とactive key cap:
  `add-depth-first-partition-groups`
- 明示current/prior contextsのavailable collection、失敗を保持した部分集約、
  単一atomic local snapshotとreceipt: `add-available-materialization-aggregation`

plan時のadapter lookupは元Issueの任意範囲で、metadata inspectionのみ。
external sink transaction、cross-run exactly-once、full-root rollback後のrevocation保持、
任意application schemaの解釈は保証しない。これらを完了扱いにはしていない。

## 検証と互換性

- 全578テスト成功（Python3.13）、format/Ruff/ty成功。
- strict Docs build、selected13ページとsearch index確認成功。
- wheel build/metadata、Python3.12のisolated core install smoke成功。
- Python3.10–3.13 CI matrix維持。3.10/3.11の実行はGitHub CIで確認する。
- stdlib-only coreを維持し、全16 Python modulesとwheelが一致。
- OpenSpec各change完了・archive、living specs strict validation成功。
- 現行CLI plan v5、run/history v4、他v1、schema collection v7。
- result record v4（default Python projection v1）、event v4、SQLite DB v1。
- file reference/manifest v2、legacy v1読み込み、invalidation ledger v1。

両Issueの必須実装に残りは見つからない。GitHub反映とCI成功を完了判定の最後のgateとする。
AGENTS.mdの明示承認ルールに従い、承認後に完了コメントとcloseを行う。
