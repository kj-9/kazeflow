## Why

Issue #1の優先順位3として、一時的な失敗後の再試行とRetry-After相当のresource共有cooldownを実装する。既存の並行数/rate permitと連携し、待機が独立branchの実行を妨げないことと、最終partition結果・実行履歴を区別できることが必要である。

## What Changes

- ResourcePolicyに最大実行回数と明示classifierを追加し、Fail/Retry decisionを提供する。
- bodyの例外だけを再試行対象にし、毎回permitを再取得する。backoff/cooldownはrun内で管理し、待機中はslotを解放する。
- 番号付きInvocationResultとinvocation lifecycle eventsを追加する。既存のlogical attemptはpartitionごとに1つとする。
- version 2 portable result/event projection、CLI plan v4とrun/history v2 schemaを公開する。SQLite database schemaはv1のまま、既存v1 recordと新規v2 recordを区別して保存・読み出す。
- Scope: 同じrunのresourceを使うasset/partition、明示classifierと秒数指定。Non-goals: HTTP解釈、自動例外判定、指数backoff/jitter、circuit breaker、process間共有、retry統計query、portable materialization provenance。

## Capabilities

### New Capabilities

- `resource-retry-cooldown`: 明示分類、bounded retries、shared cooldown、permit再取得、materialization/cancellation境界。

### Modified Capabilities

- `flow-planning`: retry metadataのinspection。
- `execution-events`: numbered invocation lifecycleとversioned records。
- `run-results`: invocation履歴とversioned lossy projections。
- `cli-json-contracts`: 新しいplan/run/history document versions。
- `sqlite-run-store`: record version独立管理とlegacy recordの読み出し。

## Impact

resources、plan、executor、AssetContext、results/events、CLI/schema/goldens、SQLite adapter、Docs/testsに変更する。必須runtime dependencyは追加しない。既存ResourcePolicy、logical result ordering、terminal event、TUI progressを維持する。RunResult.to_record()の既定v1を維持し、CLI/SQLiteは明示v2を使用する。event projectionは既定v2へ更新し、旧kindのみ明示v1を提供する。CLIの変更にはmigration案内を付ける。
