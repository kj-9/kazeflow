## Why

Issue #1のresource並行数制限だけでは、短時間に開始する外部処理の数を抑えられない。ROADMAPのIssue #1・#2優先waveの最初として、複数assetとpartitionで共有する明示的な時間窓の実行開始上限を追加する。

## What Changes

- `ResourcePolicy`へoptionalな`RateLimit(requests, per_seconds)`を追加する。
- 1回のrun内で同じresourceを使うasset bodyの開始数を、任意の連続した時間窓で制限する。
- 時間制限の待機はglobal/resource invocation slotを占有せず、独立したworkを継続する。
- materializationをloadしたattemptはbodyを呼ばないためpermitを消費しない。missの場合にbody開始前のpermitを取得する。
- planへ制限を表示し、plan JSONを新しいschema versionで公開する。

Scopeはasset bodyの開始数であり、内部の個々のHTTP requestを数えるものではない。retry、cooldown、circuit breaker、cross-run/process coordination、明示request permit APIは後続changeとする。

## Capabilities

### New Capabilities

- `flow-scoped-resource-rate-limit`: 時間窓の開始数制限、共有範囲、待機・cancellation契約。

### Modified Capabilities

- `flow-planning`: optional rate limitをimmutableなselected resource metadataに含める。
- `core-executor-integration`: permit待機と実body実行を分けて既存のterminal/dependency契約を保持する。
- `flow-plan-cli`: resourceの時間窓と開始数をtextで事前確認する。
- `cli-json-contracts`: rate limitを表すplan documentの新versionと移行契約。

## Impact

`resources.py`、`plan.py`、`flow.py`、public exports、CLI projection、schema/goldens、tests、Docsが対象。既存の`ResourcePolicy(max_concurrency=...)`とrate limitなしのflowはsource compatibilityを維持する。plan JSONはversionを上げ、過去schemaを保持する。coreはstdlib-onlyで、永続的なrate stateや外部基盤を追加しない。
