## Why

Issue #2の優先順位2を進める。既存のmaterialization protocolを利用して、別プロセスでもコミット済みpartition成果物を再利用できる、明示的なローカルfile adapterが必要である。

## What Changes

- `kazeflow.file_materialization`にstdlibだけの`FileMaterialization`、明示的な`MaterializationCodec`、`JsonCodec`、`FileMaterializationError`を追加する。
- version、typed partition key、入力ID、codec識別子を含む自己記述manifestと、payloadの整合性検証を提供する。
- 成果物をimmutableな世代として保存し、atomicな参照更新後にだけcommitted recordを返す。
- fresh processでの再利用、下流停止後の再開、保存先コピーを例とテストで示す。
- Scopeは単一ローカル保存先と明示codecに限定する。Non-goals: invalidation、GC、列挙/集約、remote storage、process locking、portable RunResult provenance、pickle、自動永続化。

## Capabilities

### New Capabilities

- `file-materialization-adapter`: 明示codec、portable file保存、整合性検証とatomic publication。

### Modified Capabilities

None. 既存executor/protocol契約は変更しない。

## Impact

新規adapter module、専用tests、ユーザー向けguide/API、wheel smokeを追加する。coreは自動的に保存先を作成せず、必須runtime dependencyも追加しない。既存公開API、CLI JSON、SQLite schemaは互換性を維持する。root exportを増やさず、adapterは明示submodule importとする。
