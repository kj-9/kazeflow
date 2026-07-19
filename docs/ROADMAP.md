# kazeflow Roadmap

> Status: Draft

## Purpose

このroadmapは、[GOAL.md](./GOAL.md)で定めたproject goalを、検証可能な実装単位へ分解する。
日付によるrelease計画ではなく、変更の依存関係、完了条件、並列化できる境界を示す。

各実装変更はOpenSpec changeとして提案、合意、実装、検証、archiveする。
GOAL.mdは「なぜ、何を守るか」、このroadmapは「どの順序で進むか」、OpenSpecは
「個々の変更で何を実現するか」を扱う。

## Current state

現在のkazeflowは、asset定義、依存関係の解決、同期・非同期taskの実行、並列数の制御、
partition、Richによる実行表示を備えている。既存の11 testsは成功している。

一方、GOAL.mdとの間には次の差分がある。

- `rich`と未使用の`netext`が必須runtime dependencyになっている。
- executorがRich TUIとloggerへ直接依存している。
- 表示から独立した構造化`FlowPlan`がない。
- 内部のasset resultはTUIだけが収集し、`run()`と`run_async()`は結果を返さない。
- dependencyをsetで扱う箇所があり、planの順序が決定的でない。
- execution、failure、partitionの一部のsemanticsが未定義である。
- 永続化可能なrun recordと、そのための安定したschemaがない。

また、現在のschedulerには、同時にreadyになったtaskまたはpartitionが`max_concurrency`を
超える場合、枠に入らなかったものが未実行のまま終了し得る問題がある。
既存testは最大同時実行数を確認するが、全taskのexactly-once実行を確認していない。

## Roadmap

### M0: Establish execution contracts

実装を分割する前に、複数の機能が共有する実行契約をOpenSpecで固定する。

最初のchangeは`define-execution-contracts`とし、少なくとも次を決める。

- flowとtaskのstatus集合: success、failed、skipped、cancelledなど
- task失敗時に、独立branchを継続するか、flow全体をfail-fastにするか
- `run()`が失敗を`RunResult`として返すか、例外を送出するoptionを持つか
- UTC wall-clock timestampとmonotonic durationの使い分け
- partition keyが未指定、空、または`0`、`""`、`False`の場合の扱い
- task outputをresultに含める範囲と、永続化対象との境界
- coreの既定表示をno-op、plain text、stdlib loggingのどれにするか
- event loop内から同期`run()`を呼んだ場合の契約

完了条件:

- OpenSpecのproposal、specs、design、tasksが揃い、strict validationを通る。
- 公開API、互換性、failure semantics、non-goalsが明記されている。
- 現行11 testsがbaselineとして維持される。

### M1: Define inspectable core models

表示や永続化から独立したcore data modelを定義する。

このmilestoneは、契約合意後に別ownerで並列実装できる。

#### Workstream A: FlowPlan

OpenSpec change: `add-flow-plan`

- target、node、dependency、決定的な実行順、partition、run configを表す。
- task本体を実行せずに作成できる。
- missing asset、cycle、不正な設定を実行前に検出する。
- 人間向けrendererとmachine-readable projectionの共通sourceになる。

完了条件:

- 同じ定義から常に同じplanが得られる。
- plan作成時にtaskの副作用が発生しない。
- target closure、cycle、missing dependency、partition、configのtestsがある。

#### Workstream B: RunResult and execution events

OpenSpec change: `add-run-result`

- flow全体、task、partitionごとの実行結果を表す。
- status、UTC start/end、duration、serializableなfailure情報を持つ。
- progress表示、logging、将来の永続化が利用できるexecution eventを定義する。
- 任意のPython outputやexception objectそのものと、保存可能なmetadataを分離する。

完了条件:

- sync、async、partition、failureの各attemptがちょうど1つのresultを持つ。
- resultとeventのdata modelがRich、SQLite、その他の外部依存を参照しない。
- JSONなどへ投影できる範囲が明確である。

### M2: Stabilize and integrate the core executor

OpenSpec change: `stabilize-core-executor`

executorが`FlowPlan`を消費して実行し、`RunResult`とexecution eventを生成する形へ統合する。
共有実行経路の競合を避けるため、このmilestoneのexecutor統合は単独ownerが担当する。

対象には次のcorrectness問題を含める。

- ready task数がconcurrency枠を超えても、全taskをexactly onceで実行する。
- partition数がconcurrency枠を超えても、全partitionをexactly onceで実行する。
- falseyなpartition keyを正しく扱う。
- partition keyが未指定または空の場合を、合意した契約どおりに扱う。
- failure後の独立branch、dependent task、実行中taskを正しく分類・終了する。
- 同じ`Flow`を再実行しても、前回outputが意図せず残らない。

完了条件:

- `run()`と`run_async()`が`RunResult`を返す。
- concurrency、partition、failure、再実行に関するcompletion testsがある。
- 全taskがterminal statusになり、未分類のtaskが残らない。
- 既存利用方法には、明示した互換性方針が適用される。

### M3: Separate presentation from execution

OpenSpec change: `extract-optional-tui`

executorからTUI、progress task id、Rich loggerを切り離す。
coreは標準ライブラリだけで動作するobserverまたはevent consumer境界を提供する。

このmilestoneでは、次を並列に進められる。

- TUI owner: Rich rendererをexecution eventのconsumerとして再構成する。
- Core observer owner: no-op、plain textまたはstdlib loggingの実装を整える。
- Test owner: TUIを導入しない環境と、TUI extraを導入した環境のsmoke testを作る。

完了条件:

- core moduleをimportしてもRichをimportしない。
- rendererなしでplan、run、result取得ができる。
- Rich rendererは明示的に選択でき、coreの実行semanticsを変更しない。

### M4: Ship the zero-dependency core

OpenSpec change: `make-core-zero-dependency`

presentation分離後にpackage metadataとrelease pipelineを変更する。

- 必須dependenciesを空にする。
- 未使用の`netext`を削除する。
- Rich UIを`kazeflow[tui]`などのoptional dependencyにする。
- clean environmentでcore-only wheelを検証する。
- public exports、README、exampleを新しいplan/result APIへ更新する。

完了条件:

- wheel metadataに必須のサードパーティ`Requires-Dist`がない。
- `pip install --no-deps`したclean environmentでimport、plan、run、result取得が成功する。
- core-onlyとTUI-enabledの両方をCIで検証する。
- GOAL.mdのsuccess criteriaをend-to-endで満たす。

### M5: Make the review workflow clear

OpenSpec change: `document-reviewable-flow`

人間またはAIが作ったflowを、plan、review、run、resultの順で扱う標準的な利用方法を整える。
AI専用機能ではなく、既存APIから得られる検査可能性を明確にする。

完了条件:

- READMEに最小exampleと、実行前reviewを含むexampleがある。
- FlowPlan、RunResult、logsの役割の違いがdocument化されている。
- security sandboxではなくreview支援であることが明記されている。
- release前にcore-only installationと公開APIのsmoke testを実行できる。

### M6: Add optional SQLite run storage

OpenSpec change: `add-sqlite-run-store`

`RunResult` schemaが安定した後に、明示的に選択するSQLite adapterを追加できる。
SQLiteはPython標準ライブラリから利用できるが、永続化機能自体はcore executionから分離する。

完了条件:

- coreは自動的にdatabase fileを作らない。
- adapterを明示的に利用した場合だけrun recordを保存する。
- schema versionとmigration方針がある。
- success、failure、partition resultをround-tripできる。
- 任意のPython output、exception、partition keyを保存する範囲が明記されている。
- adapterを利用しないcoreのAPIと挙動が変わらない。

## Parallel execution model

並列エージェント作業は、速さよりもownershipの明確さを優先する。

### Wave 0: Specification gate

`define-execution-contracts`を合意・archiveするまでは、共有executorの実装を変更しない。
同じcapability specを変更するOpenSpec changeのsyncとarchiveは直列に行う。

### Wave 1: Independent contracts

契約合意後、次の新規moduleとtestを別ownerで並列実装する。

- Plan owner: `plan.py`、`tests/test_plan.py`
- Result owner: `results.py`、`tests/test_results.py`
- Event owner: `events.py`、event contract tests

このwaveでは`flow.py`、`assets.py`、`__init__.py`を変更しない。

### Wave 2: Integration and adapters

Wave 1のinterfaceが固定された後、次を並列化する。

- Executor owner: `flow.py`、`assets.py`、`tests/test_execution.py`
- TUI owner: `tui.py`、`logger.py`、`tests/test_tui.py`
- Packaging test owner: clean installとwheel metadataのtests

integrationが必要な`flow.py`と`assets.py`は、同じwaveで1人だけが所有する。

### Wave 3: Packaging, documentation, and persistence

core integration後、次を並列化できる。

- Packaging owner: `pyproject.toml`、`uv.lock`、CI
- Documentation owner: `README.md`、examples、public API docs
- Persistence owner: SQLite adapterと専用tests

`__init__.py`、`pyproject.toml`、`uv.lock`はconflict hotspotとして、各waveで単独ownerを置く。

## OpenSpec workflow

OpenSpecは開発時だけに使い、kazeflowのPython runtime dependencyには追加しない。
このroadmap作成時点ではOpenSpec CLI 1.6.0と`spec-driven` schemaで検証している。

1. roadmap上の次のchangeを1つ選ぶ。
2. proposalで目的、scope、non-goals、該当milestone、public APIへの影響を確認する。
3. specsで外部から観測可能なbehaviorとscenarioを合意する。
4. designでmodule境界、migration、並列owner、integration pointを決める。
5. tasksを独立して検証できる作業単位へ分割する。
6. 並列化できるtaskだけを、重複しないfile ownershipでagentへ割り当てる。
7. tests、typecheck、package validationを通し、OpenSpec changeをverifyする。
8. 完了したchangeをarchiveし、living specsへ反映する。

日常的な確認には`openspec doctor`、`openspec list`、`openspec validate --all --strict`を使う。

## Definition of done

各milestoneまたはOpenSpec changeは、次を満たしたときに完了とする。

- 合意したrequirementsとscenarioに対応するtestsがある。
- `make test`と`make ci-check`が成功する。
- package変更がある場合はwheel buildとclean install testが成功する。
- OpenSpec validationとchange verificationが成功する。
- OpenSpec tasksが完了状態になっている。
- public behaviorまたは利用方法が変わる場合、documentationが更新されている。
- GOAL.mdの原則とnon-goalsに反していない。
