# kazeflow Roadmap

> Status: M0–M10 Complete; M11–M12 Proposed

この状態はroadmap上のmilestone完了を示すものであり、[GOAL.md](./GOAL.md)のproject goalを
固定または完了にするものではない。

## Purpose

このroadmapは、[GOAL.md](./GOAL.md)で定めたproject goalを、検証可能な実装単位へ分解する。
日付によるrelease計画ではなく、変更の依存関係、完了条件、並列化できる境界を示す。

各実装変更はOpenSpec changeとして提案、合意、実装、検証、archiveする。
GOAL.mdは「なぜ、何を守るか」、このroadmapは「どの順序で進むか」、OpenSpecは
「個々の変更で何を実現するか」を扱う。

## Current state

M0〜M6を完了し、2026-07-31時点でfull test suiteが成功している。coreはPython標準ライブラリだけで動作し、
Rich TUIはoptional extra、SQLite run storageは明示的に利用するadapterとして分離されている。
`FlowPlan`、`RunResult`、execution eventsにより、実行前のreview、実行中の表示、実行後の
構造化結果と任意の永続化を分離している。READMEとexampleにはreview workflowを示し、
core-only/TUI-enabledのwheel smoke testとpackage metadata検証をrelease checkに含めている。

### Completion status

- M0: execution contractsをOpenSpecで固定し、archive済み。
- M1: `FlowPlan`、`RunResult`、execution eventのcore modelを実装済み。
- M2: executorをstructured plan/resultへ統合し、concurrency、partition、failure、再実行のsemanticsを検証済み。
- M3: Rich presentationをoptional TUIへ分離し、core observer境界を実装済み。
- M4: zero-dependency coreとcore/TUI package checksを実装済み。
- M5: reviewable flow workflowのdocumentationとexampleを整備済み。
- M6: 明示的なSQLite run-store adapter、schema version、migration、round-trip testsを実装し、archive済み。

## Next direction: a reviewable CLI

次の段階では、Pythonで定義したflowを、実行前に確認し、明示的な判断のもとで実行・記録・比較するための
CLIを整える。CLIはscript-firstなPython APIの代替ではなく、既存の`FlowPlan`と`RunResult`を人間、CI、AI agentが
扱うための薄い入口とする。

CLI自体もPython標準ライブラリだけで動作させる。TUIとSQLite保存は既存のoptional featureまたは明示的なadapterのままとし、
scheduler、daemon、remote execution、sandbox、暗黙のcacheを追加しない。これはzero-dependency、infrastructure-free、
anti-platformの境界を守るためである。

flow entry pointを読むには利用者のPython moduleをimportする必要がある。CLIはasset本体をplan時に実行しないが、
通常のPython import時副作用までは防げない。したがってCLIはsandboxや安全な実行保証ではなく、実行対象を確認するための
review支援として扱う。

## Roadmap

以下は、完了時に用いたscopeとacceptance criteriaを履歴として保持している。

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

OpenSpec change: `return-run-result`

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

### M7: Define the CLI contract

OpenSpec change: `define-cli-contracts`

CLIの公開契約を、実装より先に固定する。

対象:

- `path/to/flow.py:flow`と`package.module:flow`のentry point形式
- `Flow`と、`Flow`を返す明示的なfactoryの解決規則
- import時副作用の境界と、planがasset本体を実行しない保証
- stdout/stderr、text/JSON出力、exit codeの契約
- `plan`と`run`のreview責務、実行済みfailureとCLI/load/config errorの区別
- JSON投影でraw output、例外object、partition keyを露出しない既存record境界

完了条件:

- CLIがsecurity sandboxや自動承認機構ではないことを明記する。
- Python APIとCLIのplan/result semanticsが一致する。
- Python 3.10–3.13でstdlib-onlyのCLI smoke testがある。

### M8: Add an inspectable `plan` command

OpenSpec change: `add-flow-plan-cli`

実行せずにflowを読み込み、レビュー可能な計画を表示する。

対象:

- target、依存順、partition、実行設定を含む決定的なtext表示
- `FlowPlan`のstableでmachine-readableなJSON投影
- target、partition、max concurrencyなどの明示的なrun config指定
- entry point、cycle、missing dependency、不正configの診断

完了条件:

- `plan`はasset関数を呼ばない。
- 同じentry pointとconfigから同じtext順序とJSONを得られる。
- JSONはstdoutだけに出力し、診断はstderrへ出す。

### M9: Add a deliberate `run` command

OpenSpec change: `add-reviewed-run-cli`

表示したplanと同じ条件で実行し、構造化結果を返す。

対象:

- 実行前のplan summaryと、対話端末での明示確認
- 非対話環境での`--yes`必須化
- `RunResult`のtext/JSON要約と、成功、asset failure、CLI使用error、flow load errorを区別するexit code
- `--store`指定時だけの`SQLiteRunStore`保存
- optional Rich TUIを明示的に選ぶ入口

完了条件:

- CLI経由でもcoreはdatabaseやTUIを暗黙に起動しない。
- asset failureは既存APIと同じく構造化resultとして扱う。
- `--format json`をCIとAI agentが安定して利用できる。

### M10: Add local run-history commands

OpenSpec change: `add-run-history-cli`

SQLite adapterに明示的に保存した履歴を、Pythonコードを書かずに調べられるようにする。

対象:

- `runs list`、`runs show`、`runs compare`による一覧、詳細、差分表示
- status、開始時刻、duration、task/partition結果、保存可能metadataの比較
- schema migration errorと、保存されない情報の明確な診断

完了条件:

- raw output、例外object、partition keyを復元したように見せない。
- SQLite adapterを使わない利用者に依存や副作用を増やさない。

### M11: Stabilize the CLI for public use

OpenSpec change: `stabilize-cli-public-api`

CLIを公開利用できる契約として安定させる。

対象:

- `--help`、error message、exit code、JSON schemaの互換性方針
- READMEの最短導入、review、CI利用例
- wheelからのCLI smoke testと、core-only、TUI、SQLiteの組み合わせ検証
- release notesとCLI migration policy

完了条件:

- core-only wheel環境で`kazeflow plan`と`kazeflow run`が動作する。
- optional featureの有無による挙動が文書化・検証されている。

### M12: Make execution plans legible

OpenSpec change: `add-plan-graph-rendering`

`plan`を、実行順を列挙するだけでなく、依存関係の形まで一度に判断できる
人間向けのreview画面へ育てる。これは静的解析や安全性の保証ではなく、解決済みの
`FlowPlan`を読みやすく投影するための機能である。

対象:

- default text outputでの簡潔なplan summaryと、dependency-firstのASCII DAG表示
- 分岐、合流、複数target、partitioned taskを曖昧にしない決定的な表現
- `--format mermaid`と`--format dot`による、外部の可視化ツールへ渡せるDAG投影
- 詳細なtask/partition/config情報を必要時だけ表示する`--verbose`などのUX設計
- JSON outputの既存schemaとstdout/stderr境界を維持する互換性方針

完了条件:

- 同じ解決済みplanから常に同じtext、Mermaid、DOT表現が得られる。
- text outputだけでtarget、依存関係、実行順、partitionの有無を判断できる。
- Graphviz、Richその他のthird-party dependencyをcore CLIへ追加しない。
- graph表現のtestsと、既存JSON consumerを壊さない互換性testsがある。

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

### Wave 4: CLI contracts and projections

M7を仕様gateとする。M7のOpenSpecをarchiveするまで、CLI entry pointやJSON schemaを実装で固定しない。
M8では次を並列に進められる。

- CLI owner: `src/kazeflow/cli.py`とentry point解決
- Projection owner: plan/resultのtext・JSON projectionと専用tests
- Documentation owner: CLI guide、README、examples

`pyproject.toml`のconsole-script定義と`src/kazeflow/__init__.py`は単独ownerとする。
projectionが共有contractに触れる場合は、`flow.py`、`assets.py`のownerと先にinterfaceを合意する。

### Wave 5: Reviewed execution and local history

M8のplan contractを固定した後、M9のrun commandとM10のrun-history commandを進める。

- Run CLI owner: confirmation、exit code、`--store`、integration tests
- History owner: SQLite履歴のquery/formattingと専用tests
- Verification owner: wheel smoke、core-only/TUI/SQLite組み合わせtests

`src/kazeflow/cli.py`はM9とM10で共有するhotspotのため、同じwaveで1人だけが編集する。
SQLite adapterの保存schemaを変更する場合も、migration ownerを単独で置く。

### Wave 6: Stable, legible CLI

M11で公開CLIの互換性方針を固定した後、M12でplan projectionを改善する。

- Plan rendering owner: `src/kazeflow/cli.py`のtext、Mermaid、DOT projection
- Test owner: graph shape、deterministic order、JSON compatibilityの専用tests
- Documentation owner: README、CLI guide、Graphviz/Mermaid利用例

`src/kazeflow/cli.py`はshared hotspotのため、renderer実装は単独ownerが担当する。
JSON schemaを変更する場合はM11のpublic compatibility policyを先に更新する。

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
