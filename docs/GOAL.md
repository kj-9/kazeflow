# kazeflow Project Goal

> Status: Draft

## Goal

kazeflowは、人間やAIが作る小規模なPythonタスクを、外部基盤なしで、
依存関係と実行計画を人間が理解・確認できるフローとして組み立て、実行するための、
Python標準ライブラリだけで動作する軽量なタスクフローライブラリである。

kazeflowが埋めるのは、単純なPythonスクリプトと本格的なワークフロー基盤の間にある隙間である。

- 軽い処理をPythonで書き始めると、やがて複数のタスク、依存関係、並列実行、進捗確認が必要になる。
- しかし、そのためだけにサーバー、データベース、daemon、専用のデプロイ環境を導入したくはない。
- kazeflowは、普通のPythonコードを保ったまま、必要な分だけタスクフローの構造を与える。

## Product thesis

小さな処理に構造を与えるコストは、本格的なワークフロー基盤を導入するコストよりも、
十分に小さくなければならない。

AIがコードを書ける時代には、コードを生成できることだけでなく、人間がその処理を理解し、
実行前に確認し、実行後に結果を追えることが重要になる。
kazeflowはAI専用のツールにはならないが、AIが生成した処理を人間がレビューしやすい構造へ
導くことを重要な設計上の評価軸とする。

## Target user and use case

当面は作者自身の利用を基準に設計する。パッケージはPyPIで公開し、同じ課題を持つ他の利用者も使える状態を保つ。

主な利用場面は、次のような小規模なローカル処理である。

- 1つのPythonスクリプトから始まり、複数の処理ステップへ成長したタスク
- 処理間の依存関係や実行順を明確にしたいタスク
- 一部を並列実行したいタスク
- 実行前にフロー全体を確認し、実行後に成功・失敗を把握したいタスク
- 人間またはAIが作成した処理を、小さくレビュー可能な単位に分割したいタスク

## Design principles

### Script-first

利用者は普通のPython関数から始められる。
decoratorや少量の呼び出しコードを加えるだけで、スクリプトをフローへ段階的に育てられる。
専用DSLや設定ファイルを出発点にしない。

### Zero-dependency core

kazeflowのcoreはPython標準ライブラリだけで動作し、必須のランタイム依存を持たない。

見栄えのよいTUIや永続化adapterなどの追加機能は、optional dependencyまたは別パッケージとして提供できる。
extraを導入しなくても、フローの定義、検査、実行、結果の取得というcoreの価値は失われない。

### Infrastructure-free

coreの利用に、外部サービス、daemon、データベース、コンテナ、常駐プロセスを要求しない。
インストールしたライブラリを、通常のPythonプログラム内で直接利用できる。

### Inspectable before execution

実行対象、依存関係、実行順、partition、主要な実行設定を、タスクを実行せずに確認できるようにする。
表示方法とは独立した構造化された実行計画を提供し、人間とプログラムのどちらからも検査可能にする。

### Observable after execution

実行結果を構造化された値として返す。
どのタスクがいつ実行され、成功または失敗し、どれだけ時間がかかったかを追えるようにする。

### Plain Python escape hatch

タスクの本体は普通のPython関数として保つ。
kazeflowを介さない直接実行や単体テストを可能にし、利用者を独自runtimeへ閉じ込めない。

### Reviewable AI-generated flows

AIが生成したコードについても、タスクの責務、依存関係、実行対象を人間が確認しやすいAPIを選ぶ。
AIによる生成の容易さよりも、生成されたフローを人間が理解し、修正し、責任を持って実行できることを優先する。

### Anti-platform

kazeflow自身を総合的なワークフロー管理platformへ成長させない。
本格的なorchestrationが必要な利用者には、Dagsterなどの既存基盤が適している。

## Execution information

kazeflowは、実行の前後を確認するために、表示形式から独立した構造化情報を扱う。

### Flow plan

実行前のフローを表す。少なくとも、実行対象、依存関係、実行順、partition、主要な実行設定を含む。
planの確認だけでタスク本体を実行しない。

### Run result

1回の実行結果を表す。少なくとも、フロー全体と各タスクの状態、開始・終了時刻、所要時間、失敗情報を含む。
coreはrun resultを戻り値として提供し、永続化を要求しない。

### Logs

実行中の詳細な出来事を時系列で伝える。主に人間による進捗確認とデバッグに利用する。
ログはrun resultを置き換えるものではなく、run resultから要約された状態を確認したあと、必要に応じて詳細を調べるために使う。

## Optional persistence

永続的な実行履歴はcoreの責務にしない。ただし、run resultを保存する拡張ポイントは提供できる。

将来、SQLiteなどを使った任意のrun record保存機能をextraまたはadapterとして追加してよい。
その場合も、永続化は明示的に有効化され、SQLiteを利用しないcoreの動作やAPIを損なわないものとする。

## Non-goals

以下はkazeflow coreのゴールに含めない。

- schedulerや常駐daemonの提供
- 分散実行基盤やremote workerの管理
- multi-user向けのcontrol planeやWeb UI
- データベースを必須とする実行履歴管理
- 任意のPythonコードを安全に実行するsandbox
- タスク本体の副作用や安全性の自動保証
- 大規模なデータplatformやDagsterの代替になること

これらの一部と連携するadapterを提供することは否定しないが、coreへ取り込む理由にはしない。

## Success criteria

kazeflowがゴールに沿っているかを、少なくとも次の基準で判断する。

- `pip install kazeflow`で必須のサードパーティruntime dependencyが導入されない。
- 外部サービスや事前の環境構築なしに、1つのPythonファイルから利用を始められる。
- 普通のPython関数に最小限の記述を加えてフローを定義できる。
- タスク本体を実行せず、構造化されたflow planを取得・確認できる。
- 実行APIから構造化されたrun resultを取得できる。
- タスク関数をkazeflowなしで直接実行・単体テストできる。
- optional featureを取り除いても、coreの定義・検査・実行・結果取得が成立する。

## Decision filter

新しい機能や依存を追加するときは、次の問いで判断する。

1. 小さなPython処理をフローへ育てるために必要か。
2. フローを人間が理解、確認、修正しやすくするか。
3. Python標準ライブラリだけのcoreで実現できるか。
4. 外部依存が必要なら、optional featureとして分離できるか。
5. kazeflowを常駐型のplatformへ近づけていないか。

## Documentation structure

当面は、このファイルをproject goal、設計原則、non-goalsのsingle source of truthとする。

実装計画や設計詳細が増えた場合は、目的の異なる文書として分離する。

- [ROADMAP.md](./ROADMAP.md): 変更されることを前提とした実装順序と優先度
- `DESIGN.md`: 現在のarchitectureと主要な技術設計
- `decisions/`: 個別の重要な設計判断とその背景

これらを追加しても、projectの存在理由と守るべき境界はこのファイルに残す。
