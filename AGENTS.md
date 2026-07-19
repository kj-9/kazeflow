# kazeflow Agent Instructions

このファイルは、kazeflowのリポジトリ内で作業するcoding agent向けの案内である。
ユーザーの明示的な指示を最優先し、次にこのファイル、`docs/GOAL.md`、`docs/ROADMAP.md`の順で参照する。

## 作業開始時

1. `git status --short --branch`で既存の変更を確認する。
2. `docs/GOAL.md`を読み、project goal、設計原則、non-goalsを確認する。
3. `docs/ROADMAP.md`を読み、対象milestone、依存関係、ownershipを確認する。
4. OpenSpecを使う変更では`openspec list`でactive changeを確認する。
5. 既存の変更を他agentの作業とみなし、勝手にrevert、reset、上書きしない。

## 守るべき設計境界

- kazeflow coreはPython標準ライブラリだけで動作する。必須のthird-party runtime dependencyを追加しない。
- TUI、永続化、外部連携はextraまたはadapterとして分離する。
- daemon、scheduler、database、remote worker、control plane、sandboxをcoreへ導入しない。
- 実行前の`FlowPlan`と実行後の`RunResult`を、表示や永続化から独立した構造化情報として扱う。
- asset本体は普通のPython関数として保ち、直接実行と単体テストを可能にする。
- 新しい機能は、AIが生成した処理を人間が理解・確認・修正しやすくするかを評価する。

## OpenSpec workflow

behaviorやpublic APIを変更する作業は、原則としてOpenSpec changeを先に作る。

標準の流れは次のとおり。

1. roadmapの対象milestoneを特定する。
2. `proposal.md`で目的、scope、non-goals、互換性を明記する。
3. `specs/`で外部から観測可能なrequirementsとscenarioを定義する。
4. `design.md`でmodule境界、failure semantics、migration、ownershipを定義する。
5. `tasks.md`を独立して検証可能な作業へ分割する。
6. 実装、テスト、`openspec validate`、verificationを行う。
7. 完了後にarchiveしてliving specsへ反映する。

OpenSpecの文書では、implementation detailをspecへ混ぜず、behavior-firstで書く。
仕様変更、設計判断、実装タスクを一つの未整理な文書にまとめない。

## 並列agentのルール

全agentが同じworking treeを共有する。並列作業ではfile ownershipを必ず明示する。

### 並列に向く作業

- 新規moduleと専用testsの追加
- 独立したdocumentationの更新
- 既に固定されたinterfaceに対するadapterの実装
- 調査、レビュー、テスト追加

### 単独ownerが必要なhotspot

同じwaveで複数agentが次のファイルを編集しない。

- `src/kazeflow/flow.py`
- `src/kazeflow/assets.py`
- `src/kazeflow/__init__.py`
- `pyproject.toml`
- `uv.lock`

Plan、Result、Eventなどの共有契約が先に固定されるまで、executor統合を並列実装しない。
OpenSpecの同じcapability specに対するsyncとarchiveも直列に行う。

他agentの変更を取り込む必要がある場合は、内容を確認してから自分の実装を調整する。
他agentの編集を隠すためのrevertや広範囲のformat rewriteを行わない。

agentは、明示的に依頼されない限りcommit、push、PR作成を行わない。最終的な統合とcommitはroot agentが行う。

## 実装と検証

- Pythonの対象versionは3.10〜3.13。
- 依存管理と実行にはuvを使う。
- テストは`uv run pytest`または`make test`で実行する。
- formatとlintは`make ci-check`で確認する。
- package変更時は`uv build`とclean environmentでのinstall smoke testを行う。
- OpenSpec変更時は`openspec doctor`、`openspec validate --all --strict`、必要に応じてchange verificationを行う。
- 失敗時は、どのcommandをどの条件で実行し、何が失敗したかを報告する。

変更後は、対象範囲に応じたtestsを追加し、既存のbehaviorを意図せず変更していないか確認する。
特にconcurrency、partition、failure、再実行、falseyなpartition keyについては、exactly-onceとterminal statusを検証する。

## 文書と記録

- projectの存在理由と設計境界は`docs/GOAL.md`に記録する。
- 優先順位、依存関係、parallel waveは`docs/ROADMAP.md`に記録する。
- 個別変更のwhy/how/tasksはOpenSpec changeに記録する。
- 後から再利用できる判断、調査結果、未解決事項が出た場合は、関連するGitHub Issueの有無を確認し、必要なら記録案をユーザーへ提示する。明示的な承認なしにIssueを作成・更新しない。
