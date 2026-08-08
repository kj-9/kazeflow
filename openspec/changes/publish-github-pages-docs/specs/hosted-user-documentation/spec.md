## ADDED Requirements

### Requirement: Task-oriented public documentation entry
The project SHALL publish a GitHub Pages documentation entry that lets a new user
identify kazeflow's purpose, install the core or optional TUI, and reach a first
script-to-plan-to-run workflow without first reading repository-internal planning
documents.

#### Scenario: New user reaches the landing page
- **WHEN** a user opens the published documentation entry
- **THEN** they see the product's scope, an install command, and links to the
  getting-started workflow and CLI reference

#### Scenario: User follows the first-flow journey
- **WHEN** a user opens the getting-started page
- **THEN** it presents an exported module-level `flow`, a plan command, and an
  explicitly approved run command in that order

### Requirement: Discoverable review and partition guidance
The hosted documentation SHALL provide navigation to CLI planning, graph output,
partition selection, run-result review, and stored-run history, without requiring
the reader to infer those topics from source layout.

#### Scenario: User needs to rerun one partition
- **WHEN** a user follows the partition guidance
- **THEN** they can find a repeatable `--partition-key` example that plans a
  selected partition before running it

#### Scenario: User needs a visual plan
- **WHEN** a user follows the graph guidance
- **THEN** they can find the text, Mermaid, and DOT plan-output choices and their
  intended review use

### Requirement: Accurate trust boundary
The hosted documentation SHALL state that loading a Python entry can execute module
top-level code, and that plan avoids invoking asset bodies only after the entry has
loaded. It MUST NOT describe plan or the documentation workflow as a sandbox or a
safe execution guarantee.

#### Scenario: User reads before running a script
- **WHEN** a user follows a hosted command sequence that loads a flow script
- **THEN** the surrounding guidance distinguishes review support from security
  review and directs the user to trust and inspect the Python source

### Requirement: Repository-controlled deployment
The documentation site SHALL be deployed from repository-managed files through a
GitHub Pages Actions workflow. Pull requests SHALL validate the static
documentation artifact without deploying it, and eligible main-branch changes SHALL
deploy the same artifact.

#### Scenario: Documentation pull request
- **WHEN** a pull request changes the Pages source or deployment workflow
- **THEN** GitHub Actions validates the static artifact and does not publish Pages

#### Scenario: Main-branch documentation update
- **WHEN** an eligible documentation change reaches `main`
- **THEN** GitHub Actions deploys the validated static artifact to GitHub Pages
