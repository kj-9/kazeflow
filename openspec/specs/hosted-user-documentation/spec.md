# Hosted user documentation

## Purpose

Provide a task-oriented public documentation site for installing and using kazeflow while keeping repository Markdown as the detailed reference.
## Requirements
### Requirement: Task-oriented public documentation entry
The project SHALL publish a GitHub Pages documentation entry that lets a new user identify kazeflow's purpose, install the core or optional TUI, and reach a first script-to-plan-to-run workflow without first reading repository-internal planning documents.

#### Scenario: New user reaches the landing page
- **WHEN** a user opens the published documentation entry
- **THEN** they see the product's scope, an install command, and links to the getting-started workflow and CLI reference

#### Scenario: User follows the first-flow journey
- **WHEN** a user opens the getting-started page
- **THEN** it presents an exported module-level `flow`, a plan command, and an explicitly approved run command in that order

### Requirement: Discoverable review and partition guidance
The hosted documentation SHALL provide navigation to CLI planning, graph output, partition selection, run-result review, and stored-run history, without requiring the reader to infer those topics from source layout.

#### Scenario: User needs to rerun one partition
- **WHEN** a user follows the partition guidance
- **THEN** they can find a repeatable `--partition-key` example that plans a selected partition before running it

#### Scenario: User needs a visual plan
- **WHEN** a user follows the graph guidance
- **THEN** they can find the text, Mermaid, and DOT plan-output choices and their intended review use

### Requirement: Accurate trust boundary
The hosted documentation SHALL state that loading a Python entry can execute module top-level code, and that plan avoids invoking asset bodies only after the entry has loaded. It MUST NOT describe plan or the documentation workflow as a sandbox or a safe execution guarantee.

#### Scenario: User reads before running a script
- **WHEN** a user follows a hosted command sequence that loads a flow script
- **THEN** the surrounding guidance distinguishes review support from security review and directs the user to trust and inspect the Python source

### Requirement: Repository-controlled deployment
The documentation site SHALL be deployed from repository-managed files through a GitHub Pages Actions workflow. Pull requests SHALL validate the static documentation artifact without deploying it, and eligible main-branch changes SHALL deploy the same artifact.

#### Scenario: Documentation pull request
- **WHEN** a pull request changes the Pages source or deployment workflow
- **THEN** GitHub Actions validates the static artifact and does not publish Pages

#### Scenario: Main-branch documentation update
- **WHEN** an eligible documentation change reaches `main`
- **THEN** GitHub Actions deploys the validated static artifact to GitHub Pages

### Requirement: Representative CLI output examples
The hosted documentation SHALL show representative successful text output for the
first-flow `plan` and `run` commands, graph projection, and stored-run inspection.
Each example MUST distinguish stable review information from values that vary per
run, and MUST direct automation users to JSON rather than parsing text layout.

#### Scenario: User reviews a first plan
- **WHEN** a user follows the hosted getting-started plan command
- **THEN** they can see a representative plan summary, dependency graph, and an
  explanation that no asset body has run

#### Scenario: User reviews a first run
- **WHEN** a user follows the hosted getting-started run command
- **THEN** they can see the preflight, approval prompt, and representative terminal
  task result with variable fields clearly marked

#### Scenario: User chooses a graph or history command
- **WHEN** a user opens the hosted CLI or results page
- **THEN** they can see representative Mermaid or stored-run output associated with
  its command

### Requirement: Authoritative user documentation surface
The GitHub Pages site SHALL be the project's single authoritative surface for
installation and end-user usage guidance. A user MUST be able to understand the
Python flow model, CLI inspection and execution, graph projections, partition
selection, result review, optional persistence, run history, exit statuses, and the
Python-loading trust boundary without following a repository Markdown user guide.

#### Scenario: User follows a hosted workflow deeply
- **WHEN** a user needs details beyond the first-flow walkthrough
- **THEN** the hosted navigation leads to the relevant complete guidance without a
  fallback link to a repository CLI, review-workflow, or SQLite user guide

#### Scenario: User lands on the repository or package index
- **WHEN** a user reads the README on GitHub or PyPI
- **THEN** they see a concise product introduction, install path, minimal reviewed
  flow, trust warning, and links to the authoritative hosted documentation

### Requirement: User and maintainer documentation separation
The repository SHALL retain project goals, roadmap, release history, contribution
material, and OpenSpec records as maintainer documentation, and MUST NOT duplicate
the hosted installation and usage reference in those documents.

#### Scenario: Maintainer inspects repository documentation
- **WHEN** a maintainer browses the repository `docs` directory
- **THEN** user-facing CLI, review-workflow, and SQLite guides are not maintained as
  parallel sources of truth

### Requirement: Documentation authority validation
The repository documentation check SHALL verify that hosted pages cover their
required topics, use local hosted navigation for user journeys, and do not link to
removed repository user guides.

#### Scenario: A hosted page regresses to a repository-guide fallback
- **WHEN** a Pages source links to a removed repository CLI, review-workflow, or
  SQLite user guide
- **THEN** the documentation validation fails before deployment

### Requirement: Searchable hierarchical documentation
The hosted documentation SHALL provide site search, hierarchical global navigation,
page-level section navigation, stable heading links, code-copy affordances, and a
responsive mobile navigation model across tutorial, guide, concept, reference,
example, and release content.

#### Scenario: User looks up one command option
- **WHEN** a user searches for or navigates to a specific CLI command or option
- **THEN** they can reach a command-scoped reference page and its relevant section
  without scanning the entire CLI contract

#### Scenario: User reads on a narrow screen
- **WHEN** the documentation is viewed at a mobile viewport
- **THEN** global and local navigation remain operable without consuming the page
  width or clipping the document content

### Requirement: Separated documentation modes
The hosted documentation SHALL distinguish sequential getting-started material,
task-oriented guides, explanatory concepts, exact CLI and Python reference,
runnable examples, and release compatibility information in its navigation and page
purpose.

#### Scenario: User moves from tutorial to reference
- **WHEN** a user completes the first reviewed flow and needs exact API or CLI detail
- **THEN** the site provides a clearly labeled reference path without duplicating
  the tutorial as the reference

### Requirement: Curated generated Python API reference
The hosted documentation SHALL render signatures and source documentation for the
supported public Python API and explicit SQLite adapter while excluding private
execution implementation details.

#### Scenario: User checks a public signature
- **WHEN** a user opens the Python API reference for a documented public symbol
- **THEN** its installed-source signature and docstring are included in the built
  documentation alongside curated usage and boundary guidance

### Requirement: Strict framework documentation build
The repository SHALL build the documentation with its locked documentation-only
dependencies in strict mode before publishing the generated GitHub Pages artifact.
Documentation tooling MUST NOT become a required kazeflow runtime dependency.

#### Scenario: Documentation source contains an unresolved reference
- **WHEN** a pull request produces a strict documentation build warning or error
- **THEN** the Pages validation fails and no deployment job publishes that artifact

#### Scenario: Core package is installed without documentation tooling
- **WHEN** the kazeflow wheel is installed normally or without dependencies
- **THEN** planning, execution, and CLI behavior do not require MkDocs, Material, or
  mkdocstrings

### Requirement: Stable hosted entry URLs
The generated site SHALL retain the established public documentation root and the
existing top-level getting-started, CLI, partition, and result/history page URLs.

#### Scenario: Existing reader follows a documented link
- **WHEN** a reader follows a previously published top-level Pages URL
- **THEN** the generated documentation serves the corresponding current content
  without requiring a repository Markdown fallback
