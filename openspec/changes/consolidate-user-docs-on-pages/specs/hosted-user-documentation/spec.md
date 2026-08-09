## ADDED Requirements

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
