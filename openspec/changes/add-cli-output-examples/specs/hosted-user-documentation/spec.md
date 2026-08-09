## ADDED Requirements

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
