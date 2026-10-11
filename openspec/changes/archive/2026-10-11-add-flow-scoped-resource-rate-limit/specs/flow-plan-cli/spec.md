## ADDED Requirements

### Requirement: Text plans identify the unit and scope of rate limits
Text plan output SHALL identify each selected resource's configured body-start count and duration when a rate limit exists. It SHALL explain that the unit is asset body starts within one run rather than API request quota. Plans without rate limits SHALL remain readable.

#### Scenario: Review a resource rate limit
- **WHEN** a text plan uses a resource allowing two body starts per second
- **THEN** its resource summary shows that count, duration, and invocation unit
