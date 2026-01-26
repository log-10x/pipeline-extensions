# GitHub Module — Repository Output Integration

Output extension for writing pipeline results directly to GitHub repositories. Enables automatic commit creation, pull request generation, and repository management.

## Overview

Integrates Log10x pipeline with GitHub for:

- **Direct Commits** — Write results directly to repository branches
- **Pull Requests** — Automatic PR creation with customizable templates
- **Branch Management** — Create, update, and manage branches
- **Squash Merges** — Clean commit history with merge squashing
- **Tree Operations** — Direct tree and blob manipulation

## Key Class

### GitHubOutputStream
**ByteArrayOutputStream subclass** for GitHub integration

```java
GitHubOutputStream stream = new GitHubOutputStream(
    "owner",              // Repository owner
    "repo",               // Repository name
    "target-branch");     // Branch to commit to

// Write pipeline results
stream.write(data);

// Auto-commit on close
stream.close();
```

**Capabilities:**
- Extends `ByteArrayOutputStream` for buffer accumulation
- Automatic Git commit on stream closure
- GitHub API integration via Kohsuke library
- OAuth token-based authentication

## Architecture

### Authentication Flow

```
OAuth Token
    ↓
GitHubBuilder
    ↓
GitHub API Client
    ↓
Repository Operations
```

### Commit Flow

```
1. Write data to GitHubOutputStream
2. Buffer accumulates in stream
3. Stream closes
4. Create commit:
   - Serialize accumulated data
   - Create blob in Git tree
   - Create tree object
   - Create commit object
5. Update branch reference
```

## Usage

### Basic Commit

```java
GitHubOutputStream stream = new GitHubOutputStream(
    "log-10x",
    "pipeline-extensions",
    "results");

// Write pipeline output
stream.write("Processing complete...".getBytes());
stream.write(resultData);

// Auto-commits on close
stream.close();
```

### Pull Request Creation

```java
GitHubOutputStream stream = new GitHubOutputStream(
    "log-10x",
    "pipeline-extensions",
    "feature-branch");

stream.setCreatePullRequest(true);
stream.setPullRequestTitle("Pipeline Results");
stream.setPullRequestBody(
    "## Results\n\n" +
    "- Events processed: 1,000,000\n" +
    "- Duration: 5m 23s\n" +
    "- Errors: 0");

stream.write(resultData);
stream.close();  // Creates PR
```

### Squash Merge

```java
GitHubOutputStream stream = new GitHubOutputStream(
    "log-10x", 
    "pipeline-extensions",
    "feature-branch");

stream.setSquashMerge(true);
stream.setTargetBranch("main");

// Operations merged cleanly into main
```

## Configuration

### OAuth Authentication

```java
// Via token
String token = System.getenv("GITHUB_TOKEN");
GitHubOutputStream stream = new GitHubOutputStream(...)
    .withToken(token);

// Via credentials
GitHubOutputStream stream = new GitHubOutputStream(...)
    .withCredentials(username, password);
```

### Commit Details

```java
stream.setCommitMessage("Pipeline Results: " + timestamp);
stream.setAuthor("Log10x Bot", "noreply@log10x.com");
stream.setCommitter("Log10x Bot", "noreply@log10x.com");
```

### Repository Configuration

```properties
github.owner=log-10x
github.repo=pipeline-extensions
github.token=${GITHUB_TOKEN}
github.defaultBranch=results
github.createPullRequest=true
github.squashMerge=false
```

## Implementation Details

### Stream Buffering

Data is buffered in memory during stream writes:

```
Write 1 → Buffer [data1]
Write 2 → Buffer [data1, data2]
Write 3 → Buffer [data1, data2, data3]
Close   → Commit all data as single blob
```

### Git Object Creation

On close, creates complete Git objects:

1. **Blob** — Binary object for file content
2. **Tree** — Directory listing with blob reference
3. **Commit** — Snapshot with message, author, parent

### Branch Management

Updates branch reference to new commit:

```
Before: main → Commit A
After:  main → Commit A → Commit B (new)
```

## Performance

### Throughput
- **Buffering** — Unlimited (memory constrained)
- **API calls** — ~1-5 per commit
- **Commit creation** — 1-2 seconds per operation

### Latency
- **Stream write** — <1ms per call
- **Commit creation** — 1-5 seconds
- **PR creation** — 2-10 seconds

### Concurrency
- **Repositories** — Multiple parallel commits
- **Branches** — One writer per branch recommended
- **API rate limit** — 5000 requests/hour per token

## Security

### Token Management
- OAuth tokens from environment
- Never log or expose tokens
- Use repository-specific deploy keys
- Rotate tokens regularly

### Access Control
- Repository permissions enforced
- Branch protection rules respected
- Commit signing supported
- Audit trail via GitHub

### Data Privacy
- HTTPS for all API calls
- Private repositories supported
- Token scoping (read, write, delete)

## Extension

### Custom Output Handler

```java
public class CustomGitHubOutput extends GitHubOutputStream {
    @Override
    protected void createCommit(byte[] data) {
        // Custom commit logic
        // E.g., run tests before commit
        // E.g., trigger webhooks
        super.createCommit(data);
    }
}
```

### Post-Commit Hooks

```java
stream.onCommitComplete(() -> {
    notifySlack("#pipeline", "Commit created!");
    triggerCI(owner, repo);
});
```

## Error Handling

### Authentication Failures
```java
try {
    stream = new GitHubOutputStream(owner, repo, branch);
} catch (IOException e) {
    if (e.getMessage().contains("401")) {
        // Invalid token
    }
}
```

### Network Errors
```java
stream.setMaxRetries(3);
stream.setRetryDelay(1000);  // 1 second between retries
```

### Quota Exceeded
```java
catch (IOException e) {
    if (e.getMessage().contains("403")) {
        // Rate limited - wait before retry
        Thread.sleep(3600000);  // 1 hour
    }
}
```

## Testing

### Unit Tests
- Commit object creation
- Tree structure validation
- Reference updates

### Integration Tests
- Real GitHub API calls
- Branch creation/deletion
- PR creation and merge

### Mocking
```java
@Test
public void testCommit() {
    MockGitHub github = new MockGitHub();
    stream = new GitHubOutputStream(..., github);
    
    stream.write(testData);
    stream.close();
    
    assertThat(github.getCommits()).hasSize(1);
}
```

## Limitations

### Rate Limits
- 5000 API calls/hour per token
- Plan commits accordingly
- Use batch operations

### File Size
- Single file max: ~100 MB via API
- Use multiple files for large data

### Concurrency
- Branch locks during update
- Use unique branches for parallel jobs

## Related

- [Main README](../README.md) — Cloud extensions overview
- [Camel Integration](../camel/README.md) — Stream processors
- [Index Module](../index/README.md) — Data storage

---

Enterprise-grade GitHub integration for automated repository management.
