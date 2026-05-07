# Git Connector

The Git connector provides integration with Git repositories for watching commits and file changes, as well as writing files with automatic commits.

## Package

```
Kuestenlogik.Surgewave.Connect.Git
```

## Features

### Source Connector
- **Commits Mode**: Watch for new commits and emit metadata
- **Files Mode**: Emit file contents from each commit
- **Changes Mode**: Emit diffs/patches for each changed file
- Configurable file patterns for filtering
- Start from latest (new commits) or earliest (all commits)
- Incremental polling with offset tracking
- SSH and HTTPS authentication support

### Sink Connector
- **Write Mode**: Overwrite files with record content
- **Append Mode**: Append content to existing files
- Automatic commit with configurable intervals
- Optional auto-push to remote
- Custom commit messages (from config or record field)
- Configurable author name and email

## Source Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `topic` | String | Destination topic for git events |
| `git.repository.path` | String | Path to the Git repository |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `git.branch` | String | `main` | Branch to watch for changes |
| `git.source.mode` | String | `commits` | Source mode: `commits`, `files`, or `changes` |
| `git.poll.interval.ms` | Int | `30000` | Poll interval in milliseconds |
| `git.start.from` | String | `latest` | Where to start: `latest` or `earliest` |
| `git.max.commits.per.poll` | Int | `100` | Maximum commits to process per poll |
| `git.include.file.contents` | Boolean | `false` | Include file contents in commit events |
| `git.file.pattern` | String | | Glob pattern to filter files (e.g., `*.cs`) |
| `git.exclude.pattern` | String | | Glob pattern to exclude files (e.g., `*.log`) |
| `git.remote` | String | `origin` | Remote name for fetch operations |
| `git.username` | String | | Username for remote authentication |
| `git.password` | Password | | Password or token for remote authentication |
| `git.ssh.key.path` | String | | Path to SSH private key file |
| `git.ssh.key.passphrase` | Password | | Passphrase for SSH private key |

## Sink Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `topics` | String | Comma-separated list of topics to consume |
| `git.repository.path` | String | Path to the Git repository |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `git.branch` | String | `main` | Branch to commit to |
| `git.output.mode` | String | `write` | Output mode: `write` or `append` |
| `git.output.path` | String | | Output path template (supports `${topic}`, `${key}`, `${timestamp}`) |
| `git.file.path.field` | String | `path` | JSON field containing the file path |
| `git.file.content.field` | String | `content` | JSON field containing the file content |
| `git.auto.commit` | Boolean | `true` | Automatically commit changes |
| `git.auto.push` | Boolean | `false` | Automatically push commits to remote |
| `git.commit.message` | String | `Auto-commit from Surgewave` | Default commit message |
| `git.commit.message.field` | String | | JSON field containing the commit message |
| `git.commit.interval.ms` | Int | `60000` | Interval between auto-commits |
| `git.author.name` | String | `Surgewave Connect` | Author name for commits |
| `git.author.email` | String | `surgewave@localhost` | Author email for commits |
| `git.remote` | String | `origin` | Remote name for push operations |
| `git.username` | String | | Username for remote authentication |
| `git.password` | Password | | Password or token for remote authentication |
| `git.ssh.key.path` | String | | Path to SSH private key file |
| `git.ssh.key.passphrase` | Password | | Passphrase for SSH private key |

## Examples

### Watch Repository for Commits

```json
{
  "name": "git-commit-watcher",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Git.GitSourceConnector",
  "git.repository.path": "/repos/my-project",
  "git.branch": "main",
  "git.source.mode": "commits",
  "git.poll.interval.ms": 10000,
  "topic": "git-commits"
}
```

### Stream File Changes with Diffs

```json
{
  "name": "git-diff-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Git.GitSourceConnector",
  "git.repository.path": "/repos/my-project",
  "git.source.mode": "changes",
  "git.file.pattern": "*.cs",
  "git.exclude.pattern": "*.Designer.cs",
  "git.start.from": "earliest",
  "topic": "code-changes"
}
```

### Watch Specific File Types

```json
{
  "name": "config-file-watcher",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Git.GitSourceConnector",
  "git.repository.path": "/repos/config-repo",
  "git.source.mode": "files",
  "git.file.pattern": "*.json",
  "git.include.file.contents": true,
  "topic": "config-updates"
}
```

### Include Full File Contents

```json
{
  "name": "git-files-with-content",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Git.GitSourceConnector",
  "git.repository.path": "/repos/my-project",
  "git.source.mode": "commits",
  "git.include.file.contents": true,
  "git.max.commits.per.poll": 10,
  "topic": "git-files"
}
```

### Write Files with Auto-Commit

```json
{
  "name": "git-file-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Git.GitSinkConnector",
  "git.repository.path": "/repos/output-repo",
  "git.output.mode": "write",
  "git.file.path.field": "filename",
  "git.file.content.field": "data",
  "git.auto.commit": true,
  "git.commit.interval.ms": 30000,
  "git.author.name": "Data Pipeline",
  "git.author.email": "pipeline@example.com",
  "topics": "processed-data"
}
```

### Auto-Push to Remote

```json
{
  "name": "git-auto-push-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Git.GitSinkConnector",
  "git.repository.path": "/repos/sync-repo",
  "git.auto.commit": true,
  "git.auto.push": true,
  "git.username": "git-user",
  "git.password": "${secrets:git-token}",
  "git.commit.message": "Automated sync from Surgewave",
  "topics": "sync-data"
}
```

### Template-Based Output Paths

```json
{
  "name": "git-templated-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Git.GitSinkConnector",
  "git.repository.path": "/repos/logs-repo",
  "git.output.mode": "append",
  "git.output.path": "logs/${topic}/${timestamp}.log",
  "git.auto.commit": true,
  "git.commit.interval.ms": 300000,
  "topics": "application-logs"
}
```

## Source Record Formats

### Commits Mode

```json
{
  "sha": "abc123def456789...",
  "shortSha": "abc123d",
  "message": "Add new feature\n\nDetailed description here",
  "messageShort": "Add new feature",
  "author": {
    "name": "John Doe",
    "email": "john@example.com",
    "when": "2024-01-15T10:30:00+00:00"
  },
  "committer": {
    "name": "John Doe",
    "email": "john@example.com",
    "when": "2024-01-15T10:30:00+00:00"
  },
  "parents": ["parent-sha-1", "parent-sha-2"],
  "branch": "main",
  "filesChanged": 3,
  "files": [
    {"path": "src/Feature.cs", "status": "Added", "oldPath": null},
    {"path": "src/Utils.cs", "status": "Modified", "oldPath": null},
    {"path": "src/Old.cs", "status": "Deleted", "oldPath": null}
  ]
}
```

### Files Mode

```json
{
  "path": "src/Feature.cs",
  "oldPath": null,
  "status": "Added",
  "commit": "abc123def456789...",
  "commitMessage": "Add new feature",
  "author": "John Doe",
  "authorEmail": "john@example.com",
  "timestamp": "2024-01-15T10:30:00+00:00",
  "content": "using System;\n\npublic class Feature { }",
  "isBinary": false
}
```

### Changes Mode

```json
{
  "path": "src/Feature.cs",
  "oldPath": null,
  "status": "Modified",
  "commit": "abc123def456789...",
  "commitMessage": "Update feature",
  "author": "John Doe",
  "timestamp": "2024-01-15T10:30:00+00:00",
  "linesAdded": 10,
  "linesDeleted": 5,
  "patch": "@@ -1,5 +1,10 @@\n using System;\n+using System.Linq;\n...",
  "isBinary": false
}
```

## Sink Input Format

The sink expects JSON records with file path and content:

```json
{
  "path": "data/output.json",
  "content": "{\"key\": \"value\"}",
  "commitMessage": "Update output data"
}
```

Or raw content with path from `git.output.path` template.

## File Pattern Syntax

The connector supports glob-style patterns:

| Pattern | Description |
|---------|-------------|
| `*` | Match any characters except `/` |
| `**` | Match any characters including `/` |
| `?` | Match single character |
| `*.cs` | All C# files |
| `src/**/*.cs` | All C# files under src/ |
| `*.{json,yaml}` | JSON or YAML files |

## Change Status Values

| Status | Description |
|--------|-------------|
| `Added` | New file |
| `Modified` | File content changed |
| `Deleted` | File removed |
| `Renamed` | File moved/renamed |
| `Copied` | File copied |
| `TypeChange` | File type changed (e.g., file to symlink) |

## Authentication

### HTTPS with Token

```json
{
  "git.username": "oauth2",
  "git.password": "ghp_xxxxxxxxxxxxxxxxxxxx"
}
```

### SSH Key

```json
{
  "git.ssh.key.path": "/home/user/.ssh/id_rsa",
  "git.ssh.key.passphrase": "optional-passphrase"
}
```

## Performance Considerations

- **Poll Interval**: Balance between freshness and I/O load (10-60 seconds typical)
- **Max Commits Per Poll**: Limit to avoid large batches (50-200 commits)
- **File Patterns**: Use specific patterns to reduce processing
- **Commit Interval**: Batch writes to reduce commit frequency (30-300 seconds)
- **Auto-Push**: Be mindful of remote rate limits

## Limitations

- No support for bare repositories (requires working directory)
- No support for shallow clones with `start.from: earliest`
- Merge commits show all parent diffs
- Large binary files may impact memory usage
- SSH agent authentication not supported (use key file)
- No support for git submodules
- Remote fetch not implemented (watch local changes only)
