# IMAP Connector

The IMAP connector provides a source-only integration for reading emails from IMAP servers.

## Package

```
Kuestenlogik.Surgewave.Connect.Imap
```

## Features

### Source Connector
- Connect to IMAP servers with authentication
- SSL/TLS support with optional certificate validation
- Poll-based message retrieval
- IMAP IDLE support for push notifications (server-dependent)
- Multiple folder monitoring
- Message filtering by seen status, date, subject, and sender
- Mark as read, delete, or move messages after processing
- Include message body (plain text or HTML)
- Include attachments with configurable size limits
- Full email header extraction
- JSON output format

## Source Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `topic` | String | Destination topic for email messages |
| `imap.host` | String | IMAP server hostname |
| `imap.username` | String | IMAP authentication username |

### Authentication

| Setting | Type | Description |
|---------|------|-------------|
| `imap.password` | Password | IMAP authentication password |

### Connection Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `imap.port` | Int | `993` | IMAP server port (993 for SSL, 143 for non-SSL) |
| `imap.use.ssl` | Boolean | `true` | Use SSL/TLS connection |
| `imap.timeout.seconds` | Int | `30` | Connection and operation timeout |
| `imap.accept.invalid.certificates` | Boolean | `false` | Accept invalid SSL certificates (dev only) |

### Folder Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `imap.folder` | String | `INBOX` | IMAP folder to monitor |
| `imap.folders` | String | | Comma-separated list of folders to monitor |
| `imap.recursive` | Boolean | `false` | Recursively monitor subfolders |

### Polling Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `imap.poll.interval.ms` | Int | `30000` | Poll interval in milliseconds |
| `imap.use.idle` | Boolean | `true` | Use IMAP IDLE for push notifications |
| `imap.idle.timeout.minutes` | Int | `29` | IDLE timeout before reconnecting |
| `imap.batch.size` | Int | `100` | Maximum messages to fetch per poll |

### Message Handling

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `imap.mark.as.read` | Boolean | `false` | Mark messages as read after processing |
| `imap.delete.after.read` | Boolean | `false` | Delete messages after processing |
| `imap.move.after.read` | Boolean | `false` | Move messages to another folder after processing |
| `imap.move.to.folder` | String | | Destination folder for processed messages |
| `imap.start.from` | String | `latest` | Where to start: `latest` (new messages) or `earliest` (all messages) |

### Message Filtering

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `imap.unseen.only` | Boolean | `true` | Only fetch unseen (unread) messages |
| `imap.since` | String | | Only fetch messages since date (ISO 8601 format) |
| `imap.subject.filter` | String | | Filter messages by subject (contains match) |
| `imap.from.filter` | String | | Filter messages by sender (contains match) |

### Output Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `imap.include.body` | Boolean | `true` | Include message body in output |
| `imap.include.attachments` | Boolean | `false` | Include attachments in output (base64 encoded) |
| `imap.max.attachment.size.bytes` | Long | `10485760` | Maximum attachment size to include (10MB) |
| `imap.prefer.html` | Boolean | `false` | Prefer HTML body over plain text when available |

## Examples

### Basic Email Reading

```json
{
  "name": "email-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Imap.ImapSourceConnector",
  "imap.host": "imap.example.com",
  "imap.username": "user@example.com",
  "imap.password": "${secrets:imap-password}",
  "topic": "incoming-emails"
}
```

### Gmail Configuration

```json
{
  "name": "gmail-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Imap.ImapSourceConnector",
  "imap.host": "imap.gmail.com",
  "imap.port": 993,
  "imap.use.ssl": true,
  "imap.username": "your-email@gmail.com",
  "imap.password": "${secrets:gmail-app-password}",
  "topic": "gmail-inbox",
  "imap.mark.as.read": true
}
```

### Office 365 Configuration

```json
{
  "name": "o365-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Imap.ImapSourceConnector",
  "imap.host": "outlook.office365.com",
  "imap.port": 993,
  "imap.use.ssl": true,
  "imap.username": "user@company.onmicrosoft.com",
  "imap.password": "${secrets:o365-password}",
  "topic": "o365-emails"
}
```

### Monitor Specific Folder

```json
{
  "name": "invoices-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Imap.ImapSourceConnector",
  "imap.host": "imap.example.com",
  "imap.username": "user@example.com",
  "imap.password": "${secrets:imap-password}",
  "imap.folder": "Invoices",
  "topic": "invoice-emails"
}
```

### Archive After Processing

```json
{
  "name": "process-and-archive",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Imap.ImapSourceConnector",
  "imap.host": "imap.example.com",
  "imap.username": "user@example.com",
  "imap.password": "${secrets:imap-password}",
  "imap.mark.as.read": true,
  "imap.move.after.read": true,
  "imap.move.to.folder": "Processed",
  "topic": "emails"
}
```

### Filter by Sender and Subject

```json
{
  "name": "billing-emails",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Imap.ImapSourceConnector",
  "imap.host": "imap.example.com",
  "imap.username": "user@example.com",
  "imap.password": "${secrets:imap-password}",
  "imap.from.filter": "billing@vendor.com",
  "imap.subject.filter": "Invoice",
  "topic": "vendor-invoices"
}
```

### Include Attachments

```json
{
  "name": "attachments-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Imap.ImapSourceConnector",
  "imap.host": "imap.example.com",
  "imap.username": "user@example.com",
  "imap.password": "${secrets:imap-password}",
  "imap.include.attachments": true,
  "imap.max.attachment.size.bytes": 5242880,
  "topic": "emails-with-attachments"
}
```

### Process All Historical Emails

```json
{
  "name": "historical-import",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Imap.ImapSourceConnector",
  "imap.host": "imap.example.com",
  "imap.username": "user@example.com",
  "imap.password": "${secrets:imap-password}",
  "imap.start.from": "earliest",
  "imap.unseen.only": false,
  "imap.batch.size": 500,
  "topic": "archived-emails"
}
```

### Date Range Filter

```json
{
  "name": "recent-emails",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Imap.ImapSourceConnector",
  "imap.host": "imap.example.com",
  "imap.username": "user@example.com",
  "imap.password": "${secrets:imap-password}",
  "imap.since": "2024-01-01",
  "topic": "emails-2024"
}
```

## Output Record Format

### Basic Email

```json
{
  "uid": 12345,
  "messageId": "<abc123@example.com>",
  "subject": "Hello World",
  "from": "sender@example.com",
  "to": "recipient@example.com",
  "cc": null,
  "bcc": null,
  "replyTo": null,
  "date": "2024-01-15T10:30:00Z",
  "internalDate": "2024-01-15T10:30:15Z",
  "folder": "INBOX",
  "size": 5432,
  "flags": "Seen",
  "body": "This is the email body content.",
  "bodyType": "text",
  "headers": {
    "From": "sender@example.com",
    "To": "recipient@example.com",
    "Subject": "Hello World",
    "Date": "Mon, 15 Jan 2024 10:30:00 +0000",
    "Content-Type": "text/plain; charset=utf-8",
    "Message-ID": "<abc123@example.com>"
  }
}
```

### Email with HTML Body

```json
{
  "uid": 12346,
  "messageId": "<def456@example.com>",
  "subject": "Welcome!",
  "from": "noreply@service.com",
  "to": "user@example.com",
  "date": "2024-01-15T11:00:00Z",
  "folder": "INBOX",
  "body": "<html><body><h1>Welcome!</h1></body></html>",
  "bodyType": "html",
  "headers": { ... }
}
```

### Email with Attachments

```json
{
  "uid": 12347,
  "messageId": "<ghi789@example.com>",
  "subject": "Report",
  "from": "reports@company.com",
  "to": "user@example.com",
  "date": "2024-01-15T12:00:00Z",
  "folder": "INBOX",
  "body": "Please find the report attached.",
  "bodyType": "text",
  "attachments": [
    {
      "filename": "report.pdf",
      "contentType": "application/pdf",
      "size": 102400,
      "content": "base64-encoded-content..."
    },
    {
      "filename": "large-file.zip",
      "contentType": "application/zip",
      "size": 52428800,
      "truncated": true
    }
  ],
  "headers": { ... }
}
```

## Record Headers

Each produced record includes the following headers:

| Header | Description |
|--------|-------------|
| `imap.folder` | Source folder name |
| `imap.uid` | IMAP unique identifier |
| `imap.host` | IMAP server hostname |

## IMAP Providers

### Common Provider Settings

| Provider | Host | Port | SSL |
|----------|------|------|-----|
| Gmail | imap.gmail.com | 993 | Yes |
| Outlook.com | outlook.office365.com | 993 | Yes |
| Office 365 | outlook.office365.com | 993 | Yes |
| Yahoo Mail | imap.mail.yahoo.com | 993 | Yes |
| iCloud | imap.mail.me.com | 993 | Yes |
| AOL | imap.aol.com | 993 | Yes |

### Gmail App Passwords

For Gmail, you need to use an App Password instead of your regular password:
1. Enable 2-Step Verification on your Google account
2. Generate an App Password at https://myaccount.google.com/apppasswords
3. Use the 16-character App Password as `imap.password`
4. Enable IMAP access in Gmail settings: Settings > See all settings > Forwarding and POP/IMAP

### OAuth2 Note

OAuth2 authentication is not currently supported. For services requiring OAuth2, use application-specific passwords where available.

## Error Handling

- Connection failures trigger automatic reconnection on next poll
- Authentication failures log error and retry on next poll
- Invalid messages are skipped with error logged
- Message processing errors don't affect other messages in batch
- Post-processing failures (mark read, delete, move) are logged but don't block

## Performance Considerations

- **Batch Size**: Higher values reduce connection overhead but increase memory usage
- **Poll Interval**: Balance between latency and server load
- **IDLE Support**: Use when available for real-time notifications
- **Include Attachments**: Disable if not needed to reduce memory and bandwidth
- **Unseen Only**: Enable to skip already processed messages
- **Start From**: Use `latest` for ongoing monitoring, `earliest` for one-time import

## Limitations

- Source-only connector (use SMTP connector for sending)
- No OAuth2 authentication support (use app passwords)
- Single folder per connector instance (use multiple connectors for multiple folders)
- No support for nested MIME part iteration
- Large attachments may exceed memory limits
- IDLE support depends on server capabilities
- No support for search by specific header values

## Companion Connector

For sending emails, use the [SMTP Connector](smtp.md).
