# SMTP Connector

The SMTP connector provides a sink-only integration for sending emails via SMTP servers.

## Package

```
Kuestenlogik.Surgewave.Connect.Smtp
```

## Features

### Sink Connector
- Send emails via SMTP with authentication
- SSL/TLS and STARTTLS support
- HTML and plain text email bodies
- File attachments with base64 encoding
- Template-based subjects and bodies
- Custom email headers
- CC and BCC recipients
- Retry with configurable attempts and delay
- Connection pooling with batch size

## Sink Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `topics` | String | Comma-separated list of topics to consume |
| `smtp.host` | String | SMTP server hostname |
| `smtp.from.address` | String | Sender email address |

### Authentication (optional)

| Setting | Type | Description |
|---------|------|-------------|
| `smtp.username` | String | SMTP authentication username |
| `smtp.password` | Password | SMTP authentication password |

### Connection Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `smtp.port` | Int | `587` | SMTP server port (587 for STARTTLS, 465 for SSL) |
| `smtp.use.ssl` | Boolean | `false` | Use implicit SSL/TLS connection |
| `smtp.use.starttls` | Boolean | `true` | Use STARTTLS to upgrade connection |
| `smtp.timeout.seconds` | Int | `30` | Connection and send timeout |
| `smtp.accept.invalid.certificates` | Boolean | `false` | Accept invalid SSL certificates (dev only) |

### Email Defaults

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `smtp.from.name` | String | | Sender display name |
| `smtp.reply.to` | String | | Reply-to email address |
| `smtp.default.subject` | String | `Message from Surgewave` | Default subject if not in record |

### Field Mappings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `smtp.to.field` | String | `to` | JSON field for recipient address(es) |
| `smtp.cc.field` | String | `cc` | JSON field for CC address(es) |
| `smtp.bcc.field` | String | `bcc` | JSON field for BCC address(es) |
| `smtp.subject.field` | String | `subject` | JSON field for email subject |
| `smtp.body.field` | String | `body` | JSON field for plain text body |
| `smtp.body.html.field` | String | `bodyHtml` | JSON field for HTML body |
| `smtp.attachments.field` | String | `attachments` | JSON field for attachments array |
| `smtp.headers.field` | String | `headers` | JSON field for custom headers |

### Template Settings

| Setting | Type | Description |
|---------|------|-------------|
| `smtp.subject.template` | String | Subject template with `${field}` placeholders |
| `smtp.body.template` | String | Plain text body template |
| `smtp.body.html.template` | String | HTML body template |

### Behavior Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `smtp.send.as.html` | Boolean | `false` | Wrap plain text in HTML tags |
| `smtp.batch.size` | Int | `10` | Emails per connection before reconnect |
| `smtp.retry.count` | Int | `3` | Number of retry attempts on failure |
| `smtp.retry.delay.ms` | Int | `1000` | Delay between retries in milliseconds |

## Examples

### Basic Email Sending

```json
{
  "name": "email-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Smtp.SmtpSinkConnector",
  "smtp.host": "smtp.example.com",
  "smtp.port": 587,
  "smtp.username": "user@example.com",
  "smtp.password": "${secrets:smtp-password}",
  "smtp.from.address": "notifications@example.com",
  "smtp.from.name": "Surgewave Notifications",
  "topics": "email-requests"
}
```

### Gmail Configuration

```json
{
  "name": "gmail-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Smtp.SmtpSinkConnector",
  "smtp.host": "smtp.gmail.com",
  "smtp.port": 587,
  "smtp.use.starttls": true,
  "smtp.username": "your-email@gmail.com",
  "smtp.password": "${secrets:gmail-app-password}",
  "smtp.from.address": "your-email@gmail.com",
  "topics": "gmail-outbound"
}
```

### SSL Connection (Port 465)

```json
{
  "name": "secure-email-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Smtp.SmtpSinkConnector",
  "smtp.host": "smtp.example.com",
  "smtp.port": 465,
  "smtp.use.ssl": true,
  "smtp.username": "user",
  "smtp.password": "${secrets:smtp-password}",
  "smtp.from.address": "secure@example.com",
  "topics": "secure-emails"
}
```

### Template-Based Emails

```json
{
  "name": "templated-email-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Smtp.SmtpSinkConnector",
  "smtp.host": "smtp.example.com",
  "smtp.from.address": "noreply@example.com",
  "smtp.subject.template": "Order ${orderId} Confirmation",
  "smtp.body.html.template": "<h1>Thank you, ${customerName}!</h1><p>Your order ${orderId} has been confirmed.</p>",
  "topics": "order-confirmations"
}
```

### Custom Field Mappings

```json
{
  "name": "custom-fields-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.Smtp.SmtpSinkConnector",
  "smtp.host": "smtp.example.com",
  "smtp.from.address": "system@example.com",
  "smtp.to.field": "recipient",
  "smtp.subject.field": "title",
  "smtp.body.field": "message",
  "smtp.body.html.field": "htmlContent",
  "topics": "notifications"
}
```

## Input Record Format

### Basic Email

```json
{
  "to": "recipient@example.com",
  "subject": "Hello",
  "body": "This is the plain text body"
}
```

### Multiple Recipients

```json
{
  "to": ["alice@example.com", "bob@example.com"],
  "cc": "manager@example.com",
  "bcc": "archive@example.com",
  "subject": "Team Update",
  "body": "Hello team..."
}
```

### HTML Email

```json
{
  "to": "user@example.com",
  "subject": "Welcome!",
  "body": "Welcome to our service",
  "bodyHtml": "<h1>Welcome!</h1><p>Thank you for joining us.</p>"
}
```

### Email with Attachments

```json
{
  "to": "recipient@example.com",
  "subject": "Report",
  "body": "Please find the report attached.",
  "attachments": [
    {
      "name": "report.pdf",
      "contentType": "application/pdf",
      "content": "base64-encoded-content..."
    },
    {
      "name": "data.csv",
      "contentType": "text/csv",
      "content": "base64-encoded-content..."
    }
  ]
}
```

### Custom Headers

```json
{
  "to": "recipient@example.com",
  "subject": "Important",
  "body": "This is urgent.",
  "headers": {
    "X-Priority": "1",
    "X-Custom-Header": "custom-value"
  }
}
```

## Template Placeholders

Templates support `${field}` placeholders that are replaced with values from the JSON record:

| Placeholder | Description |
|-------------|-------------|
| `${fieldName}` | Value from JSON field |
| `${topic}` | Source topic name |
| `${partition}` | Topic partition number |
| `${offset}` | Record offset |
| `${timestamp}` | Current timestamp (ISO 8601) |

### Template Example

Record:
```json
{
  "to": "user@example.com",
  "customerName": "John Doe",
  "orderId": "12345"
}
```

Template configuration:
```json
{
  "smtp.subject.template": "Order ${orderId} Confirmation",
  "smtp.body.template": "Hello ${customerName}, your order ${orderId} is confirmed."
}
```

Result:
- Subject: "Order 12345 Confirmation"
- Body: "Hello John Doe, your order 12345 is confirmed."

## SMTP Providers

### Common Provider Settings

| Provider | Host | Port | TLS |
|----------|------|------|-----|
| Gmail | smtp.gmail.com | 587 | STARTTLS |
| Outlook.com | smtp-mail.outlook.com | 587 | STARTTLS |
| Office 365 | smtp.office365.com | 587 | STARTTLS |
| Amazon SES | email-smtp.{region}.amazonaws.com | 587 | STARTTLS |
| SendGrid | smtp.sendgrid.net | 587 | STARTTLS |
| Mailgun | smtp.mailgun.org | 587 | STARTTLS |

### Gmail App Passwords

For Gmail, you need to use an App Password instead of your regular password:
1. Enable 2-Step Verification on your Google account
2. Generate an App Password at https://myaccount.google.com/apppasswords
3. Use the 16-character App Password as `smtp.password`

## Error Handling

- Connection failures trigger automatic retry with configured delay
- Invalid JSON records are skipped
- Missing recipients cause the record to be skipped
- Connection is recycled after `smtp.batch.size` emails

## Performance Considerations

- **Batch Size**: Lower values ensure fresher connections, higher values reduce overhead
- **Retry Count**: Balance between delivery assurance and latency
- **Timeout**: Set appropriate timeout for your SMTP server
- **Connection Reuse**: Connections are reused within batch for efficiency

## Limitations

- Sink-only connector (no SMTP source)
- No support for inline images (use attachments or external URLs)
- No support for S/MIME encryption
- No support for OAuth2 authentication (use app passwords)
- Attachments must be base64 encoded in JSON
- No template file support (templates are inline config)
