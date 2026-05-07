# iCal Connector

The iCal connector provides integration with iCalendar (ICS) files for reading and writing calendar events. It supports both local files and remote URLs.

## Package

```
Kuestenlogik.Surgewave.Connect.ICal
```

## Features

### Source Connector
- **URL Mode**: Poll remote .ics files via HTTP/HTTPS
- **File Mode**: Read local .ics files
- Configurable time window for event filtering
- Option to include/exclude past events
- Authentication support for protected calendars
- Incremental polling (emits only new events)

### Sink Connector
- **File Mode**: Write events to .ics files
- **Record Mode**: Emit ICS content as record value
- Configurable field mapping for event properties
- Automatic UID generation
- Calendar metadata customization

## Source Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `topic` | String | Destination topic for calendar events |
| `ical.url` | String | URL of .ics file (required for url mode) |
| `ical.file.path` | String | Path to local .ics file (required for file mode) |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `ical.source.mode` | String | `url` | Source mode: `url` or `file` |
| `ical.poll.interval.ms` | Int | `60000` | Poll interval in milliseconds |
| `ical.include.past.events` | Boolean | `false` | Include events that have ended |
| `ical.time.window.days` | Int | `30` | Days to look ahead for events |
| `ical.auth.header` | String | `Authorization` | Authentication header name |
| `ical.auth.token` | Password | | Authentication token |
| `ical.headers` | String | | Additional headers (key=value;...) |
| `ical.timeout.ms` | Int | `30000` | HTTP request timeout |

## Sink Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `topics` | String | Comma-separated list of topics to consume |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `ical.output.mode` | String | `record` | Output mode: `file` or `record` |
| `ical.output.path` | String | | Output file path (required for file mode) |
| `ical.calendar.name` | String | `Surgewave Calendar` | Calendar name (X-WR-CALNAME) |
| `ical.calendar.prodid` | String | `-//Surgewave//...` | Calendar product ID |
| `ical.default.duration.minutes` | Int | `60` | Default event duration |
| `ical.summary.field` | String | `summary` | JSON field for event summary |
| `ical.description.field` | String | `description` | JSON field for event description |
| `ical.start.field` | String | `start` | JSON field for start time (ISO 8601) |
| `ical.end.field` | String | `end` | JSON field for end time (ISO 8601) |
| `ical.location.field` | String | `location` | JSON field for event location |
| `ical.uid.field` | String | `uid` | JSON field for event UID |
| `ical.flush.interval.ms` | Int | `10000` | Flush interval for file mode |
| `ical.max.events.per.file` | Int | `100` | Max events before file rotation |

## Examples

### Poll Remote Calendar

```json
{
  "name": "google-calendar-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.ICal.ICalSourceConnector",
  "ical.url": "https://calendar.google.com/calendar/ical/example%40gmail.com/public/basic.ics",
  "ical.poll.interval.ms": 300000,
  "ical.time.window.days": 60,
  "topic": "calendar-events"
}
```

### Poll Protected Calendar with Auth

```json
{
  "name": "private-calendar-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.ICal.ICalSourceConnector",
  "ical.url": "https://calendar.example.com/feed.ics",
  "ical.auth.token": "Bearer ${secrets:calendar-token}",
  "ical.poll.interval.ms": 60000,
  "topic": "private-events"
}
```

### Read Local ICS File

```json
{
  "name": "local-calendar-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.ICal.ICalSourceConnector",
  "ical.source.mode": "file",
  "ical.file.path": "/data/calendars/team-events.ics",
  "ical.poll.interval.ms": 30000,
  "ical.include.past.events": true,
  "topic": "team-events"
}
```

### Include Past Events

```json
{
  "name": "historical-calendar-source",
  "connector.class": "Kuestenlogik.Surgewave.Connect.ICal.ICalSourceConnector",
  "ical.url": "https://example.com/archive.ics",
  "ical.include.past.events": true,
  "ical.time.window.days": 365,
  "topic": "historical-events"
}
```

### Generate ICS Files

```json
{
  "name": "events-to-ics-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.ICal.ICalSinkConnector",
  "ical.output.mode": "file",
  "ical.output.path": "/exports/${topic}_${timestamp}.ics",
  "ical.calendar.name": "Exported Events",
  "ical.max.events.per.file": 500,
  "topics": "events"
}
```

### Custom Field Mapping

```json
{
  "name": "custom-fields-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.ICal.ICalSinkConnector",
  "ical.output.mode": "file",
  "ical.output.path": "/data/meetings.ics",
  "ical.summary.field": "title",
  "ical.description.field": "notes",
  "ical.start.field": "startTime",
  "ical.end.field": "endTime",
  "ical.location.field": "room",
  "ical.uid.field": "meetingId",
  "topics": "meetings"
}
```

### Record Mode (ICS as Value)

```json
{
  "name": "ics-record-sink",
  "connector.class": "Kuestenlogik.Surgewave.Connect.ICal.ICalSinkConnector",
  "ical.output.mode": "record",
  "ical.calendar.name": "Stream Events",
  "ical.default.duration.minutes": 30,
  "topics": "quick-events"
}
```

## Source Record Format

Events are emitted as JSON with the following structure:

```json
{
  "uid": "event-123@example.com",
  "summary": "Team Meeting",
  "description": "Weekly sync",
  "location": "Room 101",
  "start": "2024-01-15T10:00:00Z",
  "end": "2024-01-15T11:00:00Z",
  "startTimezone": "America/New_York",
  "endTimezone": "America/New_York",
  "allDay": false,
  "status": "CONFIRMED",
  "created": "2024-01-01T00:00:00Z",
  "lastModified": "2024-01-10T12:00:00Z",
  "sequence": 0,
  "transparency": "OPAQUE",
  "priority": 0,
  "organizer": "mailto:organizer@example.com",
  "categories": "Work,Meeting",
  "recurrenceRule": "FREQ=WEEKLY;BYDAY=MO",
  "attendees": [
    {
      "email": "mailto:attendee@example.com",
      "name": "John Doe",
      "role": "REQ-PARTICIPANT",
      "status": "ACCEPTED"
    }
  ]
}
```

## Sink Input Format

The sink expects JSON records with event data:

```json
{
  "uid": "meeting-456",
  "summary": "Product Review",
  "description": "Quarterly review meeting",
  "start": "2024-02-01T14:00:00Z",
  "end": "2024-02-01T15:00:00Z",
  "location": "Conference Room A",
  "status": "CONFIRMED",
  "priority": 5,
  "categories": "Meeting"
}
```

## URL Schemes Supported

| Scheme | Description |
|--------|-------------|
| `http://` | Unencrypted HTTP |
| `https://` | HTTPS with TLS |
| `webcal://` | Converted to HTTPS automatically |

## Common Calendar URLs

- **Google Calendar**: `https://calendar.google.com/calendar/ical/{calendar_id}/public/basic.ics`
- **Outlook.com**: `https://outlook.live.com/owa/calendar/{user_id}/{calendar_id}/calendar.ics`
- **iCloud**: `https://p{xx}-caldav.icloud.com/{user_id}/calendars/{calendar_id}/`
- **CalDAV**: Standard CalDAV endpoints (subscribe URLs)

## Performance Considerations

- **Poll Interval**: Balance freshness vs server load (60-300 seconds typical)
- **Time Window**: Limit to reduce processing for large calendars
- **Max Events Per File**: Keep under 1000 for reasonable file sizes
- **Past Events**: Disable for ongoing sync to reduce duplicate processing

## Limitations

- No CalDAV write support (read-only for CalDAV)
- No WebDAV support
- Recurring event expansion is handled by the Ical.Net library
- Timezone handling relies on Ical.Net's implementation
- VTODO (tasks) and VJOURNAL are not processed, only VEVENT
