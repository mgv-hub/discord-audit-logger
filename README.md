# Discord Audit Logger

Audit logging for Discord.js bots. Captures guild events, member actions, channel changes, messages, roles, and moderation activity. Queues events, writes to disk in batches, supports compression, encryption, and JSON export.

## Legal & Ethical Use

This is for server administration where you have explicit permission to log activity. You're responsible for complying with Discord's ToS and local privacy laws. Don't use this for unauthorized tracking or surveillance. Software is provided as-is.

## Installation

```bash
npm install discord-audit-logger
# or
pnpm add discord-audit-logger
# or
yarn add discord-audit-logger
```

Requires Node.js 22+ and discord.js v14+.

## Quick Start

Initialize before your bot connects. The logger hooks into Discord events automatically.

```typescript
import { Client, GatewayIntentBits } from 'discord.js';
import { initSystemLogger } from 'discord-audit-logger';

const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMembers,
        GatewayIntentBits.GuildBans,
        GatewayIntentBits.GuildEmojisAndStickers,
        GatewayIntentBits.GuildIntegrations,
        GatewayIntentBits.GuildWebhooks,
        GatewayIntentBits.GuildInvites,
        GatewayIntentBits.GuildVoiceStates,
        GatewayIntentBits.GuildPresences,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.GuildMessageReactions,
        GatewayIntentBits.GuildMessageTyping,
        GatewayIntentBits.DirectMessages,
        GatewayIntentBits.MessageContent,
        GatewayIntentBits.GuildScheduledEvents,
        GatewayIntentBits.AutoModerationConfiguration,
        GatewayIntentBits.AutoModerationExecution,
    ],
});

const logger = initSystemLogger(client, {
    logDirectory: './audit-logs',
    outputMode: 'json',
    sanitizeSensitiveData: true,
    autoPurgeOldLogsDays: 14,
});

client.login(process.env.DISCORD_TOKEN);
```

## Configuration Options

| Parameter               | Type                    | Default      | Notes                                                                |
| ----------------------- | ----------------------- | ------------ | -------------------------------------------------------------------- |
| `logDirectory`          | `string`                | `'logs'`     | Where logs get written. Created if missing.                          |
| `encryptionKey`         | `string`                | `''`         | 64-char hex key required if encryption is enabled.                   |
| `outputMode`            | `'archive'` \| `'json'` | `'archive'`  | `'archive'` = compressed/encrypted; `'json'` = plain text.           |
| `maxEventsPerWindow`    | `number`                | `50`         | Events per ID allowed in throttle window before dropping.            |
| `maxFileSize`           | `number`                | `2168434688` | ~2GB cap per daily file before truncation.                           |
| `throttleWindow`        | `number`                | `3000`       | Time window (ms) for rate limiting calculations.                     |
| `maxLogsPerFile`        | `number`                | `9000000`    | Max entries per file; oldest dropped when exceeded.                  |
| `flushInterval`         | `number`                | `20000`      | Auto-flush every N ms. Set `0` to disable periodic flushes.          |
| `backupEnabled`         | `boolean`               | `true`       | Enable backup queue processing for archived files.                   |
| `enableMetrics`         | `boolean`               | `true`       | Track performance counters, memory, flush times, event distribution. |
| `logLevel`              | `string`                | `'info'`     | Internal console verbosity. `'debug'` for detailed output.           |
| `compressionEnabled`    | `boolean`               | `true`       | Gzip compression (level 9) on archived logs.                         |
| `maxQueueSize`          | `number`                | `130000`     | Flush triggered when queue hits this limit.                          |
| `encryptionEnabled`     | `boolean`               | `false`      | AES-256-GCM encryption on archived logs.                             |
| `maxBackupFiles`        | `number`                | `10`         | Max archived files retained when `keepLogsForever` is false.         |
| `multiGuildMode`        | `boolean`               | `true`       | Tag logs per-guild vs. attributing all to primary guild.             |
| `keepLogsForever`       | `boolean`               | `true`       | If false, cleanup old backups beyond `maxBackupFiles`.               |
| `includeRawData`        | `boolean`               | `true`       | Store full event payloads vs. summarized metadata only.              |
| `sanitizeSensitiveData` | `boolean`               | `true`       | Redact tokens, keys, secrets, webhook URLs from payloads.            |
| `enableHealthChecks`    | `boolean`               | `true`       | Monitor disk space, heap usage, file descriptor counts.              |
| `healthCheckInterval`   | `number`                | `60000`      | Ms between health check evaluations.                                 |
| `autoPurgeOldLogsDays`  | `number`                | `0`          | Delete daily logs older than N days. `0` = disabled.                 |

## Config Interactions

Some settings override others to prevent conflicts:

1. **`outputMode: 'json'` disables compression/encryption/backups**
    - JSON mode is for readability and external parsers. Compression would break that.

2. **Encryption requires a valid key**
    - `encryptionKey` must be a 64-character hex string. Generate with `crypto.randomBytes(32).toString('hex')`.

3. **`autoPurgeOldLogsDays` and `keepLogsForever` work independently**
    - One handles daily rotation files, the other manages the `archives/` folder.

4. **`includeRawData: false` reduces detail**
    - Stores only identifiers and change counts. Saves disk space but limits forensic use.

## Example Configs

### Production Archive

```typescript
{
  logDirectory: 'audit-logs',
  outputMode: 'archive',
  compressionEnabled: true,
  encryptionEnabled: false,
  sanitizeSensitiveData: true,
  includeRawData: true,
  maxQueueSize: 130000,
  flushInterval: 20000,
  autoPurgeOldLogsDays: 0,
  keepLogsForever: true,
  enableMetrics: true,
  enableHealthChecks: true
}
```

### Debugging

```typescript
{
  logDirectory: 'debug-logs',
  outputMode: 'json',
  compressionEnabled: false,
  encryptionEnabled: false,
  sanitizeSensitiveData: true,
  includeRawData: true,
  logLevel: 'debug',
  autoPurgeOldLogsDays: 2,
  enableHealthChecks: false
}
```

### Compliance / Security Focus

```typescript
{
  logDirectory: 'secure-logs',
  outputMode: 'archive',
  compressionEnabled: true,
  encryptionEnabled: true,
  encryptionKey: '<64-CHAR-HEX-KEY>',
  sanitizeSensitiveData: true,
  includeRawData: true,
  multiGuildMode: true,
  keepLogsForever: true,
  autoPurgeOldLogsDays: 0,
  maxBackupFiles: 50,
  enableMetrics: true,
  enableHealthChecks: true,
  healthCheckInterval: 30000
}
```

### Low Overhead / High Throughput

```typescript
{
  logDirectory: 'fast-logs',
  outputMode: 'archive',
  compressionEnabled: true,
  includeRawData: false,
  maxQueueSize: 500000,
  flushInterval: 30000,
  throttleWindow: 1000,
  maxEventsPerWindow: 200,
  enableMetrics: false,
  enableHealthChecks: false
}
```

## API Methods

`initSystemLogger` returns a `SystemLogger` instance with these methods:

### `shutdown(): Promise<void>`

Drains the queue, clears timers, releases caches, writes pending data. Call before process exit.

### `getMetrics()`

Returns performance snapshot: event counts, flush stats, queue overflows, memory, uptime, disk usage, event distribution.

### `getHealthStatus()`

Latest health check: disk space, memory pressure, file descriptor status.

### `exportLogs(filter?: Record<string, any>): Promise<string | null>`

Exports filtered entries to JSON. Filters: `eventType`, `guildId`, `fromDate`, `toDate`. Returns path to generated file.

### `searchLogs(query: string): Promise<Array<LogEntry>>`

Case-insensitive substring search across stored logs. Use carefully on large datasets.

### `logEvent(eventType: string, data: any): void`

Inject custom events into the pipeline. Bypasses Discord listeners but respects throttling and sanitization.

## How It Works

### Queue & Flush

Events buffer in memory, flush in batches to disk. Write-lock prevents race conditions. Queue overflow triggers immediate flush. Failed writes retry with smaller batches.

### Rate Limiting

Rolling window tracks event frequency per identifier. Exceed `maxEventsPerWindow` within `throttleWindow` and events drop until reset. Prevents log flooding during raids or spam.

### Sanitization

When enabled, regex patterns scan for credential formats. Fields named `token`, `apiKey`, `webhookURL`, `url`, `webhookUrl` get replaced with `[REDACTED]`. Original payload structure stays intact.

### Daily Rotation

At midnight UTC, pending entries flush, new file created with date suffix, old files purged if `autoPurgeOldLogsDays` is set. Uses UTC to avoid timezone issues.

### Health Checks

Background checks at `healthCheckInterval`:

- Disk: warning below 100 MB free
- Memory: warning above 90% heap usage
- File descriptors: warning if tracking >10 log files

### Error Handling

I/O operations wrapped in isolated handlers. Failed reads return empty arrays, failed writes retry, init errors log to console without blocking the event loop. System degrades gracefully.

## License & Notes

Built for production Discord bots. Node.js 22+, discord.js v14. Set `tsconfig.json` to target `ES2022` or higher when compiling from source. Report issues via the repository tracker
