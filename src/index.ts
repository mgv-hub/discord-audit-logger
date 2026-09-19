import * as fsp from 'fs/promises';
import * as fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { Client, AuditLogEvent } from 'discord.js';
import zlib from 'zlib';
import os from 'os';
import pack from '../package.json' with { type: 'json' };

const { version } = pack;

export interface SystemLoggerConfig {
    logDirectory?: string;
    maxQueueSize?: number;
    flushInterval?: number;
    maxFileSize?: number;
    maxLogsPerFile?: number;
    throttleWindow?: number;
    maxEventsPerWindow?: number;
    backupEnabled?: boolean;
    compressionEnabled?: boolean;
    encryptionEnabled?: boolean;
    encryptionKey?: string;
    logLevel?: string;
    includeRawData?: boolean;
    sanitizeSensitiveData?: boolean;
    enableMetrics?: boolean;
    enableHealthChecks?: boolean;
    healthCheckInterval?: number;
    keepLogsForever?: boolean;
    multiGuildMode?: boolean;
    maxBackupFiles?: number;
    outputMode?: 'archive' | 'json';
    autoPurgeOldLogsDays?: number;
}

export const DEFAULT_LOGGER_CONFIG: Required<SystemLoggerConfig> = {
    logDirectory: 'logs',
    maxQueueSize: 130000,
    flushInterval: 20000,
    maxFileSize: 2068 * 1024 * 1024,
    maxLogsPerFile: 9000000,
    throttleWindow: 3000,
    maxEventsPerWindow: 50,
    backupEnabled: true,
    compressionEnabled: true,
    encryptionEnabled: false,
    encryptionKey: '',
    logLevel: 'info',
    includeRawData: true,
    sanitizeSensitiveData: true,
    enableMetrics: true,
    enableHealthChecks: true,
    healthCheckInterval: 60000,
    keepLogsForever: true,
    multiGuildMode: true,
    maxBackupFiles: 10,
    outputMode: 'archive',
    autoPurgeOldLogsDays: 0,
};

interface Metrics {
    totalEventsLogged: number;
    totalErrors: number;
    flushCount: number;
    queueOverflows: number;
    throttleEvents: number;
    backupCount: number;
    memoryUsage: {
        rss: number;
        heapTotal: number;
        heapUsed: number;
    };
    uptime: number;
    diskUsage: {
        total: number;
        free: number;
        used: number;
    };
    eventDistribution: Record<string, number>;
    averageQueueLatency: number;
    lastFlushDuration: number;
    memoryPressure?: boolean;
}

interface HealthStatus {
    diskSpace: boolean;
    memoryPressure: boolean;
    fileDescriptorLeak: boolean;
    lastCheck: string | null;
}

interface LogEntry {
    id: string;
    timestamp: string;
    eventType: string;
    guildId: string | null;
    shardId: number | null;
    data: any;
    metrics: {
        queueSize: number;
        throttleActive: boolean;
        memoryUsage?: Metrics['memoryUsage'];
        uptime?: number;
    };
}

export class SystemLoggerCore {
    protected config: Required<SystemLoggerConfig>;
    protected logDirectory: string;
    protected currentDate: string;
    protected currentLogFilePath: string;
    protected logQueue: LogEntry[];
    protected isWriting: boolean;
    protected isShuttingDown: boolean;
    protected guildId: string | null;
    protected guildContextMap: Map<string, any>;
    protected highVolumeThrottle: Map<string, { timestamp: number; count: number }>;
    protected eventStats: Map<string, number>;
    protected errorStats: Map<string, number>;
    protected startTime: number;
    protected backupQueue: string[];
    protected metrics: Metrics | null;
    protected healthStatus: HealthStatus;
    protected flushIntervalId: any;
    protected metricsIntervalId: any;
    protected healthIntervalId: any;
    protected rotationTimeout: any;

    constructor(config: SystemLoggerConfig = {}) {
        this.config = {
            ...DEFAULT_LOGGER_CONFIG,
            ...config,
        } as Required<SystemLoggerConfig>;

        if (
            this.config.encryptionEnabled &&
            (!this.config.encryptionKey || this.config.encryptionKey.length !== 64)
        ) {
            throw new Error(
                'encryptionKey must be a 64-character hex string when encryptionEnabled is true',
            );
        }

        this._resolveConfigConflicts();

        this.logDirectory = path.resolve(this.config.logDirectory);
        this.currentDate = this._getCurrentDate();
        this.currentLogFilePath = this._getLogFilePathForDate(this.currentDate);
        this.logQueue = [];
        this.isWriting = false;
        this.isShuttingDown = false;
        this.guildId = null;
        this.guildContextMap = new Map();

        // TODO: clean up throttle map periodically to avoid memory leak on large bots
        this.highVolumeThrottle = new Map();
        this.eventStats = new Map();
        this.errorStats = new Map();
        this.startTime = Date.now();
        this.backupQueue = [];
        this.metrics = null;

        this.healthStatus = {
            diskSpace: true,
            memoryPressure: false,
            fileDescriptorLeak: false,
            lastCheck: null,
        };

        if (this.config.enableMetrics) {
            this._initializeMetrics();
        }

        this._ensureLogDirectory().catch((err) => this._handleInitError(err));
        this._ensureCurrentLogFile().catch((err) => this._handleInitError(err));

        if (this.config.flushInterval > 0) this._startPeriodicFlush();
        if (this.config.enableMetrics) this._startMetricsCollection();
        if (this.config.enableHealthChecks) this._startHealthChecks();

        this._startDailyRotationScheduler();

        this._logSystemEvent('loggerInitialized', {
            configHash: this._generateConfigHash(),
            version,
            currentDate: this.currentDate,
        });
    }

    protected _resolveConfigConflicts() {
        if (this.config.outputMode === 'json') {
            this.config.compressionEnabled = false;
            this.config.encryptionEnabled = false;
            this.config.backupEnabled = false;
        }
    }

    protected _initializeMetrics() {
        this.metrics = {
            totalEventsLogged: 0,
            totalErrors: 0,
            flushCount: 0,
            queueOverflows: 0,
            throttleEvents: 0,
            backupCount: 0,
            memoryUsage: {
                rss: 0,
                heapTotal: 0,
                heapUsed: 0,
            },
            uptime: 0,
            diskUsage: { total: 0, free: 0, used: 0 },
            eventDistribution: {},
            averageQueueLatency: 0,
            lastFlushDuration: 0,
        };
        this._collectMemoryStats();
    }

    protected async _ensureLogDirectory() {
        await fsp.mkdir(this.logDirectory, {
            recursive: true,
        });
    }

    protected _getCurrentDate(): string {
        return new Date().toISOString().split('T')[0];
    }

    protected _getLogFilePathForDate(date: string): string {
        return path.join(this.logDirectory, `system_logs_${date}.json`);
    }

    protected _getFullLogPath(basePath: string): string {
        if (this.config.outputMode === 'json') return basePath;
        if (this.config.encryptionEnabled) return basePath + '.enc.gz';
        return basePath + '.gz';
    }

    protected async _ensureCurrentLogFile() {
        const finalPath = this._getFullLogPath(this.currentLogFilePath);
        try {
            await fsp.access(finalPath);
        } catch {
            const initialLog = {
                metadata: {
                    version,
                    createdAt: new Date().toISOString(),
                    configHash: this._generateConfigHash(),
                    features: {
                        dailyRotation: true,
                        compression: this.config.compressionEnabled,
                        encryption: this.config.encryptionEnabled,
                        multiGuild: this.config.multiGuildMode,
                        autoUpload: false,
                        summaryReport: false,
                        onDemandUpload: false,
                        keepLogsForever: this.config.keepLogsForever,
                    },
                    date: this.currentDate,
                },
                logs: [],
                metrics: this.metrics || {},
            };
            await this._writeToFile(
                JSON.stringify(initialLog, null, 2),
                this.currentLogFilePath,
            );
        }
    }

    protected _generateConfigHash(): string {
        return crypto
            .createHash('md5')
            .update(JSON.stringify(this.config))
            .digest('hex')
            .slice(0, 16);
    }

    protected _startPeriodicFlush() {
        this.flushIntervalId = setInterval(async () => {
            if (!this.isShuttingDown) {
                await this._flushQueue().catch((err) => this._handleFlushError(err));
            }
        }, this.config.flushInterval);
        if (this.flushIntervalId.unref) this.flushIntervalId.unref();
    }

    protected _startMetricsCollection() {
        this.metricsIntervalId = setInterval(() => {
            if (!this.isShuttingDown) {
                this._collectMemoryStats();
                this._updateUptime();
                if (this.backupQueue.length > 0) {
                    this._handleBackupQueue().catch(console.error);
                }
            }
        }, 5000);
        if (this.metricsIntervalId.unref) this.metricsIntervalId.unref();
    }

    protected _startHealthChecks() {
        this.healthIntervalId = setInterval(async () => {
            if (!this.isShuttingDown) await this._runHealthChecks();
        }, this.config.healthCheckInterval);
        if (this.healthIntervalId.unref) this.healthIntervalId.unref();
    }

    protected _startDailyRotationScheduler() {
        this._scheduleNextRotation();
    }

    protected _scheduleNextRotation() {
        const now = new Date();
        const target = new Date(now);
        target.setUTCHours(0, 0, 0, 0);
        if (now >= target) target.setUTCDate(target.getUTCDate() + 1);

        const delay = target.getTime() - now.getTime();

        this.rotationTimeout = setTimeout(async () => {
            await this._performDailyRotation();
            this._scheduleNextRotation();
        }, delay);
        if (this.rotationTimeout.unref) this.rotationTimeout.unref();
    }

    protected async _performDailyRotation() {
        try {
            await this._flushQueue();
            this.currentDate = this._getCurrentDate();
            this.currentLogFilePath = this._getLogFilePathForDate(this.currentDate);
            await this._ensureCurrentLogFile();
            await this._purgeOldLogsByFilename();
            this._logSystemEvent('dailyRotationCompleted', { newDate: this.currentDate });
        } catch (err) {
            this._logSystemEvent('dailyRotationFailed', {
                error: (err as Error).message,
            });
            if (this.metrics) this.metrics.totalErrors++;
        }
    }

    protected async _purgeOldLogsByFilename() {
        if (this.config.autoPurgeOldLogsDays <= 0) return;
        try {
            const files = await fsp.readdir(this.logDirectory);
            const dateRegex = /^system_logs_(\d{4}-\d{2}-\d{2})\./;
            const today = new Date(this._getCurrentDate());
            let deletedCount = 0;

            for (const file of files) {
                const match = file.match(dateRegex);
                if (!match) continue;
                const fileDate = new Date(match[1]);
                const ageInDays = Math.floor(
                    (today.getTime() - fileDate.getTime()) / (1000 * 60 * 60 * 24),
                );
                if (ageInDays >= this.config.autoPurgeOldLogsDays) {
                    await fsp.unlink(path.join(this.logDirectory, file));
                    deletedCount++;
                }
            }

            if (deletedCount > 0) {
                this._logSystemEvent('oldLogsPurged', {
                    count: deletedCount,
                    thresholdDays: this.config.autoPurgeOldLogsDays,
                });
            }
        } catch (err) {
            this._logSystemEvent('purgeOldLogsFailed', { error: (err as Error).message });
        }
    }

    protected _collectMemoryStats() {
        if (!this.metrics) return;
        const usage = process.memoryUsage();
        this.metrics.memoryUsage = {
            rss: usage.rss,
            heapTotal: usage.heapTotal,
            heapUsed: usage.heapUsed,
        };
        this.metrics.memoryPressure = usage.heapUsed / usage.heapTotal > 0.85;
    }

    protected _updateUptime() {
        if (!this.metrics) return;
        this.metrics.uptime = (Date.now() - this.startTime) / 1000;
    }

    protected async _flushQueue() {
        if (this.logQueue.length === 0 || this.isWriting || this.isShuttingDown) return;

        const currentDate = this._getCurrentDate();
        if (currentDate !== this.currentDate) {
            this.currentDate = currentDate;
            this.currentLogFilePath = this._getLogFilePathForDate(this.currentDate);
            await this._ensureCurrentLogFile();
        }

        this.isWriting = true;
        const startTime = Date.now();

        try {
            let currentLogs = await this._readCurrentLogs();
            currentLogs.push(...this.logQueue);
            await this._enforceFileLimits(currentLogs);
            await this._writeLogs(currentLogs);

            const processedCount = this.logQueue.length;
            this.logQueue = [];

            if (this.metrics) {
                this.metrics.flushCount++;
                this.metrics.lastFlushDuration = Date.now() - startTime;
                this.metrics.averageQueueLatency =
                    (this.metrics.averageQueueLatency * (this.metrics.flushCount - 1) +
                        (Date.now() - startTime)) /
                    this.metrics.flushCount;
            }

            await this._updateMetricsInFile();
            this._logSystemEvent('flushSuccess', {
                count: processedCount,
                duration: Date.now() - startTime,
            });
        } catch (error) {
            this._handleFlushError(error);
            await this._retryFlush();
        } finally {
            this.isWriting = false;
        }
    }

    protected async _readCurrentLogs(): Promise<LogEntry[]> {
        try {
            const content = await this._readFromFile(this.currentLogFilePath);
            if (!content.trim()) return [];
            const parsed = JSON.parse(content);
            if (Array.isArray(parsed)) return parsed;
            if (parsed.logs && Array.isArray(parsed.logs)) return parsed.logs;
            return [];
        } catch (err) {
            if (
                (err as NodeJS.ErrnoException).code === 'ENOENT' ||
                (err as Error).message.includes('unexpected end')
            )
                return [];
            this._logSystemEvent('readLogsFailed', {
                error: (err as Error).message,
            });
            return [];
        }
    }

    protected async _enforceFileLimits(logs: any[]) {
        if (logs.length > this.config.maxLogsPerFile) {
            logs.splice(0, logs.length - this.config.maxLogsPerFile);
        }
        const logSize = Buffer.byteLength(JSON.stringify(logs, null, 2), 'utf8');
        if (logSize > this.config.maxFileSize) {
            logs.splice(0, Math.floor(logs.length / 2));
        }
    }

    protected async _cleanupOldBackups() {
        if (this.config.keepLogsForever) return;
        try {
            const files = await fsp.readdir(this.logDirectory);
            const logFiles = await Promise.all(
                files
                    .filter(
                        (f) =>
                            f.startsWith('system_logs_') &&
                            (f.endsWith('.json') || f.endsWith('.json.gz')),
                    )
                    .map(async (f) => {
                        const stat = await fsp.stat(path.join(this.logDirectory, f));
                        return {
                            name: f,
                            time: stat.mtimeMs,
                        };
                    }),
            );
            logFiles.sort((a, b) => b.time - a.time);
            for (let i = this.config.maxBackupFiles; i < logFiles.length; i++) {
                const filePath = path.join(this.logDirectory, logFiles[i].name);
                await fsp.unlink(filePath).catch(() => {});
            }
        } catch (err) {
            this._logSystemEvent('backupCleanupFailed', {
                error: (err as Error).message,
            });
        }
    }

    protected async _writeLogs(logs: any[]) {
        const logContent = {
            metadata: {
                lastUpdated: new Date().toISOString(),
                version,
                configHash: this._generateConfigHash(),
                date: this.currentDate,
            },
            logs,
            metrics: this.metrics || {},
        };
        const content = JSON.stringify(logContent, null, 2);
        await this._writeToFile(content, this.currentLogFilePath);
    }

    protected async _writeToFile(content: string, filePath: string) {
        const targetPath = this._getFullLogPath(filePath);
        if (this.config.outputMode === 'json') {
            await fsp.writeFile(targetPath, content);
            return;
        }

        let finalContent: string | Buffer = content;
        if (this.config.encryptionEnabled) {
            finalContent = await this._encryptContent(finalContent as string);
        }
        finalContent = await this._compressContent(finalContent);
        await fsp.writeFile(targetPath, finalContent);
    }

    protected async _readFromFile(filePath: string): Promise<string> {
        const targetPath = this._getFullLogPath(filePath);
        if (!(await this._fileExists(targetPath))) return '';

        let content: Buffer = await fsp.readFile(targetPath);
        if (this.config.outputMode === 'json') return content.toString('utf8');

        content = await this._decompressContent(content);
        if (this.config.encryptionEnabled) content = await this._decryptContent(content);
        return content.toString('utf8');
    }

    protected async _encryptContent(content: string): Promise<Buffer> {
        return new Promise((resolve, reject) => {
            try {
                const key = Buffer.from(this.config.encryptionKey, 'hex');
                const iv: Buffer = crypto.randomBytes(12);
                const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
                const encrypted = Buffer.concat([
                    cipher.update(content, 'utf8') as Buffer,
                    cipher.final() as Buffer,
                ]);
                const authTag: Buffer = cipher.getAuthTag() as Buffer;
                resolve(Buffer.concat([iv, authTag, encrypted]));
            } catch (err) {
                reject(err);
            }
        });
    }

    protected async _decryptContent(encryptedContent: Buffer): Promise<Buffer> {
        return new Promise((resolve, reject) => {
            try {
                const key = Buffer.from(this.config.encryptionKey, 'hex');
                const iv = encryptedContent.slice(0, 12);
                const authTag = encryptedContent.slice(12, 28);
                const encrypted = encryptedContent.slice(28);
                const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
                decipher.setAuthTag(authTag);
                const decrypted = Buffer.concat([
                    decipher.update(encrypted) as Buffer,
                    decipher.final() as Buffer,
                ]);
                resolve(decrypted);
            } catch (err) {
                reject(err);
            }
        });
    }

    protected async _compressContent(content: string | Buffer): Promise<Buffer> {
        return new Promise((resolve, reject) => {
            zlib.gzip(content, { level: 9 }, (err, buffer) => {
                if (err) reject(err);
                else resolve(buffer as Buffer);
            });
        });
    }

    protected async _decompressContent(compressed: Buffer): Promise<Buffer> {
        return new Promise((resolve, reject) => {
            zlib.gunzip(compressed, (err, result) => {
                if (err) reject(err);
                else resolve(result);
            });
        });
    }

    protected async _fileExists(filePath: string): Promise<boolean> {
        try {
            await fsp.access(filePath);
            return true;
        } catch {
            return false;
        }
    }

    protected async _updateMetricsInFile() {
        if (!this.metrics) return;
        try {
            const current = await this._readCurrentLogs();
            const fileContent = await this._readFromFile(this.currentLogFilePath);
            let fullData: any;
            try {
                fullData = JSON.parse(fileContent);
            } catch {
                fullData = {
                    logs: current,
                    metrics: this.metrics,
                };
            }
            fullData.metrics = {
                ...fullData.metrics,
                ...this.metrics,
            };
            fullData.logs = current;
            await this._writeToFile(
                JSON.stringify(fullData, null, 2),
                this.currentLogFilePath,
            );
        } catch (err) {
            this._handleMetricsError(err);
        }
    }

    protected async _retryFlush() {
        if (this.logQueue.length === 0 || this.isShuttingDown) return;
        const batchSize = Math.min(50, this.logQueue.length);
        const batch = this.logQueue.splice(0, batchSize);
        try {
            let currentLogs = await this._readCurrentLogs();
            currentLogs.push(...batch);
            await this._writeLogs(currentLogs);
        } catch (retryErr) {
            this.logQueue.unshift(...batch);
            if (this.metrics) this.metrics.totalErrors++;
            this._logSystemEvent('retryFlushFailed', {
                error: (retryErr as Error).message,
                batchSize,
            });
        }
    }

    protected _handleFlushError(error: unknown) {
        if (this.metrics) {
            this.metrics.totalErrors++;
            this.errorStats.set('flush', (this.errorStats.get('flush') || 0) + 1);
        }
        this._logSystemEvent('flushError', {
            error: (error as Error).message,
            stack: (error as Error).stack?.substring(0, 500),
        });
    }

    protected _handleInitError(error: unknown) {
        if (this.metrics) {
            this.metrics.totalErrors++;
            this.errorStats.set('init', (this.errorStats.get('init') || 0) + 1);
        }
        console.error('SystemLogger init error:', error);
    }

    protected _handleMetricsError(error: unknown) {
        if (this.metrics) {
            this.metrics.totalErrors++;
            this.errorStats.set('metrics', (this.errorStats.get('metrics') || 0) + 1);
        }
        this._logSystemEvent('metricsError', {
            error: (error as Error).message,
        });
    }

    protected _handleError(error: unknown, context: string) {
        if (this.metrics) {
            this.metrics.totalErrors++;
            if (!this.errorStats) this.errorStats = new Map<string, number>();
            const key = `${context}_error`;
            const prev = this.errorStats.get(key) ?? 0;
            this.errorStats.set(key, prev + 1);
        }
        const err = error instanceof Error ? error : new Error(String(error));
        this._logSystemEvent('internalError', {
            context,
            error: err.message,
            stack: err.stack ? err.stack.substring(0, 500) : null,
        });
    }

    protected _logSystemEvent(eventType: string, data: any) {
        if (
            this.config.logLevel === 'debug' ||
            eventType.includes('Error') ||
            eventType.includes('Failed')
        ) {
            console.log(`[SystemLogger] ${eventType}:`, JSON.stringify(data, null, 2));
        }
    }

    protected async _handleBackupQueue() {
        const items = this.backupQueue.splice(0, 5);
        for (const backupPath of items) {
            try {
                const archiveDir = path.join(this.logDirectory, 'archives');
                await fsp.mkdir(archiveDir, {
                    recursive: true,
                });
                const archiveName = path
                    .basename(backupPath)
                    .replace('.backup', '.tar.gz');
                let tar: any;
                try {
                    tar = require('tar');
                } catch {
                    continue;
                }
                await tar.c(
                    {
                        file: path.join(archiveDir, archiveName),
                        gzip: true,
                        cwd: path.dirname(backupPath),
                    },
                    [path.basename(backupPath)],
                );
                await fsp.unlink(backupPath);
                this._logSystemEvent('backupArchived', {
                    archivePath: path.join(archiveDir, archiveName),
                });
            } catch (backupErr) {
                if (this.metrics) this.metrics.totalErrors++;
                this._logSystemEvent('backupArchiveFailed', {
                    error: (backupErr as Error).message,
                    backupPath,
                });
            }
        }
    }

    protected _shouldThrottle(identifier: string, eventType: string): boolean {
        try {
            const key = `${identifier}_${eventType}`;
            const now = Date.now();
            let entry = this.highVolumeThrottle.get(key);

            if (!entry || now - entry.timestamp > this.config.throttleWindow) {
                entry = {
                    timestamp: now,
                    count: 1,
                };
                this.highVolumeThrottle.set(key, entry);
                return false;
            }

            if (entry.count >= this.config.maxEventsPerWindow) {
                if (this.metrics) this.metrics.throttleEvents++;
                return true;
            }

            entry.count++;
            this.highVolumeThrottle.set(key, entry);
            return false;
        } catch (error) {
            if (this.metrics) this.metrics.totalErrors++;
            return true;
        }
    }

    protected _sanitizeData(data: any): any {
        if (!this.config.sanitizeSensitiveData) return data;
        const sanitized = { ...data };
        if (typeof sanitized.content === 'string') {
            sanitized.content = sanitized.content.replace(
                /(\b(?:token|password|secret|key|auth|api[_\-]?key|webhook[_\-]?url)\b[^:]*:)[^,\s}]+/gi,
                '$1 [REDACTED]',
            );
        }
        const sensitiveFields = ['token', 'apiKey', 'webhookURL', 'webhookUrl', 'url'];
        for (const field of sensitiveFields) {
            if (sanitized[field]) sanitized[field] = '[REDACTED]';
        }
        return sanitized;
    }

    protected _createLogEntry(eventType: string, data: any): LogEntry {
        const ts = new Date().toISOString();
        const entry = {
            id: this._generateEntryId(),
            timestamp: ts,
            eventType,
            guildId: this.config.multiGuildMode
                ? data.guildId || null
                : this.guildId || data.guildId || null,
            shardId: data.shardId || null,
            data: this.config.includeRawData
                ? this._sanitizeData(data)
                : {
                      summary: this._summarizeData(data),
                  },
            metrics: {
                queueSize: this.logQueue.length,
                throttleActive: false,
                memoryUsage: this.metrics?.memoryUsage,
                uptime: this.metrics?.uptime,
            },
        };
        this._incrementEventStat(eventType);
        return entry;
    }

    protected _generateEntryId(): string {
        return crypto.randomUUID();
    }

    protected _summarizeData(data: any): any {
        return {
            userId: data.userId || data.authorId || null,
            channelId: data.channelId || null,
            messageId: data.messageId || null,
            roleId: data.roleId || null,
            executorId: data.executor || null,
            changes: data.changes ? Object.keys(data.changes).length : 0,
            reason: data.reason ? 'Provided' : null,
            guildId: data.guildId || null,
            shardId: data.shardId || null,
        };
    }

    protected _incrementEventStat(eventType: string) {
        if (!this.metrics) return;
        const count = this.eventStats.get(eventType) || 0;
        this.eventStats.set(eventType, count + 1);
        this.metrics.totalEventsLogged++;
        this.metrics.eventDistribution[eventType] =
            (this.metrics.eventDistribution[eventType] || 0) + 1;
    }

    logEvent(eventType: string, data: any) {
        if (this.isShuttingDown) return;
        try {
            const identifier = data.userId || data.authorId || 'global';
            if (this._shouldThrottle(identifier, eventType)) {
                if (this.metrics) this.metrics.throttleEvents++;
                return;
            }
            const entry = this._createLogEntry(eventType, data);
            this.logQueue.push(entry);

            if (this.logQueue.length > this.config.maxQueueSize) {
                if (this.metrics) this.metrics.queueOverflows++;
                this._flushQueue().catch((err) => this._handleFlushError(err));
            }
        } catch (error) {
            this._handleError(error, 'logEvent');
        }
    }

    protected async _runHealthChecks() {
        try {
            const diskUsage = await this._checkDiskSpace();
            this.healthStatus.diskSpace = diskUsage.free > 100 * 1024 * 1024;
            this._collectMemoryStats();
            this.healthStatus.memoryPressure =
                (this.metrics?.memoryUsage.heapUsed || 0) /
                    (this.metrics?.memoryUsage.heapTotal || 1) >
                0.9;

            const openFiles = await this._countOpenLogFiles();
            this.healthStatus.fileDescriptorLeak = openFiles > 10;
            this.healthStatus.lastCheck = new Date().toISOString();

            if (
                !this.healthStatus.diskSpace ||
                this.healthStatus.memoryPressure ||
                this.healthStatus.fileDescriptorLeak
            ) {
                this._logSystemEvent('healthCheckWarning', { status: this.healthStatus });
            }
        } catch (err) {
            this._logSystemEvent('healthCheckFailed', { error: (err as Error).message });
        }
    }

    protected async _checkDiskSpace(): Promise<{
        total: number;
        free: number;
        used: number;
    }> {
        return new Promise((resolve) => {
            const totalmem = os.totalmem();
            const freemem = os.freemem();
            resolve({
                total: totalmem,
                free: freemem,
                used: totalmem - freemem,
            });
        });
    }

    protected async _countOpenLogFiles(): Promise<number> {
        try {
            const files = await fsp.readdir(this.logDirectory);
            return files.filter((f) => f.includes('system_logs')).length;
        } catch {
            return 0;
        }
    }

    async shutdown() {
        this.isShuttingDown = true;
        try {
            if (this.flushIntervalId) clearInterval(this.flushIntervalId);
            if (this.metricsIntervalId) clearInterval(this.metricsIntervalId);
            if (this.healthIntervalId) clearInterval(this.healthIntervalId);
            if (this.rotationTimeout) clearTimeout(this.rotationTimeout);

            await this._flushQueue();
            await this._updateMetricsInFile();

            this.highVolumeThrottle.clear();
            this.eventStats.clear();
            this.errorStats.clear();

            if (this.backupQueue.length > 0) await this._handleBackupQueue();

            this._logSystemEvent('loggerShutdown', {
                reason: 'graceful',
            });
        } catch (error) {
            this._handleError(error, 'shutdown');
        }
    }

    getMetrics() {
        return {
            ...this.metrics,
            eventStats: Object.fromEntries(this.eventStats),
            errorStats: Object.fromEntries(this.errorStats),
            config: {
                ...this.config,
                logDirectory: '[REDACTED]',
            },
            health: this.healthStatus,
        };
    }

    async exportLogs(filter: Record<string, any> = {}) {
        // TODO: add support for exporting to sqlite instead of JSON files
        try {
            const files = await fsp.readdir(this.logDirectory);
            const logFiles = files.filter(
                (f) =>
                    f.startsWith('system_logs_') &&
                    (f.endsWith('.json') || f.endsWith('.json.gz')),
            );
            let allLogs: any[] = [];

            for (const file of logFiles) {
                const filePath = path.join(this.logDirectory, file);
                const content = await this._readFromFile(filePath);
                const parsed = JSON.parse(content);
                const logs = Array.isArray(parsed) ? parsed : parsed.logs || [];
                allLogs = allLogs.concat(logs);
            }

            const filtered = allLogs.filter((log) => {
                if (filter.eventType && log.eventType !== filter.eventType) return false;
                if (filter.guildId && log.guildId !== filter.guildId) return false;
                if (
                    filter.fromDate &&
                    new Date(log.timestamp) < new Date(filter.fromDate)
                )
                    return false;
                if (filter.toDate && new Date(log.timestamp) > new Date(filter.toDate))
                    return false;
                return true;
            });

            const exportPath = path.join(this.logDirectory, `export_${Date.now()}.json`);
            await fsp.writeFile(exportPath, JSON.stringify(filtered, null, 2));
            this._logSystemEvent('logsExported', {
                count: filtered.length,
                path: exportPath,
                filter,
            });
            return exportPath;
        } catch (err) {
            this._handleError(err, 'exportLogs');
            return null;
        }
    }

    async searchLogs(query: string) {
        try {
            const files = await fsp.readdir(this.logDirectory);
            const logFiles = files.filter(
                (f) =>
                    f.startsWith('system_logs_') &&
                    (f.endsWith('.json') || f.endsWith('.json.gz')),
            );
            let allLogs: any[] = [];

            for (const file of logFiles) {
                const filePath = path.join(this.logDirectory, file);
                const content = await this._readFromFile(filePath);
                const parsed = JSON.parse(content);
                const logs = Array.isArray(parsed) ? parsed : parsed.logs || [];
                allLogs = allLogs.concat(logs);
            }

            const results = allLogs.filter((log) =>
                JSON.stringify(log).toLowerCase().includes(query.toLowerCase()),
            );
            this._logSystemEvent('logsSearched', {
                query,
                results: results.length,
            });
            return results;
        } catch (err) {
            this._handleError(err, 'searchLogs');
            return [];
        }
    }

    getHealthStatus() {
        return { ...this.healthStatus };
    }
}

export class SystemLogger extends SystemLoggerCore {
    protected client!: Client;

    async registerHandlers(client: Client) {
        try {
            this.client = client;

            if (this.config.multiGuildMode) {
                client.guilds.cache.forEach((guild) => {
                    this.guildContextMap.set(guild.id, {
                        id: guild.id,
                        name: guild.name,
                        memberCount: guild.memberCount,
                    });
                });
            }

            await this._setupReadyHandler();
            await this._setupGuildHandlers();
            await this._setupMemberHandlers();
            await this._setupBanHandlers();
            await this._setupRoleHandlers();
            await this._setupChannelHandlers();
            this._setupMessageHandlers();
            this._setupReactionHandlers();
            this._setupVoiceHandlers();
            this._setupTypingHandlers();
            this._setupPresenceHandlers();
            this._setupUserHandlers();
            this._setupEmojiHandlers();
            this._setupStickerHandlers();
            this._setupThreadHandlers();
            this._setupStageHandlers();
            this._setupScheduledEventHandlers();
            this._setupInviteHandlers();
            this._setupWebhookHandlers();
            this._setupIntegrationHandlers();
            this._setupAutoModHandlers();
            this._setupClientHandlers();
            this._setupInteractionHandlers();
            this._setupApplicationCommandHandlers();
            this._setupGuildStickerHandlers();
            this._setupEntitlementHandlers();
            this._setupGuildFeatureHandlers();
            this._setupModerationHandlers();
            this._setupPremiumHandlers();

            this._logSystemEvent('handlersRegistered', {});
        } catch (error) {
            this._handleError(error, 'registerHandlers');
        }
    }

    private _fetchAuditDetails(guild: any, type: any, targetId?: string) {
        if (!guild) return { executor: null, reason: null };
        try {
            const logs = guild.fetchAuditLogs({
                limit: 1,
                type,
            });
            const log = logs?.entries?.first();
            if (log && (!targetId || (log.target as any)?.id === targetId)) {
                return {
                    executor: log.executor?.id || null,
                    reason: log.reason || null,
                };
            }
        } catch {}
        return { executor: null, reason: null };
    }

    private _setupReadyHandler() {
        this.client.once('clientReady', async () => {
            try {
                const guilds = this.client.guilds.cache;
                if (guilds.size > 0) {
                    this.guildId = guilds.first()?.id || null;
                    this.logEvent('ready', {
                        userId: this.client.user?.id || null,
                        username: this.client.user?.username || null,
                        guildsCount: guilds.size,
                        uptime: this.client.uptime,
                        shardId: this.client.shard?.ids?.[0] || null,
                        clusterId: process.env.CLUSTER_ID || null,
                        processId: process.pid,
                        nodeVersion: process.version,
                        memoryUsage: process.memoryUsage(),
                    });
                }
                await this.logEvent('cacheReady', {
                    guilds: guilds.size,
                    users: this.client.users.cache.size,
                    channels: this.client.channels.cache.size,
                    shards: this.client.shard?.count || 1,
                    shardId: this.client.shard?.ids?.[0] || null,
                });
            } catch (err) {
                this._handleError(err, 'ready');
            }
        });
    }

    private _setupGuildHandlers() {
        this.client.on('guildCreate', (guild: any) => {
            try {
                const { executor, reason } = this._fetchAuditDetails(
                    guild,
                    AuditLogEvent.GuildUpdate as any,
                );
                this.logEvent('guildCreate', {
                    guildId: guild.id,
                    name: guild.name,
                    icon: guild.icon ? guild.icon : null,
                    splash: guild.splash ? guild.splash : null,
                    discoverySplash: guild.discoverySplash ? guild.discoverySplash : null,
                    ownerId: guild.ownerId,
                    description: guild.description || null,
                    memberCount: guild.memberCount,
                    approximateMemberCount: guild.approximateMemberCount,
                    approximatePresenceCount: guild.approximatePresenceCount,
                    afkTimeout: guild.afkTimeout,
                    afkChannelId: guild.afkChannelId || null,
                    verificationLevel: guild.verificationLevel,
                    defaultMessageNotifications: guild.defaultMessageNotifications,
                    explicitContentFilter: guild.explicitContentFilter,
                    mfaLevel: guild.mfaLevel,
                    joinedTimestamp: guild.joinedTimestamp
                        ? new Date(guild.joinedTimestamp).toISOString()
                        : null,
                    premiumTier: guild.premiumTier,
                    premiumSubscriptionCount: guild.premiumSubscriptionCount || 0,
                    systemChannelId: guild.systemChannelId || null,
                    rulesChannelId: guild.rulesChannelId || null,
                    publicUpdatesChannelId: guild.publicUpdatesChannelId || null,
                    preferredLocale: guild.preferredLocale || 'en-US',
                    executor,
                    reason,
                    shardId: guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'guildCreate');
            }
        });

        this.client.on('guildUpdate', (oldGuild: any, newGuild: any) => {
            try {
                const { executor, reason } = this._fetchAuditDetails(
                    newGuild,
                    AuditLogEvent.GuildUpdate as any,
                );
                const changes = this._detectGuildChanges(oldGuild, newGuild);
                this.logEvent('guildUpdate', {
                    guildId: newGuild.id,
                    changes,
                    executor,
                    reason,
                    shardId: newGuild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'guildUpdate');
            }
        });

        this.client.on('guildDelete', (guild: any) => {
            try {
                const { executor, reason } = this._fetchAuditDetails(
                    guild,
                    AuditLogEvent.GuildUpdate as any,
                );
                this.logEvent('guildDelete', {
                    guildId: guild.id,
                    name: guild.name,
                    memberCount: guild.memberCount,
                    executor,
                    reason,
                    shardId: guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'guildDelete');
            }
        });

        this.client.on('guildUnavailable', (guild: any) => {
            try {
                this.logEvent('guildUnavailable', {
                    guildId: guild.id,
                    name: guild.name,
                    shardId: guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'guildUnavailable');
            }
        });

        this.client.on('guildIntegrationsUpdate', (guild: any) => {
            try {
                const { executor } = this._fetchAuditDetails(
                    guild,
                    AuditLogEvent.IntegrationCreate as any,
                );
                this.logEvent('guildIntegrationsUpdate', {
                    guildId: guild.id,
                    executor,
                    shardId: guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'guildIntegrationsUpdate');
            }
        });
    }

    private _detectGuildChanges(oldGuild: any, newGuild: any): Record<string, any> {
        const changes: Record<string, any> = {};
        const fields = [
            'name',
            'icon',
            'splash',
            'discoverySplash',
            'ownerId',
            'description',
            'verificationLevel',
            'defaultMessageNotifications',
            'explicitContentFilter',
            'mfaLevel',
            'afkTimeout',
            'afkChannelId',
            'systemChannelId',
            'rulesChannelId',
            'publicUpdatesChannelId',
            'preferredLocale',
            'premiumTier',
            'premiumProgressBarEnabled',
        ];
        fields.forEach((field) => {
            if (oldGuild[field] !== newGuild[field])
                changes[field] = {
                    old: oldGuild[field],
                    new: newGuild[field],
                };
        });
        if (oldGuild.banner !== newGuild.banner)
            changes.banner = {
                old: !!oldGuild.banner,
                new: !!newGuild.banner,
            };
        if (oldGuild.systemChannelFlags !== newGuild.systemChannelFlags)
            changes.systemChannelFlags = {
                old: oldGuild.systemChannelFlags,
                new: newGuild.systemChannelFlags,
            };
        return changes;
    }

    private _setupMemberHandlers() {
        this.client.on('guildMemberAdd', (member: any) => {
            try {
                const { executor, reason } = this._fetchAuditDetails(
                    member.guild,
                    AuditLogEvent.MemberKick as any,
                    member.id,
                );
                this.logEvent('guildMemberAdd', {
                    userId: member.user?.id,
                    username: member.user?.username,
                    discriminator: member.user?.discriminator,
                    globalName: member.user?.globalName || null,
                    avatar: member.user?.avatar ? member.user.avatar : null,
                    bot: member.user?.bot,
                    system: member.user?.system,
                    flags: member.user?.flags?.toArray() || [],
                    guildId: member.guild?.id,
                    nickname: member.nickname || null,
                    joinedTimestamp: member.joinedTimestamp
                        ? new Date(member.joinedTimestamp).toISOString()
                        : null,
                    premiumSince: member.premiumSinceTimestamp
                        ? new Date(member.premiumSinceTimestamp).toISOString()
                        : null,
                    pending: member.pending,
                    permissions: member.permissions?.bitfield.toString() || '0',
                    communicationDisabledUntil: member.communicationDisabledUntil
                        ? new Date(member.communicationDisabledUntil).toISOString()
                        : null,
                    roles: Array.from(member.roles.cache.keys()),
                    executor,
                    reason,
                    shardId: member.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'guildMemberAdd');
            }
        });

        this.client.on('guildMemberRemove', (member: any) => {
            try {
                const { executor, reason } = this._fetchAuditDetails(
                    member.guild,
                    AuditLogEvent.MemberKick as any,
                    member.id,
                );
                this.logEvent('guildMemberRemove', {
                    userId: member.user?.id,
                    username: member.user?.username,
                    guildId: member.guild?.id,
                    nickname: member.nickname || null,
                    executor,
                    reason,
                    shardId: member.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'guildMemberRemove');
            }
        });

        this.client.on('guildMemberUpdate', (oldMember: any, newMember: any) => {
            try {
                const changes = this._detectMemberChanges(oldMember, newMember);
                if (Object.keys(changes).length === 0) return;
                const { executor, reason } = this._fetchAuditDetails(
                    newMember.guild,
                    AuditLogEvent.MemberUpdate as any,
                    newMember.id,
                );
                this.logEvent('guildMemberUpdate', {
                    userId: newMember.user?.id,
                    guildId: newMember.guild?.id,
                    changes,
                    executor,
                    reason,
                    shardId: newMember.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'guildMemberUpdate');
            }
        });

        this.client.on('guildMemberAvailable', (member: any) => {
            try {
                this.logEvent('guildMemberAvailable', {
                    userId: member.user?.id,
                    guildId: member.guild?.id,
                    shardId: member.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'guildMemberAvailable');
            }
        });
    }

    private _detectMemberChanges(oldMember: any, newMember: any): Record<string, any> {
        const changes: Record<string, any> = {};
        if (oldMember.nickname !== newMember.nickname)
            changes.nickname = {
                old: oldMember.nickname,
                new: newMember.nickname,
            };
        if (
            (oldMember.roles?.cache.size ?? 0) !== (newMember.roles?.cache.size ?? 0) ||
            !oldMember.roles?.cache.equals(newMember.roles?.cache)
        ) {
            changes.roles = {
                old: Array.from(oldMember.roles?.cache.keys() || []),
                new: Array.from(newMember.roles?.cache.keys() || []),
            };
        }
        if (
            oldMember.communicationDisabledUntil !== newMember.communicationDisabledUntil
        ) {
            changes.communicationDisabledUntil = {
                old: oldMember.communicationDisabledUntil
                    ? new Date(oldMember.communicationDisabledUntil).toISOString()
                    : null,
                new: newMember.communicationDisabledUntil
                    ? new Date(newMember.communicationDisabledUntil).toISOString()
                    : null,
            };
        }
        if (oldMember.premiumSinceTimestamp !== newMember.premiumSinceTimestamp) {
            changes.premiumSince = {
                old: oldMember.premiumSinceTimestamp
                    ? new Date(oldMember.premiumSinceTimestamp).toISOString()
                    : null,
                new: newMember.premiumSinceTimestamp
                    ? new Date(newMember.premiumSinceTimestamp).toISOString()
                    : null,
            };
        }
        if (oldMember.pending !== newMember.pending)
            changes.pending = {
                old: oldMember.pending,
                new: newMember.pending,
            };
        return changes;
    }

    private _setupBanHandlers() {
        this.client.on('guildBanAdd', (ban: any) => {
            try {
                const { executor, reason } = this._fetchAuditDetails(
                    ban.guild,
                    AuditLogEvent.MemberBanAdd as any,
                    ban.user?.id,
                );
                this.logEvent('guildBanAdd', {
                    userId: ban.user?.id,
                    username: ban.user?.username,
                    guildId: ban.guild?.id,
                    executor,
                    reason,
                    shardId: ban.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'guildBanAdd');
            }
        });

        this.client.on('guildBanRemove', (ban: any) => {
            try {
                const { executor, reason } = this._fetchAuditDetails(
                    ban.guild,
                    AuditLogEvent.MemberBanRemove as any,
                    ban.user?.id,
                );
                this.logEvent('guildBanRemove', {
                    userId: ban.user?.id,
                    username: ban.user?.username,
                    guildId: ban.guild?.id,
                    executor,
                    reason,
                    shardId: ban.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'guildBanRemove');
            }
        });
    }

    private _setupRoleHandlers() {
        this.client.on('roleCreate', (role: any) => {
            try {
                const { executor, reason } = this._fetchAuditDetails(
                    role.guild,
                    AuditLogEvent.RoleCreate as any,
                    role.id,
                );
                this.logEvent('roleCreate', {
                    roleId: role.id,
                    name: role.name,
                    color: role.hexColor,
                    hoist: role.hoist,
                    position: role.position,
                    permissions: role.permissions.bitfield.toString(),
                    mentionable: role.mentionable,
                    guildId: role.guild.id,
                    executor,
                    reason,
                    shardId: role.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'roleCreate');
            }
        });

        this.client.on('roleUpdate', (oldRole: any, newRole: any) => {
            try {
                const changes = this._detectRoleChanges(oldRole, newRole);
                if (Object.keys(changes).length === 0) return;
                const { executor, reason } = this._fetchAuditDetails(
                    newRole.guild,
                    AuditLogEvent.RoleUpdate as any,
                    newRole.id,
                );
                this.logEvent('roleUpdate', {
                    roleId: newRole.id,
                    guildId: newRole.guild.id,
                    changes,
                    executor,
                    reason,
                    shardId: newRole.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'roleUpdate');
            }
        });

        this.client.on('roleDelete', (role: any) => {
            try {
                const { executor, reason } = this._fetchAuditDetails(
                    role.guild,
                    AuditLogEvent.RoleDelete as any,
                    role.id,
                );
                this.logEvent('roleDelete', {
                    roleId: role.id,
                    name: role.name,
                    guildId: role.guild.id,
                    executor,
                    reason,
                    shardId: role.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'roleDelete');
            }
        });
    }

    private _detectRoleChanges(oldRole: any, newRole: any): Record<string, any> {
        const changes: Record<string, any> = {};
        const fields = ['name', 'color', 'hoist', 'position', 'mentionable'];
        fields.forEach((field) => {
            if (oldRole[field] !== newRole[field])
                changes[field] = {
                    old: oldRole[field],
                    new: newRole[field],
                };
        });
        if (oldRole.permissions.bitfield !== newRole.permissions.bitfield) {
            changes.permissions = {
                old: oldRole.permissions.bitfield.toString(),
                new: newRole.permissions.bitfield.toString(),
            };
        }
        return changes;
    }

    private _setupChannelHandlers() {
        this.client.on('channelCreate', (channel: any) => {
            try {
                const { executor, reason } = this._fetchAuditDetails(
                    channel.guild,
                    AuditLogEvent.ChannelCreate as any,
                    channel.id,
                );
                this.logEvent('channelCreate', {
                    channelId: channel.id,
                    name: channel.name,
                    type: channel.type,
                    guildId: channel.guild.id,
                    position: channel.position,
                    parentId: channel.parentId || null,
                    nsfw: channel.nsfw,
                    rateLimitPerUser: channel.rateLimitPerUser || 0,
                    topic: channel.topic || null,
                    bitrate: channel.bitrate || null,
                    userLimit: channel.userLimit || null,
                    permissionOverwrites: channel.permissionOverwrites?.cache.size || 0,
                    executor,
                    reason,
                    shardId: channel.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'channelCreate');
            }
        });

        this.client.on('channelUpdate', (oldChannel: any, newChannel: any) => {
            try {
                const changes = this._detectChannelChanges(oldChannel, newChannel);
                if (Object.keys(changes).length === 0) return;
                const { executor, reason } = this._fetchAuditDetails(
                    newChannel.guild,
                    AuditLogEvent.ChannelUpdate as any,
                    newChannel.id,
                );
                this.logEvent('channelUpdate', {
                    channelId: newChannel.id,
                    guildId: newChannel.guild.id,
                    changes,
                    executor,
                    reason,
                    shardId: newChannel.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'channelUpdate');
            }
        });

        this.client.on('channelDelete', (channel: any) => {
            try {
                const { executor, reason } = this._fetchAuditDetails(
                    channel.guild,
                    AuditLogEvent.ChannelDelete as any,
                    channel.id,
                );
                this.logEvent('channelDelete', {
                    channelId: channel.id,
                    name: channel.name,
                    type: channel.type,
                    guildId: channel.guild.id,
                    executor,
                    reason,
                    shardId: channel.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'channelDelete');
            }
        });

        this.client.on('channelPinsUpdate', (channel: any, date: Date | null) => {
            try {
                const { executor } = this._fetchAuditDetails(
                    channel.guild,
                    (AuditLogEvent as any).ChannelPinsUpdate,
                );
                this.logEvent('channelPinsUpdate', {
                    channelId: channel.id,
                    lastPinTimestamp: date?.toISOString() || null,
                    guildId: channel.guild?.id,
                    executor,
                    shardId: channel.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'channelPinsUpdate');
            }
        });
    }

    private _detectChannelChanges(oldChannel: any, newChannel: any): Record<string, any> {
        const changes: Record<string, any> = {};
        const fields = [
            'name',
            'type',
            'position',
            'topic',
            'nsfw',
            'parentId',
            'rateLimitPerUser',
        ];
        fields.forEach((field) => {
            if (oldChannel[field] !== newChannel[field])
                changes[field] = {
                    old: oldChannel[field],
                    new: newChannel[field],
                };
        });
        if (
            oldChannel.permissionOverwrites?.cache.size !==
            newChannel.permissionOverwrites?.cache.size
        ) {
            changes.permissionOverwrites = {
                old: oldChannel.permissionOverwrites?.cache.size,
                new: newChannel.permissionOverwrites?.cache.size,
            };
        }
        if (oldChannel.bitrate !== newChannel.bitrate)
            changes.bitrate = {
                old: oldChannel.bitrate,
                new: newChannel.bitrate,
            };
        if (oldChannel.userLimit !== newChannel.userLimit)
            changes.userLimit = {
                old: oldChannel.userLimit,
                new: newChannel.userLimit,
            };
        return changes;
    }

    private _setupMessageHandlers() {
        this.client.on('messageCreate', (message: any) => {
            try {
                if (message.author?.bot || message.system) return;
                if (this._shouldThrottle(message.author.id, 'messageCreate')) return;

                let executor: string | null = null;
                try {
                    if (message.guild) {
                        const logs = message.guild.fetchAuditLogs({
                            limit: 1,
                            type: (AuditLogEvent as any).MessageContent,
                        });
                        executor = logs.entries?.first()?.executor?.id || null;
                    }
                } catch {}

                const attachmentsData =
                    message.attachments?.map((att: any) => ({
                        id: att.id,
                        filename: att.name,
                        contentType: att.contentType,
                        size: att.size,
                        url: att.url,
                        proxyUrl: att.proxyURL,
                        height: att.height,
                        width: att.width,
                        spoiler: att.spoiler,
                    })) || [];

                const embedsData =
                    message.embeds?.map((embed: any) => ({
                        type: embed.type,
                        url: embed.url,
                        provider: embed.provider
                            ? {
                                  name: embed.provider.name,
                                  url: embed.provider.url,
                              }
                            : null,
                        title: embed.title,
                        description: embed.description,
                        fields:
                            embed.fields?.map((f: any) => ({
                                name: f.name,
                                value: f.value,
                                inline: f.inline,
                            })) || [],
                        thumbnail: embed.thumbnail
                            ? {
                                  url: embed.thumbnail.url,
                                  height: embed.thumbnail.height,
                                  width: embed.thumbnail.width,
                              }
                            : null,
                        image: embed.image
                            ? {
                                  url: embed.image.url,
                                  height: embed.image.height,
                                  width: embed.image.width,
                              }
                            : null,
                        video: embed.video
                            ? {
                                  url: embed.video.url,
                                  height: embed.video.height,
                                  width: embed.video.width,
                              }
                            : null,
                        author: embed.author
                            ? {
                                  name: embed.author.name,
                                  url: embed.author.url,
                                  iconUrl: embed.author.iconURL,
                              }
                            : null,
                        footer: embed.footer
                            ? {
                                  text: embed.footer.text,
                                  iconUrl: embed.footer.iconURL,
                              }
                            : null,
                        timestamp: embed.timestamp,
                        color: embed.color,
                    })) || [];

                this.logEvent('messageCreate', {
                    messageId: message.id,
                    channelId: message.channel?.id,
                    authorId: message.author?.id,
                    authorUsername: message.author?.username,
                    authorDiscriminator: message.author?.discriminator,
                    authorGlobalName: message.author?.globalName || null,
                    authorAvatar: message.author?.avatar ? message.author.avatar : null,
                    authorBot: message.author?.bot,
                    content: message.content ? message.content.substring(0, 1000) : null,
                    tts: message.tts,
                    mentionedChannels:
                        message.mentions?.channels?.cache?.map((c: any) => c.id) || [],
                    mentionedRoles:
                        message.mentions?.roles?.cache?.map((r: any) => r.id) || [],
                    mentionedUsers:
                        message.mentions?.users?.cache?.map((u: any) => u.id) || [],
                    attachments: attachmentsData,
                    embeds: embedsData,
                    pinned: message.pinned,
                    webhookId: message.webhookId || null,
                    type: message.type,
                    activity: message.activity
                        ? {
                              type: message.activity.type,
                              partyId: message.activity.partyId,
                          }
                        : null,
                    applicationId: message.applicationId || null,
                    messageReference: message.reference
                        ? {
                              messageId: message.reference.messageId,
                              channelId: message.reference.channelId,
                              guildId: message.reference.guildId,
                          }
                        : null,
                    flags: message.flags?.bitfield.toString() || '0',
                    guildId: message.guild?.id,
                    executor,
                    shardId: message.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'messageCreate');
            }
        });

        this.client.on('messageDelete', async (message: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    message.guild,
                    AuditLogEvent.MessageDelete as any,
                    message.id,
                );
                this.logEvent('messageDelete', {
                    messageId: message.id,
                    channelId: message.channel?.id,
                    authorId: message.author?.id || null,
                    content: message.content
                        ? message.content.substring(0, 500)
                        : '[Unknown]',
                    attachments: message.attachments?.size || 0,
                    embeds: message.embeds?.length || 0,
                    pinned: message.pinned,
                    guildId: message.guild?.id,
                    executor,
                    reason,
                    shardId: message.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'messageDelete');
            }
        });

        this.client.on('messageDeleteBulk', async (messages: any) => {
            try {
                const channel = messages.first().channel;
                // FIXME: executor fetch can return null on bulk deletes, needs investigation
                const { executor, reason } = await this._fetchAuditDetails(
                    channel.guild,
                    AuditLogEvent.MessageBulkDelete as any,
                );
                this.logEvent('messageDeleteBulk', {
                    channelId: channel.id,
                    messageIds: messages.map((m: any) => m.id),
                    deleteCount: messages.size,
                    hasBotMessages: messages.some((m: any) => m.author?.bot),
                    guildId: channel.guild?.id,
                    executor,
                    reason,
                    shardId: channel.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'messageDeleteBulk');
            }
        });

        this.client.on('messageUpdate', async (oldMessage: any, newMessage: any) => {
            try {
                if (!oldMessage.content && !newMessage.content) return;
                if (oldMessage.content === newMessage.content) return;
                const { executor, reason } = await this._fetchAuditDetails(
                    newMessage.guild,
                    (AuditLogEvent as any).MessageUpdate,
                    newMessage.id,
                );
                this.logEvent('messageUpdate', {
                    messageId: newMessage.id,
                    channelId: newMessage.channel?.id,
                    authorId: newMessage.author?.id,
                    oldContent: oldMessage.content
                        ? oldMessage.content.substring(0, 500)
                        : '[Not Cached]',
                    newContent: newMessage.content
                        ? newMessage.content.substring(0, 500)
                        : null,
                    oldEditedTimestamp: oldMessage.editedTimestamp
                        ? new Date(oldMessage.editedTimestamp).toISOString()
                        : null,
                    newEditedTimestamp: newMessage.editedTimestamp
                        ? new Date(newMessage.editedTimestamp).toISOString()
                        : null,
                    guildId: newMessage.guild?.id,
                    executor,
                    reason,
                    shardId: newMessage.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'messageUpdate');
            }
        });

        this.client.on('messageStickyUpdate', (oldSticky: any, newSticky: any) => {
            try {
                this.logEvent('messageStickyUpdate', {
                    channelId: newSticky?.channelId || oldSticky?.channelId,
                    oldMessageId: oldSticky?.messageId,
                    newMessageId: newSticky?.messageId,
                    guildId: newSticky?.guildId || oldSticky?.guildId,
                    shardId: newSticky?.guild?.shardId || oldSticky?.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'messageStickyUpdate');
            }
        });
    }

    private _setupReactionHandlers() {
        this.client.on('messageReactionAdd', async (reaction: any, user: any) => {
            try {
                if (this._shouldThrottle(user.id, 'messageReactionAdd')) return;
                let executor: string | null = null;
                try {
                    if (reaction.message.guild)
                        executor =
                            (
                                await reaction.message.guild.fetchAuditLogs({
                                    limit: 1,
                                    type: (AuditLogEvent as any).MessageReactionAdd,
                                })
                            ).entries?.first()?.executor?.id || null;
                } catch {}
                const emojiData = reaction.emoji.id
                    ? {
                          id: reaction.emoji.id,
                          name: reaction.emoji.name,
                          animated: reaction.emoji.animated,
                      }
                    : {
                          name: reaction.emoji.name,
                      };
                this.logEvent('messageReactionAdd', {
                    messageId: reaction.message.id,
                    channelId: reaction.message.channel.id,
                    userId: user.id,
                    username: user.username,
                    guildId: reaction.message.guild?.id,
                    emoji: emojiData,
                    executor,
                    shardId: reaction.message.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'messageReactionAdd');
            }
        });

        this.client.on('messageReactionRemove', async (reaction: any, user: any) => {
            try {
                if (this._shouldThrottle(user.id, 'messageReactionRemove')) return;
                let executor: string | null = null;
                try {
                    if (reaction.message.guild)
                        executor =
                            (
                                await reaction.message.guild.fetchAuditLogs({
                                    limit: 1,
                                    type: (AuditLogEvent as any).MessageReactionRemove,
                                })
                            ).entries?.first()?.executor?.id || null;
                } catch {}
                const emojiData = reaction.emoji.id
                    ? {
                          id: reaction.emoji.id,
                          name: reaction.emoji.name,
                          animated: reaction.emoji.animated,
                      }
                    : {
                          name: reaction.emoji.name,
                      };
                this.logEvent('messageReactionRemove', {
                    messageId: reaction.message.id,
                    channelId: reaction.message.channel.id,
                    userId: user.id,
                    username: user.username,
                    guildId: reaction.message.guild?.id,
                    emoji: emojiData,
                    executor,
                    shardId: reaction.message.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'messageReactionRemove');
            }
        });

        this.client.on('messageReactionRemoveAll', async (message: any) => {
            try {
                let executor: string | null = null;
                try {
                    if (message.guild)
                        executor =
                            (
                                await message.guild.fetchAuditLogs({
                                    limit: 1,
                                    type: (AuditLogEvent as any).MessageReactionRemoveAll,
                                })
                            ).entries?.first()?.executor?.id || null;
                } catch {}
                this.logEvent('messageReactionRemoveAll', {
                    messageId: message.id,
                    channelId: message.channel?.id,
                    guildId: message.guild?.id,
                    executor,
                    shardId: message.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'messageReactionRemoveAll');
            }
        });

        this.client.on('messageReactionRemoveEmoji', async (reaction: any) => {
            try {
                let executor: string | null = null;
                try {
                    if (reaction.message.guild)
                        executor =
                            (
                                await reaction.message.guild.fetchAuditLogs({
                                    limit: 1,
                                    type: (AuditLogEvent as any)
                                        .MessageReactionRemoveEmoji,
                                })
                            ).entries?.first()?.executor?.id || null;
                } catch {}
                const emojiData = reaction.emoji.id
                    ? {
                          id: reaction.emoji.id,
                          name: reaction.emoji.name,
                          animated: reaction.emoji.animated,
                      }
                    : {
                          name: reaction.emoji.name,
                      };
                this.logEvent('messageReactionRemoveEmoji', {
                    messageId: reaction.message.id,
                    channelId: reaction.message.channel.id,
                    guildId: reaction.message.guild?.id,
                    emoji: emojiData,
                    executor,
                    shardId: reaction.message.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'messageReactionRemoveEmoji');
            }
        });
    }

    private _setupVoiceHandlers() {
        this.client.on('voiceStateUpdate', async (oldState: any, newState: any) => {
            try {
                if (oldState.id !== newState.id) return;
                if (this._shouldThrottle(newState.id, 'voiceStateUpdate')) return;
                const changes = this._detectVoiceStateChanges(oldState, newState);
                if (Object.keys(changes).length === 0) return;
                let executor: string | null = null;
                try {
                    if (newState.guild)
                        executor =
                            (
                                await newState.guild.fetchAuditLogs({
                                    limit: 1,
                                    type: (AuditLogEvent as any).VoiceChannelUpdate,
                                })
                            ).entries?.first()?.executor?.id || null;
                } catch {}
                this.logEvent('voiceStateUpdate', {
                    userId: newState.id,
                    guildId: newState.guild.id,
                    channelId: newState.channelId || null,
                    changes,
                    executor,
                    shardId: newState.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'voiceStateUpdate');
            }
        });

        this.client.on('voiceAdapterCreator', (adapters: any) => {
            try {
                this.logEvent('voiceAdapterCreator', {
                    guildId: adapters.guild.id,
                    channelId: adapters.channel.id,
                    shardId: adapters.guild.shardId,
                });
            } catch (err) {
                console.error('[audit] voiceAdapterCreator:', err);
            }
        });
    }

    private _detectVoiceStateChanges(oldState: any, newState: any): Record<string, any> {
        const changes: Record<string, any> = {};
        const fields = [
            'channelId',
            'selfMute',
            'selfDeaf',
            'selfVideo',
            'streaming',
            'serverMute',
            'serverDeaf',
            'suppress',
        ];
        fields.forEach((field) => {
            if (oldState[field] !== newState[field])
                changes[field] = {
                    old: oldState[field],
                    new: newState[field],
                };
        });
        if (!oldState.channelId && newState.channelId) changes.action = 'join';
        else if (oldState.channelId && !newState.channelId) changes.action = 'leave';
        else if (oldState.channelId !== newState.channelId) {
            changes.action = 'move';
            changes.oldChannelId = oldState.channelId;
        }
        return changes;
    }

    private _setupTypingHandlers() {
        this.client.on('typingStart', async (typing: any) => {
            try {
                if (this._shouldThrottle(typing.user.id, 'typingStart')) return;
                this.logEvent('typingStart', {
                    userId: typing.user.id,
                    username: typing.user.username,
                    channelId: typing.channel.id,
                    guildId: typing.guild?.id,
                    startedTimestamp: new Date(typing.startedTimestamp).toISOString(),
                    shardId: typing.guild?.shardId,
                });
            } catch {}
        });
    }

    private _setupPresenceHandlers() {
        this.client.on('presenceUpdate', async (oldPresence: any, newPresence: any) => {
            try {
                const userId = newPresence.userId;
                if (this._shouldThrottle(userId, 'presenceUpdate')) return;
                const changes = this._detectPresenceChanges(oldPresence, newPresence);
                if (Object.keys(changes).length === 0) return;
                this.logEvent('presenceUpdate', {
                    userId,
                    guildId: newPresence.guild?.id,
                    changes,
                    shardId: newPresence.guild?.shardId,
                });
            } catch (err) {
                if (this.metrics) this.metrics.totalErrors++;
            }
        });
    }

    private _detectPresenceChanges(
        oldPresence: any,
        newPresence: any,
    ): Record<string, any> {
        const changes: Record<string, any> = {};
        if (oldPresence?.status !== newPresence.status)
            changes.status = {
                old: oldPresence?.status ?? 'offline',
                new: newPresence.status,
            };

        const oldActivities = oldPresence?.activities ?? [];
        const newActivities = newPresence.activities ?? [];
        if (oldActivities.length !== newActivities.length) {
            changes.activities = {
                old: oldActivities.map((a: any) => ({
                    name: a.name,
                    type: a.type,
                    url: a.url,
                })),
                new: newActivities.map((a: any) => ({
                    name: a.name,
                    type: a.type,
                    url: a.url,
                })),
            };
        }
        if (oldPresence?.clientStatus !== newPresence.clientStatus)
            changes.clientStatus = {
                old: oldPresence?.clientStatus,
                new: newPresence.clientStatus,
            };
        return changes;
    }

    private _setupUserHandlers() {
        this.client.on('userUpdate', async (oldUser: any, newUser: any) => {
            try {
                const changes = this._detectUserChanges(oldUser, newUser);
                if (Object.keys(changes).length === 0) return;
                this.logEvent('userUpdate', {
                    userId: newUser.id,
                    changes,
                });
            } catch (err) {
                this._handleError(err, 'userUpdate');
            }
        });
    }

    private _detectUserChanges(oldUser: any, newUser: any): Record<string, any> {
        const changes: Record<string, any> = {};
        if (oldUser.username !== newUser.username)
            changes.username = {
                old: oldUser.username,
                new: newUser.username,
            };
        if (oldUser.discriminator !== newUser.discriminator) {
            changes.discriminator = {
                old: oldUser.discriminator,
                new: newUser.discriminator,
            };
        }
        if (oldUser.globalName !== newUser.globalName)
            changes.globalName = {
                old: oldUser.globalName,
                new: newUser.globalName,
            };
        if (oldUser.avatar !== newUser.avatar)
            changes.avatar = {
                old: !!oldUser.avatar,
                new: !!newUser.avatar,
            };
        if (oldUser.banner !== newUser.banner) {
            changes.banner = {
                old: !!oldUser.banner,
                new: !!newUser.banner,
            };
        }
        if (oldUser.accentColor !== newUser.accentColor)
            changes.accentColor = {
                old: oldUser.accentColor,
                new: newUser.accentColor,
            };
        if (oldUser.locale !== newUser.locale)
            changes.locale = {
                old: oldUser.locale,
                new: newUser.locale,
            };
        if (oldUser.verified !== newUser.verified)
            changes.verified = {
                old: oldUser.verified,
                new: newUser.verified,
            };
        if (oldUser.mfaEnabled !== newUser.mfaEnabled)
            changes.mfaEnabled = {
                old: oldUser.mfaEnabled,
                new: newUser.mfaEnabled,
            };
        if (oldUser.flags !== newUser.flags)
            changes.flags = {
                old: oldUser.flags?.bitfield.toString(),
                new: newUser.flags?.bitfield.toString(),
            };
        if (oldUser.publicFlags !== newUser.publicFlags) {
            changes.publicFlags = {
                old: oldUser.publicFlags?.bitfield.toString(),
                new: newUser.publicFlags?.bitfield.toString(),
            };
        }
        if (oldUser.premiumType !== newUser.premiumType)
            changes.premiumType = {
                old: oldUser.premiumType,
                new: newUser.premiumType,
            };
        return changes;
    }

    private _setupEmojiHandlers() {
        this.client.on('emojiCreate', async (emoji: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    emoji.guild,
                    AuditLogEvent.EmojiCreate as any,
                    emoji.id,
                );
                this.logEvent('emojiCreate', {
                    emojiId: emoji.id,
                    name: emoji.name,
                    animated: emoji.animated,
                    url: emoji.url,
                    guildId: emoji.guild.id,
                    executor,
                    reason,
                    shardId: emoji.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'emojiCreate');
            }
        });

        this.client.on('emojiUpdate', async (oldEmoji: any, newEmoji: any) => {
            try {
                const changes = this._detectEmojiChanges(oldEmoji, newEmoji);
                if (Object.keys(changes).length === 0) return;
                const { executor, reason } = await this._fetchAuditDetails(
                    newEmoji.guild,
                    AuditLogEvent.EmojiUpdate as any,
                    newEmoji.id,
                );
                this.logEvent('emojiUpdate', {
                    emojiId: newEmoji.id,
                    guildId: newEmoji.guild.id,
                    changes,
                    executor,
                    reason,
                    shardId: newEmoji.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'emojiUpdate');
            }
        });

        this.client.on('emojiDelete', async (emoji: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    emoji.guild,
                    AuditLogEvent.EmojiDelete as any,
                    emoji.id,
                );
                this.logEvent('emojiDelete', {
                    emojiId: emoji.id,
                    name: emoji.name,
                    guildId: emoji.guild.id,
                    executor,
                    reason,
                    shardId: emoji.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'emojiDelete');
            }
        });
    }

    private _detectEmojiChanges(oldEmoji: any, newEmoji: any) {
        if (oldEmoji.name === newEmoji.name) return {};
        return {
            name: {
                old: oldEmoji.name,
                new: newEmoji.name,
            },
        };
    }

    private _setupStickerHandlers() {
        this.client.on('stickerCreate', async (sticker: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    sticker.guild,
                    AuditLogEvent.StickerCreate as any,
                    sticker.id,
                );
                this.logEvent('stickerCreate', {
                    stickerId: sticker.id,
                    name: sticker.name,
                    description: sticker.description || null,
                    tags: sticker.tags || null,
                    type: sticker.type,
                    formatType: sticker.format,
                    guildId: sticker.guild.id,
                    available: sticker.available,
                    url: sticker.url,
                    executor,
                    reason,
                    shardId: sticker.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'stickerCreate');
            }
        });

        this.client.on('stickerUpdate', async (oldSticker: any, newSticker: any) => {
            try {
                const changes = this._detectStickerChanges(oldSticker, newSticker);
                if (Object.keys(changes).length === 0) return;
                const { executor, reason } = await this._fetchAuditDetails(
                    newSticker.guild,
                    AuditLogEvent.StickerUpdate as any,
                    newSticker.id,
                );
                this.logEvent('stickerUpdate', {
                    stickerId: newSticker.id,
                    guildId: newSticker.guild.id,
                    changes,
                    executor,
                    reason,
                    shardId: newSticker.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'stickerUpdate');
            }
        });

        this.client.on('stickerDelete', async (sticker: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    sticker.guild,
                    AuditLogEvent.StickerDelete as any,
                    sticker.id,
                );
                this.logEvent('stickerDelete', {
                    stickerId: sticker.id,
                    name: sticker.name,
                    guildId: sticker.guild.id,
                    executor,
                    reason,
                    shardId: sticker.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'stickerDelete');
            }
        });
    }

    private _detectStickerChanges(oldSticker: any, newSticker: any) {
        const changes: Record<string, any> = {};
        if (oldSticker.name !== newSticker.name)
            changes.name = {
                old: oldSticker.name,
                new: newSticker.name,
            };
        if (oldSticker.description !== newSticker.description)
            changes.description = {
                old: oldSticker.description,
                new: newSticker.description,
            };
        if (oldSticker.tags !== newSticker.tags)
            changes.tags = {
                old: oldSticker.tags,
                new: newSticker.tags,
            };
        return changes;
    }

    private _setupThreadHandlers() {
        this.client.on('threadCreate', async (thread: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    thread.guild,
                    AuditLogEvent.ThreadCreate as any,
                    thread.id,
                );
                this.logEvent('threadCreate', {
                    threadId: thread.id,
                    name: thread.name,
                    type: thread.type,
                    parentId: thread.parentId,
                    guildId: thread.guild.id,
                    ownerId: thread.ownerId,
                    archived: thread.archived,
                    locked: thread.locked,
                    invitable: thread.invitable,
                    autoArchiveDuration: thread.autoArchiveDuration,
                    executor,
                    reason,
                    shardId: thread.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'threadCreate');
            }
        });

        this.client.on('threadUpdate', async (oldThread: any, newThread: any) => {
            try {
                const changes = this._detectThreadChanges(oldThread, newThread);
                if (Object.keys(changes).length === 0) return;
                const { executor, reason } = await this._fetchAuditDetails(
                    newThread.guild,
                    AuditLogEvent.ThreadUpdate as any,
                    newThread.id,
                );
                this.logEvent('threadUpdate', {
                    threadId: newThread.id,
                    guildId: newThread.guild.id,
                    changes,
                    executor,
                    reason,
                    shardId: newThread.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'threadUpdate');
            }
        });

        this.client.on('threadDelete', async (thread: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    thread.guild,
                    AuditLogEvent.ThreadDelete as any,
                    thread.id,
                );
                this.logEvent('threadDelete', {
                    threadId: thread.id,
                    name: thread.name,
                    parentId: thread.parentId,
                    guildId: thread.guild.id,
                    executor,
                    reason,
                    shardId: thread.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'threadDelete');
            }
        });

        this.client.on('threadListSync', (threads: any, channel: any) => {
            try {
                this.logEvent('threadListSync', {
                    channelId: channel.id,
                    threadIds: threads.map((t: any) => t.id),
                    guildId: channel.guild?.id,
                    threadCount: threads.size || threads.length,
                    shardId: channel.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'threadListSync');
            }
        });

        this.client.on('threadMemberUpdate', (oldMember: any, newMember: any) => {
            try {
                const changes: Record<string, any> = {};
                if (oldMember.joinedTimestamp !== newMember.joinedTimestamp) {
                    changes.joinTimestamp = {
                        old: oldMember.joinedTimestamp
                            ? new Date(oldMember.joinedTimestamp).toISOString()
                            : null,
                        new: newMember.joinedTimestamp
                            ? new Date(newMember.joinedTimestamp).toISOString()
                            : null,
                    };
                }
                if (Object.keys(changes).length > 0) {
                    this.logEvent('threadMemberUpdate', {
                        threadId: newMember.thread.id,
                        userId: newMember.id,
                        guildId: newMember.thread.guild?.id,
                        changes,
                        shardId: newMember.thread.guild?.shardId,
                    });
                }
            } catch (err) {
                this._handleError(err, 'threadMemberUpdate');
            }
        });

        this.client.on('threadPublicArchived', (thread: any) => {
            try {
                this.logEvent('threadPublicArchived', {
                    threadId: thread.id,
                    guildId: thread.guild.id,
                    archivedTimestamp: thread.archivedTimestamp
                        ? new Date(thread.archivedTimestamp).toISOString()
                        : null,
                    shardId: thread.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'threadPublicArchived');
            }
        });

        this.client.on('threadPrivateArchived', (thread: any) => {
            try {
                this.logEvent('threadPrivateArchived', {
                    threadId: thread.id,
                    guildId: thread.guild.id,
                    archivedTimestamp: thread.archivedTimestamp
                        ? new Date(thread.archivedTimestamp).toISOString()
                        : null,
                    shardId: thread.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'threadPrivateArchived');
            }
        });

        this.client.on('threadUserJoined', (member: any, thread: any) => {
            try {
                if (this._shouldThrottle(member.id, 'threadUserJoined')) return;
                this.logEvent('threadUserJoined', {
                    threadId: thread.id,
                    userId: member.id,
                    guildId: thread.guild.id,
                    shardId: thread.guild.shardId,
                });
            } catch {
                /* ignore */
            }
        });

        this.client.on('threadUserLeft', (member: any, thread: any) => {
            try {
                if (this._shouldThrottle(member.id, 'threadUserLeft')) return;
                this.logEvent('threadUserLeft', {
                    threadId: thread.id,
                    userId: member.id,
                    guildId: thread.guild.id,
                    shardId: thread.guild.shardId,
                });
            } catch {
                /* ignore */
            }
        });
    }

    private _detectThreadChanges(oldThread: any, newThread: any): Record<string, any> {
        const changes: Record<string, any> = {};
        const fields = ['name', 'archived', 'locked', 'invitable', 'autoArchiveDuration'];
        fields.forEach((field) => {
            if (oldThread[field] !== newThread[field])
                changes[field] = {
                    old: oldThread[field],
                    new: newThread[field],
                };
        });
        if (oldThread.archivedTimestamp !== newThread.archivedTimestamp) {
            changes.archivedTimestamp = {
                old: oldThread.archivedTimestamp
                    ? new Date(oldThread.archivedTimestamp).toISOString()
                    : null,
                new: newThread.archivedTimestamp
                    ? new Date(newThread.archivedTimestamp).toISOString()
                    : null,
            };
        }
        return changes;
    }

    private _setupStageHandlers() {
        this.client.on('stageInstanceCreate', async (stageInstance: any) => {
            try {
                const { executor } = await this._fetchAuditDetails(
                    stageInstance.guild,
                    AuditLogEvent.StageInstanceCreate as any,
                );
                this.logEvent('stageInstanceCreate', {
                    stageInstanceId: stageInstance.id,
                    channelId: stageInstance.channelId,
                    guildId: stageInstance.guild.id,
                    topic: stageInstance.topic,
                    privacyLevel: stageInstance.privacyLevel,
                    discoverable: stageInstance.discoverable,
                    executor,
                    shardId: stageInstance.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'stageInstanceCreate');
            }
        });

        this.client.on(
            'stageInstanceUpdate',
            async (oldInstance: any, newInstance: any) => {
                try {
                    const changes = this._detectStageInstanceChanges(
                        oldInstance,
                        newInstance,
                    );
                    if (Object.keys(changes).length === 0) return;
                    const { executor } = await this._fetchAuditDetails(
                        newInstance.guild,
                        AuditLogEvent.StageInstanceUpdate as any,
                    );
                    this.logEvent('stageInstanceUpdate', {
                        stageInstanceId: newInstance.id,
                        channelId: newInstance.channelId,
                        guildId: newInstance.guild.id,
                        changes,
                        executor,
                        shardId: newInstance.guild.shardId,
                    });
                } catch (err) {
                    this._handleError(err, 'stageInstanceUpdate');
                }
            },
        );

        this.client.on('stageInstanceDelete', async (stageInstance: any) => {
            try {
                const { executor } = await this._fetchAuditDetails(
                    stageInstance.guild,
                    AuditLogEvent.StageInstanceDelete as any,
                );
                this.logEvent('stageInstanceDelete', {
                    stageInstanceId: stageInstance.id,
                    channelId: stageInstance.channelId,
                    guildId: stageInstance.guild.id,
                    topic: stageInstance.topic,
                    privacyLevel: stageInstance.privacyLevel,
                    executor,
                    shardId: stageInstance.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'stageInstanceDelete');
            }
        });
    }

    private _detectStageInstanceChanges(
        oldInstance: any,
        newInstance: any,
    ): Record<string, any> {
        const changes: Record<string, any> = {};
        if (oldInstance.topic !== newInstance.topic)
            changes.topic = {
                old: oldInstance.topic,
                new: newInstance.topic,
            };
        if (oldInstance.privacyLevel !== newInstance.privacyLevel)
            changes.privacyLevel = {
                old: oldInstance.privacyLevel,
                new: newInstance.privacyLevel,
            };
        if (oldInstance.discoverable !== newInstance.discoverable)
            changes.discoverable = {
                old: oldInstance.discoverable,
                new: newInstance.discoverable,
            };
        return changes;
    }

    private _setupScheduledEventHandlers() {
        this.client.on('guildScheduledEventCreate', async (scheduledEvent: any) => {
            try {
                const { executor } = await this._fetchAuditDetails(
                    scheduledEvent.guild,
                    (AuditLogEvent as any).ScheduledEventCreate,
                );
                this.logEvent('guildScheduledEventCreate', {
                    eventId: scheduledEvent.id,
                    guildId: scheduledEvent.guild.id,
                    name: scheduledEvent.name,
                    privacyLevel: scheduledEvent.privacyLevel,
                    status: scheduledEvent.status,
                    entityType: scheduledEvent.entityType,
                    channelId: scheduledEvent.channelId || null,
                    creatorId: scheduledEvent.creatorId,
                    description: scheduledEvent.description || null,
                    scheduledStartTime: scheduledEvent.scheduledStartTimestamp
                        ? new Date(scheduledEvent.scheduledStartTimestamp).toISOString()
                        : null,
                    scheduledEndTime: scheduledEvent.scheduledEndTimestamp
                        ? new Date(scheduledEvent.scheduledEndTimestamp).toISOString()
                        : null,
                    entityId: scheduledEvent.entityId || null,
                    entityMetadata: scheduledEvent.entityMetadata || null,
                    userCount: scheduledEvent.userCount || 0,
                    executor,
                    shardId: scheduledEvent.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'guildScheduledEventCreate');
            }
        });

        this.client.on(
            'guildScheduledEventUpdate',
            async (oldEvent: any, newEvent: any) => {
                try {
                    const changes = this._detectScheduledEventChanges(oldEvent, newEvent);
                    if (Object.keys(changes).length === 0) return;
                    const { executor } = await this._fetchAuditDetails(
                        newEvent.guild,
                        (AuditLogEvent as any).ScheduledEventUpdate,
                    );
                    this.logEvent('guildScheduledEventUpdate', {
                        eventId: newEvent.id,
                        guildId: newEvent.guild.id,
                        changes,
                        executor,
                        shardId: newEvent.guild.shardId,
                    });
                } catch (err) {
                    this._handleError(err, 'guildScheduledEventUpdate');
                }
            },
        );

        this.client.on('guildScheduledEventDelete', async (scheduledEvent: any) => {
            try {
                const { executor } = await this._fetchAuditDetails(
                    scheduledEvent.guild,
                    (AuditLogEvent as any).ScheduledEventDelete,
                );
                this.logEvent('guildScheduledEventDelete', {
                    eventId: scheduledEvent.id,
                    guildId: scheduledEvent.guild.id,
                    name: scheduledEvent.name,
                    channelId: scheduledEvent.channelId || null,
                    executor,
                    shardId: scheduledEvent.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'guildScheduledEventDelete');
            }
        });

        this.client.on(
            'guildScheduledEventUserAdd',
            async (scheduledEvent: any, user: any) => {
                try {
                    if (this._shouldThrottle(user.id, 'guildScheduledEventUserAdd'))
                        return;
                    this.logEvent('guildScheduledEventUserAdd', {
                        eventId: scheduledEvent.id,
                        userId: user.id,
                        guildId: scheduledEvent.guild.id,
                        shardId: scheduledEvent.guild.shardId,
                    });
                } catch (err) {
                    this._handleError(err, 'guildScheduledEventUserAdd');
                }
            },
        );

        this.client.on(
            'guildScheduledEventUserRemove',
            async (scheduledEvent: any, user: any) => {
                try {
                    if (this._shouldThrottle(user.id, 'guildScheduledEventUserRemove'))
                        return;
                    this.logEvent('guildScheduledEventUserRemove', {
                        eventId: scheduledEvent.id,
                        userId: user.id,
                        guildId: scheduledEvent.guild.id,
                        shardId: scheduledEvent.guild.shardId,
                    });
                } catch (err) {
                    this._handleError(err, 'guildScheduledEventUserRemove');
                }
            },
        );
    }

    private _detectScheduledEventChanges(
        oldEvent: any,
        newEvent: any,
    ): Record<string, any> {
        const changes: Record<string, any> = {};
        const fields = ['name', 'privacyLevel', 'status', 'entityType', 'description'];
        fields.forEach((field) => {
            if (oldEvent[field] !== newEvent[field])
                changes[field] = {
                    old: oldEvent[field],
                    new: newEvent[field],
                };
        });
        if (oldEvent.scheduledStartTimestamp !== newEvent.scheduledStartTimestamp) {
            changes.scheduledStartTime = {
                old: oldEvent.scheduledStartTimestamp
                    ? new Date(oldEvent.scheduledStartTimestamp).toISOString()
                    : null,
                new: newEvent.scheduledStartTimestamp
                    ? new Date(newEvent.scheduledStartTimestamp).toISOString()
                    : null,
            };
        }
        if (oldEvent.scheduledEndTimestamp !== newEvent.scheduledEndTimestamp) {
            changes.scheduledEndTime = {
                old: oldEvent.scheduledEndTimestamp
                    ? new Date(oldEvent.scheduledEndTimestamp).toISOString()
                    : null,
                new: newEvent.scheduledEndTimestamp
                    ? new Date(newEvent.scheduledEndTimestamp).toISOString()
                    : null,
            };
        }
        if (oldEvent.channelId !== newEvent.channelId)
            changes.channelId = {
                old: oldEvent.channelId,
                new: newEvent.channelId,
            };
        if (oldEvent.entityId !== newEvent.entityId)
            changes.entityId = {
                old: oldEvent.entityId,
                new: newEvent.entityId,
            };
        if (
            JSON.stringify(oldEvent.entityMetadata) !==
            JSON.stringify(newEvent.entityMetadata)
        ) {
            changes.entityMetadata = {
                old: oldEvent.entityMetadata,
                new: newEvent.entityMetadata,
            };
        }
        return changes;
    }

    private _setupInviteHandlers() {
        this.client.on('inviteCreate', async (invite: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    invite.guild,
                    AuditLogEvent.InviteCreate as any,
                );
                this.logEvent('inviteCreate', {
                    code: invite.code,
                    channelId: invite.channelId,
                    inviterId: invite.inviter?.id || null,
                    guildId: invite.guildId,
                    targetUserId: invite.targetUser?.id || null,
                    targetUserType: invite.targetUserType || 0,
                    maxAge: invite.maxAge,
                    maxUses: invite.maxUses,
                    uses: invite.uses,
                    temporary: invite.temporary,
                    unique: invite.unique,
                    targetApplicationId: invite.targetApplication?.id || null,
                    executor,
                    reason,
                    shardId: invite.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'inviteCreate');
            }
        });

        this.client.on('inviteDelete', async (invite: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    invite.guild,
                    AuditLogEvent.InviteDelete as any,
                );
                this.logEvent('inviteDelete', {
                    code: invite.code,
                    channelId: invite.channelId,
                    guildId: invite.guildId,
                    uses: invite.uses,
                    maxUses: invite.maxUses,
                    executor,
                    reason,
                    shardId: invite.guild?.shardId,
                });
            } catch (err) {
                this._handleError(err, 'inviteDelete');
            }
        });
    }

    private _setupWebhookHandlers() {
        this.client.on('webhookUpdate', async (channel: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    channel.guild,
                    AuditLogEvent.WebhookUpdate as any,
                    channel.id,
                );
                this.logEvent('webhookUpdate', {
                    channelId: channel.id,
                    guildId: channel.guild.id,
                    executor,
                    reason,
                    shardId: channel.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'webhookUpdate');
            }
        });
    }

    private _setupIntegrationHandlers() {
        this.client.on('integrationCreate', async (integration: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    integration.guild,
                    AuditLogEvent.IntegrationCreate as any,
                    integration.id,
                );
                this.logEvent('integrationCreate', {
                    integrationId: integration.id,
                    name: integration.name,
                    type: integration.type,
                    enabled: integration.enabled,
                    syncing: integration.syncing,
                    subscribedChannels:
                        integration.subscribedChannels?.map((c: any) => c.id) || [],
                    emoji: integration.emoji || null,
                    expireBehavior: integration.expireBehavior,
                    expireGracePeriod: integration.expireGracePeriod,
                    user: integration.user
                        ? {
                              id: integration.user.id,
                              username: integration.user.username,
                          }
                        : null,
                    account: integration.account
                        ? {
                              id: integration.account.id,
                              name: integration.account.name,
                          }
                        : null,
                    guildId: integration.guild.id,
                    executor,
                    reason,
                    shardId: integration.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'integrationCreate');
            }
        });

        this.client.on('integrationUpdate', async (integration: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    integration.guild,
                    AuditLogEvent.IntegrationUpdate as any,
                    integration.id,
                );
                this.logEvent('integrationUpdate', {
                    integrationId: integration.id,
                    guildId: integration.guild.id,
                    executor,
                    reason,
                    shardId: integration.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'integrationUpdate');
            }
        });

        this.client.on('integrationReset', async (integration: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    integration.guild,
                    (AuditLogEvent as any).IntegrationReset,
                    integration.id,
                );
                this.logEvent('integrationReset', {
                    integrationId: integration.id,
                    guildId: integration.guild.id,
                    executor,
                    reason,
                    shardId: integration.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'integrationReset');
            }
        });
    }

    private _setupAutoModHandlers() {
        this.client.on('autoModerationRuleCreate', async (autoModerationRule: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    autoModerationRule.guild,
                    AuditLogEvent.AutoModerationRuleCreate as any,
                    autoModerationRule.id,
                );
                this.logEvent('autoModerationRuleCreate', {
                    ruleId: autoModerationRule.id,
                    name: autoModerationRule.name,
                    guildId: autoModerationRule.guild.id,
                    executor,
                    reason,
                    eventType: autoModerationRule.eventType,
                    triggerType: autoModerationRule.triggerType,
                    triggerMetadata: autoModerationRule.triggerMetadata || null,
                    creatorId: autoModerationRule.creatorId,
                    enabled: autoModerationRule.enabled,
                    exemptRoles: autoModerationRule.exemptRoles,
                    exemptChannels: autoModerationRule.exemptChannels,
                    actions: autoModerationRule.actions.map((a: any) => ({
                        type: a.type,
                        metadata: a.metadata || null,
                    })),
                    shardId: autoModerationRule.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'autoModerationRuleCreate');
            }
        });

        this.client.on('autoModerationRuleUpdate', async (oldRule: any, newRule: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    newRule.guild,
                    AuditLogEvent.AutoModerationRuleUpdate as any,
                    newRule.id,
                );
                this.logEvent('autoModerationRuleUpdate', {
                    ruleId: newRule.id,
                    guildId: newRule.guild.id,
                    executor,
                    reason,
                    shardId: newRule.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'autoModerationRuleUpdate');
            }
        });

        this.client.on('autoModerationRuleDelete', async (rule: any) => {
            try {
                const { executor, reason } = await this._fetchAuditDetails(
                    rule.guild,
                    AuditLogEvent.AutoModerationRuleDelete as any,
                    rule.id,
                );
                this.logEvent('autoModerationRuleDelete', {
                    ruleId: rule.id,
                    guildId: rule.guild.id,
                    executor,
                    reason,
                    shardId: rule.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'autoModerationRuleDelete');
            }
        });

        this.client.on('autoModerationActionExecution', async (action: any) => {
            try {
                this.logEvent('autoModerationActionExecution', {
                    actionId: action.action.id,
                    ruleId: action.rule.id,
                    guildId: action.guild.id,
                    channelId: action.channel?.id || null,
                    messageId: action.messageId || null,
                    alertChannelId: action.extra?.alertChannelId || null,
                    userId: action.extra?.userId || null,
                    matchContent: action.extra?.content || null,
                    actionType: action.action.type,
                    metadata: action.action.metadata || null,
                    systemMessageChannelId: action.extra?.systemMessageChannelId || null,
                    shardId: action.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'autoModerationActionExecution');
            }
        });
    }

    private _setupClientHandlers() {
        this.client.on('debug', (info: string) => {
            try {
                if (this.config.logLevel === 'debug')
                    this.logEvent('debug', {
                        info: info.substring(0, 1000),
                    });
            } catch (err) {
                this._handleError(err, 'debug');
            }
        });

        this.client.on('warn', (info: string) => {
            try {
                this.logEvent('warn', { info });
            } catch (err) {
                this._handleError(err, 'warn');
            }
        });

        this.client.on('error', (error: Error) => {
            try {
                this.logEvent('error', {
                    error: error.message,
                    stack: error.stack,
                });
            } catch (err) {
                this._handleError(err, 'error');
            }
        });

        this.client.on('invalidated', () => {
            try {
                this.logEvent('invalidated', {
                    userId: this.client.user?.id,
                });
            } catch (err) {
                this._handleError(err, 'invalidated');
            }
        });

        this.client.on('shardDisconnect', (event: any, id: number) => {
            try {
                this.logEvent('shardDisconnect', {
                    shardId: id,
                    event,
                    closeEvent: event.code,
                });
            } catch (err) {
                this._handleError(err, 'shardDisconnect');
            }
        });

        this.client.on('shardError', (error: Error, id: number) => {
            try {
                this.logEvent('shardError', {
                    shardId: id,
                    error: error.message,
                });
            } catch (err) {
                this._handleError(err, 'shardError');
            }
        });

        this.client.on('shardReady', (id: number) => {
            try {
                this.logEvent('shardReady', {
                    shardId: id,
                });
            } catch (err) {
                this._handleError(err, 'shardReady');
            }
        });

        this.client.on('shardReconnecting', (id: number) => {
            try {
                this.logEvent('shardReconnecting', { shardId: id });
            } catch (err) {
                this._handleError(err, 'shardReconnecting');
            }
        });

        this.client.on('shardResume', (id: number, resumed: number) => {
            try {
                this.logEvent('shardResume', {
                    shardId: id,
                    resumed,
                });
            } catch (err) {
                this._handleError(err, 'shardResume');
            }
        });

        (this.client.ws as any).on('close', (code: number, reason: Buffer) => {
            try {
                this.logEvent('wsClose', {
                    code,
                    reason: reason.toString(),
                });
            } catch (err) {
                this._handleError(err, 'wsClose');
            }
        });
    }

    private _setupInteractionHandlers() {
        this.client.on('interactionCreate', async (interaction: any) => {
            try {
                if (this._shouldThrottle(interaction.user.id, 'interactionCreate'))
                    return;
                const data: Record<string, any> = {
                    id: interaction.id,
                    type: interaction.type,
                    guildId: interaction.guild?.id || null,
                    channelId: interaction.channel?.id || null,
                    userId: interaction.user.id,
                    username: interaction.user.username,
                    applicationId: interaction.applicationId,
                    token: '[REDACTED]',
                    version: interaction.version,
                    shardId: interaction.guild?.shardId,
                };
                if (interaction.isChatInputCommand()) {
                    data.commandName = interaction.commandName;
                    data.options = interaction.options.data.map((opt: any) => ({
                        name: opt.name,
                        type: opt.type,
                        value: opt.value
                            ? typeof opt.value === 'string'
                                ? opt.value.substring(0, 100)
                                : opt.value
                            : null,
                        focused: opt.focused,
                    }));
                } else if (interaction.isMessageComponent()) {
                    data.customId = interaction.customId;
                    data.componentType = interaction.componentType;
                } else if (interaction.isModalSubmit()) {
                    data.customId = interaction.customId;
                    data.components = interaction.fields.components.map((comp: any) => ({
                        type: comp.type,
                        components: comp.components.map((c: any) => ({
                            type: c.type,
                            customId: c.customId,
                            value: c.value ? c.value.substring(0, 100) : null,
                        })),
                    }));
                } else if (interaction.isAutocomplete()) {
                    data.commandName = interaction.commandName;
                    data.focusedOption =
                        interaction.options.getFocused(true)?.name || null;
                }
                this.logEvent('interactionCreate', data);
            } catch (err) {
                this._handleError(err, 'interactionCreate');
            }
        });
    }

    private _setupApplicationCommandHandlers() {
        this.client.on('applicationCommandPermissionsUpdate', async (data: any) => {
            try {
                const { executor } = await this._fetchAuditDetails(
                    data.guild,
                    AuditLogEvent.ApplicationCommandPermissionUpdate as any,
                );
                this.logEvent('applicationCommandPermissionsUpdate', {
                    applicationId: data.applicationId,
                    guildId: data.guildId,
                    commandId: data.commandId || null,
                    permissions:
                        data.permissions?.map((p: any) => ({
                            id: p.id,
                            type: p.type,
                            permission: p.permission,
                        })) || [],
                    executor,
                    shardId: data.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'applicationCommandPermissionsUpdate');
            }
        });
    }

    private _setupGuildStickerHandlers() {
        this.client.on('guildStickersUpdate', async (guild: any, stickers: any) => {
            try {
                this.logEvent('guildStickersUpdate', {
                    guildId: guild.id,
                    stickerIds: stickers.map((s: any) => s.id),
                    stickerCount: stickers.length,
                    shardId: guild.shardId,
                });
            } catch (e) {
                this._handleError(e, 'guildStickersUpdate');
            }
        });
    }

    private _setupEntitlementHandlers() {
        this.client.on('entitlementCreate', (entitlement: any) => {
            try {
                this.logEvent('entitlementCreate', {
                    id: entitlement.id,
                    skuId: entitlement.skuId,
                    applicationId: entitlement.applicationId,
                    userId: entitlement.userId || null,
                    guildId: entitlement.guildId || null,
                    type: entitlement.type,
                    consumed: entitlement.consumed,
                    endsAt: entitlement.endsAt
                        ? new Date(entitlement.endsAt).toISOString()
                        : null,
                    shardId: entitlement.guildId
                        ? this.client.guilds.cache.get(entitlement.guildId)?.shardId
                        : null,
                });
            } catch (err) {
                this._handleError(err, 'entitlementCreate');
            }
        });

        this.client.on(
            'entitlementUpdate',
            (oldEntitlement: any, newEntitlement: any) => {
                try {
                    const changes: Record<string, any> = {};
                    if (oldEntitlement.consumed !== newEntitlement.consumed)
                        changes.consumed = {
                            old: oldEntitlement.consumed,
                            new: newEntitlement.consumed,
                        };
                    if (oldEntitlement.endsAt !== newEntitlement.endsAt) {
                        changes.endsAt = {
                            old: oldEntitlement.endsAt
                                ? new Date(oldEntitlement.endsAt).toISOString()
                                : null,
                            new: newEntitlement.endsAt
                                ? new Date(newEntitlement.endsAt).toISOString()
                                : null,
                        };
                    }
                    if (Object.keys(changes).length > 0) {
                        this.logEvent('entitlementUpdate', {
                            id: newEntitlement.id,
                            changes,
                            shardId: newEntitlement.guildId
                                ? this.client.guilds.cache.get(newEntitlement.guildId)
                                      ?.shardId
                                : null,
                        });
                    }
                } catch (err) {
                    this._handleError(err, 'entitlementUpdate');
                }
            },
        );

        this.client.on('entitlementDelete', (entitlement: any) => {
            try {
                this.logEvent('entitlementDelete', {
                    id: entitlement.id,
                    skuId: entitlement.skuId,
                    shardId: entitlement.guildId
                        ? this.client.guilds.cache.get(entitlement.guildId)?.shardId
                        : null,
                });
            } catch (err) {
                this._handleError(err, 'entitlementDelete');
            }
        });
    }

    private _setupGuildFeatureHandlers() {
        this.client.on('guildFeaturesUpdate', async (guild: any, features: any) => {
            try {
                this.logEvent('guildFeaturesUpdate', {
                    guildId: guild.id,
                    features,
                    shardId: guild.shardId,
                });
            } catch (e) {
                this._handleError(e, 'guildFeaturesUpdate');
            }
        });
    }

    private _setupModerationHandlers() {
        this.client.on('guildModerationRuleCreate', async (rule: any) => {
            try {
                const { executor } = await this._fetchAuditDetails(
                    rule.guild,
                    (AuditLogEvent as any).ModerationRuleCreate,
                );
                this.logEvent('guildModerationRuleCreate', {
                    ruleId: rule.id,
                    guildId: rule.guild.id,
                    name: rule.name,
                    creatorId: rule.creatorId,
                    triggerType: rule.triggerType,
                    triggerMetadata: rule.triggerMetadata,
                    actions: rule.actions,
                    enabled: rule.enabled,
                    exemptRoles: rule.exemptRoles,
                    exemptChannels: rule.exemptChannels,
                    chatInputCommandIds: rule.chatInputCommandIds,
                    mentionIds: rule.mentionIds,
                    executor,
                    shardId: rule.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'guildModerationRuleCreate');
            }
        });

        this.client.on(
            'guildModerationRuleUpdate',
            async (oldRule: any, newRule: any) => {
                try {
                    const changes = this._detectModerationRuleChanges(oldRule, newRule);
                    if (Object.keys(changes).length === 0) return;
                    const { executor } = await this._fetchAuditDetails(
                        newRule.guild,
                        (AuditLogEvent as any).ModerationRuleUpdate,
                    );
                    this.logEvent('guildModerationRuleUpdate', {
                        ruleId: newRule.id,
                        guildId: newRule.guild.id,
                        changes,
                        executor,
                        shardId: newRule.guild.shardId,
                    });
                } catch (err) {
                    this._handleError(err, 'guildModerationRuleUpdate');
                }
            },
        );

        this.client.on('guildModerationRuleDelete', async (rule: any) => {
            try {
                const { executor } = await this._fetchAuditDetails(
                    rule.guild,
                    (AuditLogEvent as any).ModerationRuleDelete,
                );
                this.logEvent('guildModerationRuleDelete', {
                    ruleId: rule.id,
                    guildId: rule.guild.id,
                    name: rule.name,
                    executor,
                    shardId: rule.guild.shardId,
                });
            } catch (err) {
                this._handleError(err, 'guildModerationRuleDelete');
            }
        });

        this.client.on('moderationRuleUpdate', (oldRule: any, newRule: any) => {
            try {
                const changes = this._detectModerationRuleChanges(oldRule, newRule);
                if (Object.keys(changes).length > 0) {
                    this.logEvent('moderationRuleUpdate', {
                        ruleId: newRule.id,
                        changes,
                        shardId: newRule.guildId
                            ? this.client.guilds.cache.get(newRule.guildId)?.shardId
                            : null,
                    });
                }
            } catch (err) {
                this._handleError(err, 'moderationRuleUpdate');
            }
        });
    }

    private _detectModerationRuleChanges(
        oldRule: any,
        newRule: any,
    ): Record<string, any> {
        const changes: Record<string, any> = {};
        const fields = ['name', 'enabled'];
        fields.forEach((field) => {
            if (oldRule[field] !== newRule[field])
                changes[field] = {
                    old: oldRule[field],
                    new: newRule[field],
                };
        });
        if (JSON.stringify(oldRule.actions) !== JSON.stringify(newRule.actions))
            changes.actions = {
                old: oldRule.actions,
                new: newRule.actions,
            };
        return changes;
    }

    private _setupPremiumHandlers() {
        this.client.on(
            'guildPremiumSubscriptionLevelUpdate',
            async (oldLevel: number, newLevel: number, guild: any) => {
                try {
                    const { executor } = await this._fetchAuditDetails(
                        guild,
                        AuditLogEvent.GuildUpdate as any,
                    );
                    this.logEvent('guildPremiumSubscriptionLevelUpdate', {
                        guildId: guild.id,
                        oldLevel,
                        newLevel,
                        executor,
                        shardId: guild.shardId,
                    });
                } catch (err) {
                    this._handleError(err, 'guildPremiumSubscriptionLevelUpdate');
                }
            },
        );
    }
}

let loggerInstance: SystemLogger | null = null;

export function initSystemLogger(client: Client, config: SystemLoggerConfig = {}) {
    if (loggerInstance) return loggerInstance;
    loggerInstance = new SystemLogger(config);
    loggerInstance.registerHandlers(client);
    return loggerInstance;
}
