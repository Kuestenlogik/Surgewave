using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Schema.Registry.Evolution;

/// <summary>
/// Background service that monitors the Schema Registry for new schema versions,
/// analyzes changes, generates impact reports and migration code, and optionally
/// notifies the Assistant.
/// </summary>
public sealed class SchemaEvolutionService : BackgroundService
{
    private readonly SchemaEvolutionConfig _config;
    private readonly ISchemaStore _store;
    private readonly SchemaEvolutionAnalyzer _analyzer;
    private readonly SchemaMigrationCodeGenerator _codeGen;
    private readonly SchemaEvolutionLlmEnhancer _llmEnhancer;
    private readonly ILogger<SchemaEvolutionService> _logger;

    /// <summary>
    /// Tracks the last known version per subject so we can detect new versions.
    /// </summary>
    private readonly ConcurrentDictionary<string, int> _lastKnownVersions = new();

    /// <summary>
    /// Detected schema changes, keyed by "{subject}/{newVersion}".
    /// </summary>
    private readonly ConcurrentDictionary<string, SchemaChange> _detectedChanges = new();

    /// <summary>
    /// Generated impact reports, keyed by "{subject}/{newVersion}".
    /// </summary>
    private readonly ConcurrentDictionary<string, SchemaImpactReport> _reports = new();

    /// <summary>
    /// Event raised when a schema change is detected — used by Assistant integration.
    /// </summary>
    public event Action<SchemaChange, SchemaImpactReport>? OnSchemaChangeDetected;

    public SchemaEvolutionService(
        SchemaEvolutionConfig config,
        ISchemaStore store,
        SchemaEvolutionAnalyzer analyzer,
        SchemaMigrationCodeGenerator codeGen,
        SchemaEvolutionLlmEnhancer llmEnhancer,
        ILogger<SchemaEvolutionService> logger)
    {
        _config = config;
        _store = store;
        _analyzer = analyzer;
        _codeGen = codeGen;
        _llmEnhancer = llmEnhancer;
        _logger = logger;
    }

    /// <summary>
    /// Get all detected schema changes.
    /// </summary>
    public IReadOnlyList<SchemaChange> GetAllChanges()
    {
        return _detectedChanges.Values.OrderByDescending(c => c.DetectedAt).ToList();
    }

    /// <summary>
    /// Get schema changes for a specific subject.
    /// </summary>
    public IReadOnlyList<SchemaChange> GetChangesForSubject(string subject)
    {
        return _detectedChanges.Values
            .Where(c => string.Equals(c.SubjectName, subject, StringComparison.Ordinal))
            .OrderByDescending(c => c.DetectedAt)
            .ToList();
    }

    /// <summary>
    /// Get the impact report for a specific subject and version.
    /// </summary>
    public SchemaImpactReport? GetReport(string subject, int version)
    {
        var key = $"{subject}/{version}";
        return _reports.TryGetValue(key, out var report) ? report : null;
    }

    /// <summary>
    /// Get generated migration code for a specific subject and version.
    /// </summary>
    public string? GetMigrationCode(string subject, int version)
    {
        var key = $"{subject}/{version}";
        return _reports.TryGetValue(key, out var report) ? report.GeneratedCode : null;
    }

    /// <summary>
    /// Manually analyze two schema JSON strings and return the change analysis.
    /// </summary>
    public SchemaEvolutionAnalyzeResult AnalyzeManually(string oldSchemaJson, string newSchemaJson, string subject = "manual")
    {
        var change = _analyzer.AnalyzeChanges(oldSchemaJson, newSchemaJson, subject, 0, 1);
        var report = _analyzer.GenerateImpactReport(change);

        return new SchemaEvolutionAnalyzeResult
        {
            Change = change,
            Report = report
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_config.Enabled)
        {
            _logger.LogInformation("Schema evolution monitoring is disabled");
            return;
        }

        _logger.LogInformation(
            "Schema evolution monitoring started (interval={Interval}s, autoCode={AutoCode}, notifyAssistant={NotifyAssistant})",
            _config.CheckIntervalSeconds, _config.AutoGenerateCode, _config.NotifyAssistant);

        // Initial delay to let the broker and schema registry start up
        await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken);

        // Seed the last-known versions
        SeedKnownVersions();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CheckForNewVersionsAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Schema evolution check cycle failed");
            }

            await Task.Delay(TimeSpan.FromSeconds(_config.CheckIntervalSeconds), stoppingToken);
        }

        _logger.LogInformation("Schema evolution monitoring stopped");
    }

    private void SeedKnownVersions()
    {
        foreach (var subject in _store.GetSubjects())
        {
            var versions = _store.GetVersions(subject);
            if (versions.Count > 0)
            {
                _lastKnownVersions[subject] = versions[^1];
            }
        }

        _logger.LogDebug("Seeded {Count} subjects for evolution monitoring", _lastKnownVersions.Count);
    }

    private async Task CheckForNewVersionsAsync(CancellationToken ct)
    {
        foreach (var subject in _store.GetSubjects())
        {
            if (ct.IsCancellationRequested)
            {
                break;
            }

            var versions = _store.GetVersions(subject);
            if (versions.Count < 2)
            {
                // Need at least 2 versions to detect evolution
                if (versions.Count == 1)
                {
                    _lastKnownVersions[subject] = versions[0];
                }
                continue;
            }

            var latestVersion = versions[^1];
            var hasLastKnown = _lastKnownVersions.TryGetValue(subject, out var lastKnown);

            if (hasLastKnown && latestVersion <= lastKnown)
            {
                continue; // No new version
            }

            // New version detected — analyze the change
            var previousVersion = hasLastKnown ? lastKnown : versions[^2];
            _lastKnownVersions[subject] = latestVersion;

            var oldSchema = _store.GetSchema(subject, previousVersion);
            var newSchema = _store.GetSchema(subject, latestVersion);

            if (oldSchema is null || newSchema is null)
            {
                continue;
            }

            _logger.LogInformation(
                "Detected schema evolution for '{Subject}': v{Old} -> v{New}",
                subject, previousVersion, latestVersion);

            try
            {
                var change = _analyzer.AnalyzeChanges(
                    oldSchema.SchemaString, newSchema.SchemaString,
                    subject, previousVersion, latestVersion);

                var key = $"{subject}/{latestVersion}";
                _detectedChanges[key] = change;

                if (_config.AutoGenerateCode)
                {
                    var report = _analyzer.GenerateImpactReport(change);

                    // Try LLM enhancement
                    try
                    {
                        var explanation = await _llmEnhancer.ExplainChangeAsync(change, ct);
                        report = report with { LlmExplanation = explanation };
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex, "LLM enhancement failed for {Subject} v{Version}, using rule-based analysis",
                            subject, latestVersion);
                    }

                    _reports[key] = report;

                    _logger.LogInformation(
                        "Generated impact report for '{Subject}' v{Version}: {Breaking}, {StepCount} migration steps",
                        subject, latestVersion, change.Breaking, report.MigrationSteps.Count);

                    // Notify listeners (Assistant integration)
                    if (_config.NotifyAssistant)
                    {
                        OnSchemaChangeDetected?.Invoke(change, report);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to analyze schema evolution for '{Subject}' v{Version}",
                    subject, latestVersion);
            }
        }
    }
}

/// <summary>
/// Result of a manual schema evolution analysis.
/// </summary>
public sealed class SchemaEvolutionAnalyzeResult
{
    /// <summary>The detected schema change.</summary>
    public required SchemaChange Change { get; init; }

    /// <summary>The generated impact report.</summary>
    public required SchemaImpactReport Report { get; init; }
}
