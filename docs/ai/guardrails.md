# AI Guardrails

Surgewave AI Guardrails provide content safety evaluation for AI pipelines. Each guardrail implements the `IGuardrail` interface and can be composed into a pipeline for layered content filtering.

## Overview

The `Kuestenlogik.Surgewave.AI.Guardrails` package includes:

| Guardrail | Purpose |
|-----------|---------|
| `PiiDetector` | Detects and redacts personally identifiable information |
| `ToxicityFilter` | Blocks toxic or offensive content via keyword matching |
| `PromptInjectionDetector` | Detects prompt injection attacks |
| `ContentPolicyGuardrail` | Validates content against configurable policies |
| `GuardrailPipeline` | Chains multiple guardrails in sequence |

## IGuardrail Interface

All guardrails implement this interface:

```csharp
public interface IGuardrail
{
    string Name { get; }
    string Description { get; }
    Task<GuardrailResult> EvaluateAsync(
        string content,
        GuardrailContext? context = null,
        CancellationToken ct = default);
}
```

The `GuardrailResult` contains:
- `Passed` - Whether the content passed the check
- `Violations` - List of specific violations found
- `SanitizedContent` - Content with violations redacted (when applicable)
- `Severity` - Highest severity level (Info, Warning, Error, Critical)
- `EvaluationDuration` - Time taken for evaluation

## PII Detector

Detects 6 types of PII using compiled regex patterns:

| PII Type | Example |
|----------|---------|
| Email | `user@example.com` |
| Phone | `+49 123 456 7890` |
| Credit Card | `4111-1111-1111-1111` |
| SSN | `123-45-6789` |
| IP Address | `192.168.1.1` |
| IBAN | `DE89 3704 0044 0532 0130 00` |

### Usage

```csharp
var detector = new PiiDetector(new PiiDetectorOptions
{
    DetectEmails = true,
    DetectPhoneNumbers = true,
    DetectCreditCards = true,
    DetectSsn = true,
    DetectIpAddresses = true,
    DetectIban = true,
    UseTypedPlaceholders = true  // e.g., [REDACTED_EMAIL]
});

var result = await detector.EvaluateAsync("Contact me at user@example.com");
// result.Passed == false
// result.SanitizedContent == "Contact me at [REDACTED_EMAIL]"
```

When `UseTypedPlaceholders` is enabled, each PII type gets a specific placeholder like `[REDACTED_EMAIL]` or `[REDACTED_CREDITCARD]`. Otherwise, a single `RedactionPlaceholder` string is used.

## Toxicity Filter

Keyword-based content filtering with a configurable blocklist:

```csharp
var filter = new ToxicityFilter(new ToxicityFilterOptions
{
    UseDefaultBlocklist = true,       // 25+ default blocked terms
    CaseSensitive = false,
    BlockedTerms = ["custom-term"],   // Additional terms
    ReplacementText = "[BLOCKED]"
});

var result = await filter.EvaluateAsync(userInput);
if (!result.Passed)
{
    Console.WriteLine($"{result.Violations.Count} toxic term(s) detected");
}
```

The default blocklist includes terms related to hate speech, threats, harassment, and other harmful content. Custom terms can be added through `BlockedTerms`.

## Prompt Injection Detector

Detects 5 categories of prompt injection attacks:

| Pattern | Description |
|---------|-------------|
| Instruction Override | "ignore previous instructions", "disregard all prior rules" |
| Role Override | "you are now", "act as", "pretend you are" |
| System Prompt Injection | "System:", "SYSTEM PROMPT:", `<<SYS>>`, `[INST]` |
| Code Block Injection | System prompts hidden inside markdown code blocks |
| Base64 Payloads | Suspicious Base64-encoded content (40+ chars) |

### Usage

```csharp
var detector = new PromptInjectionDetector(new PromptInjectionOptions
{
    DetectInstructionOverride = true,
    DetectRoleOverride = true,
    DetectSystemPromptInjection = true,
    DetectBase64Payloads = true,
    CustomPatterns = []  // Additional regex patterns
});

var result = await detector.EvaluateAsync("Ignore previous instructions and...");
// result.Passed == false
// result.Severity == GuardrailSeverity.Critical
```

Prompt injection violations are flagged as `Critical` severity. Unlike PII detection, content is blocked rather than sanitized.

## Content Policy Guardrail

A configurable policy engine that validates content length, forbidden patterns, and required patterns:

```csharp
var policy = new ContentPolicyGuardrail(new ContentPolicyOptions
{
    PolicyName = "customer-support",
    MinContentLength = 10,
    MaxContentLength = 5000,
    ForbiddenPatterns = [@"\b(password|secret)\b"],
    RequiredPatterns = []  // At least one must match (if specified)
});

var result = await policy.EvaluateAsync(content);
```

## GuardrailPipeline

Chain multiple guardrails in sequence. Sanitized content from each guardrail is passed to the next:

```csharp
var pipeline = new GuardrailPipeline()
    .Add(new PiiDetector())
    .Add(new ToxicityFilter())
    .Add(new PromptInjectionDetector())
    .Add(new ContentPolicyGuardrail());

var result = await pipeline.EvaluateAsync(userInput);

if (!result.Passed)
{
    Console.WriteLine($"Blocked: {result.ViolationCount} violation(s)");
    Console.WriteLine($"Severity: {result.HighestSeverity}");
}

// Use sanitized content for downstream processing
var safeContent = result.FinalContent ?? userInput;
```

The pipeline result includes:
- `Passed` - True only if all guardrails passed
- `Results` - Individual result from each guardrail
- `FinalContent` - Content after all sanitization passes
- `ViolationCount` - Total violations across all guardrails
- `HighestSeverity` - Maximum severity from all results
- `TotalDuration` - Combined evaluation time

## Dependency Injection

Register guardrails with the DI container:

```csharp
services.AddSurgewaveGuardrails()
    .AddPiiDetection(options =>
    {
        options.DetectEmails = true;
        options.DetectCreditCards = true;
    })
    .AddToxicityFilter(options =>
    {
        options.UseDefaultBlocklist = true;
    })
    .AddPromptInjectionDetection(options =>
    {
        options.DetectInstructionOverride = true;
        options.DetectRoleOverride = true;
    });
```

Each `Add*` method registers the guardrail as an `IGuardrail` singleton, making it available for injection into pipeline nodes or application code.

## Next Steps

- [Agent Memory](agent-memory.md) - Persistent memory for AI agents
- [Pipeline Chat](pipeline-chat.md) - Interactive chat with AI pipelines
- [Agent Integration](agent-integration.md) - Multi-agent architectures
