using System.Diagnostics;

namespace Kuestenlogik.Surgewave.Client.Resilience;

/// <summary>
/// Configuration for the circuit breaker.
/// </summary>
public sealed class CircuitBreakerConfig
{
    /// <summary>
    /// Number of consecutive failures before opening the circuit.
    /// Default: 5
    /// </summary>
    public int FailureThreshold { get; set; } = 5;

    /// <summary>
    /// Time to wait before transitioning from Open to HalfOpen.
    /// Default: 30 seconds
    /// </summary>
    public TimeSpan OpenDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Number of successful requests in HalfOpen state before closing the circuit.
    /// Default: 1
    /// </summary>
    public int SuccessThresholdInHalfOpen { get; set; } = 1;

    /// <summary>
    /// Optional predicate to determine which exceptions should count as failures.
    /// If null, all exceptions count as failures.
    /// </summary>
    public Func<Exception, bool>? ShouldHandle { get; set; }

    /// <summary>
    /// Optional callback when state changes.
    /// </summary>
    public Action<CircuitBreakerState, CircuitBreakerState>? OnStateChange { get; set; }
}

/// <summary>
/// Exception thrown when the circuit breaker is open.
/// </summary>
public sealed class CircuitBreakerOpenException : Exception
{
    /// <summary>
    /// Time until the circuit breaker will attempt to transition to half-open.
    /// </summary>
    public TimeSpan RetryAfter { get; }

    public CircuitBreakerOpenException() : base("Circuit breaker is open.")
    {
        RetryAfter = TimeSpan.Zero;
    }

    public CircuitBreakerOpenException(string message) : base(message)
    {
        RetryAfter = TimeSpan.Zero;
    }

    public CircuitBreakerOpenException(string message, Exception innerException) : base(message, innerException)
    {
        RetryAfter = TimeSpan.Zero;
    }

    public CircuitBreakerOpenException(TimeSpan retryAfter)
        : base($"Circuit breaker is open. Retry after {retryAfter.TotalSeconds:F1} seconds.")
    {
        RetryAfter = retryAfter;
    }
}

/// <summary>
/// Circuit breaker pattern implementation for fault tolerance.
///
/// States:
/// - Closed: Normal operation, requests flow through
/// - Open: Circuit tripped, requests are rejected immediately
/// - HalfOpen: Testing if the service has recovered
///
/// Transitions:
/// - Closed → Open: When failure threshold is reached
/// - Open → HalfOpen: After open duration elapses
/// - HalfOpen → Closed: When success threshold is reached
/// - HalfOpen → Open: When a failure occurs
/// </summary>
public sealed class CircuitBreaker
{
    private readonly CircuitBreakerConfig _config;
    private readonly object _lock = new();
    private CircuitBreakerState _state = CircuitBreakerState.Closed;
    private int _failureCount;
    private int _successCountInHalfOpen;
    private long _openedAtTicks;

    /// <summary>
    /// Current state of the circuit breaker.
    /// </summary>
    public CircuitBreakerState State
    {
        get
        {
            lock (_lock)
            {
                if (_state == CircuitBreakerState.Open)
                {
                    // Check if we should transition to half-open
                    var elapsed = TimeSpan.FromTicks(Stopwatch.GetTimestamp() - _openedAtTicks);
                    if (elapsed >= _config.OpenDuration)
                    {
                        TransitionTo(CircuitBreakerState.HalfOpen);
                    }
                }
                return _state;
            }
        }
    }

    /// <summary>
    /// Number of consecutive failures.
    /// </summary>
    public int FailureCount => _failureCount;

    /// <summary>
    /// Time remaining until the circuit transitions to half-open (when open).
    /// </summary>
    public TimeSpan? TimeUntilHalfOpen
    {
        get
        {
            lock (_lock)
            {
                if (_state != CircuitBreakerState.Open)
                    return null;

                var elapsed = TimeSpan.FromTicks(Stopwatch.GetTimestamp() - _openedAtTicks);
                var remaining = _config.OpenDuration - elapsed;
                return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
            }
        }
    }

    /// <summary>
    /// Creates a new circuit breaker with default configuration.
    /// </summary>
    public CircuitBreaker() : this(new CircuitBreakerConfig())
    {
    }

    /// <summary>
    /// Creates a new circuit breaker with the specified configuration.
    /// </summary>
    public CircuitBreaker(CircuitBreakerConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    /// <summary>
    /// Execute an operation through the circuit breaker.
    /// </summary>
    public async Task<TResult> ExecuteAsync<TResult>(
        Func<CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken = default)
    {
        // Check if circuit is open
        var currentState = State; // This triggers state transition check
        if (currentState == CircuitBreakerState.Open)
        {
            throw new CircuitBreakerOpenException(TimeUntilHalfOpen ?? TimeSpan.Zero);
        }

        try
        {
            var result = await operation(cancellationToken);
            RecordSuccess();
            return result;
        }
        catch (Exception ex) when (ShouldHandle(ex))
        {
            RecordFailure();
            throw;
        }
    }

    /// <summary>
    /// Execute a void operation through the circuit breaker.
    /// </summary>
    public async Task ExecuteAsync(
        Func<CancellationToken, Task> operation,
        CancellationToken cancellationToken = default)
    {
        await ExecuteAsync(async ct =>
        {
            await operation(ct);
            return true;
        }, cancellationToken);
    }

    /// <summary>
    /// Execute a synchronous operation through the circuit breaker.
    /// </summary>
    public TResult Execute<TResult>(Func<TResult> operation)
    {
        var currentState = State;
        if (currentState == CircuitBreakerState.Open)
        {
            throw new CircuitBreakerOpenException(TimeUntilHalfOpen ?? TimeSpan.Zero);
        }

        try
        {
            var result = operation();
            RecordSuccess();
            return result;
        }
        catch (Exception ex) when (ShouldHandle(ex))
        {
            RecordFailure();
            throw;
        }
    }

    /// <summary>
    /// Manually reset the circuit breaker to closed state.
    /// </summary>
    public void Reset()
    {
        lock (_lock)
        {
            _failureCount = 0;
            _successCountInHalfOpen = 0;
            TransitionTo(CircuitBreakerState.Closed);
        }
    }

    /// <summary>
    /// Manually trip the circuit breaker to open state.
    /// </summary>
    public void Trip()
    {
        lock (_lock)
        {
            TransitionTo(CircuitBreakerState.Open);
            _openedAtTicks = Stopwatch.GetTimestamp();
        }
    }

    private void RecordSuccess()
    {
        lock (_lock)
        {
            if (_state == CircuitBreakerState.HalfOpen)
            {
                _successCountInHalfOpen++;
                if (_successCountInHalfOpen >= _config.SuccessThresholdInHalfOpen)
                {
                    _failureCount = 0;
                    _successCountInHalfOpen = 0;
                    TransitionTo(CircuitBreakerState.Closed);
                }
            }
            else if (_state == CircuitBreakerState.Closed)
            {
                // Reset failure count on success in closed state
                _failureCount = 0;
            }
        }
    }

    private void RecordFailure()
    {
        lock (_lock)
        {
            _failureCount++;

            if (_state == CircuitBreakerState.HalfOpen)
            {
                // Any failure in half-open immediately opens the circuit
                _successCountInHalfOpen = 0;
                TransitionTo(CircuitBreakerState.Open);
                _openedAtTicks = Stopwatch.GetTimestamp();
            }
            else if (_state == CircuitBreakerState.Closed && _failureCount >= _config.FailureThreshold)
            {
                TransitionTo(CircuitBreakerState.Open);
                _openedAtTicks = Stopwatch.GetTimestamp();
            }
        }
    }

    private void TransitionTo(CircuitBreakerState newState)
    {
        if (_state != newState)
        {
            var oldState = _state;
            _state = newState;
            _config.OnStateChange?.Invoke(oldState, newState);
        }
    }

    private bool ShouldHandle(Exception ex)
    {
        if (_config.ShouldHandle == null)
            return true;

        return _config.ShouldHandle(ex);
    }
}
