using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Tests for the RetryHelper exponential backoff helper.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class RetryHelperTests
{
    [Fact]
    public void CalculateBackoffDelay_FirstAttempt_ReturnsInitialDelay()
    {
        // Act
        var delay = RetryHelper.CalculateBackoffDelay(1, initialDelayMs: 100);

        // Assert
        Assert.Equal(100, delay);
    }

    [Fact]
    public void CalculateBackoffDelay_SecondAttempt_DoublesDelay()
    {
        // Act
        var delay = RetryHelper.CalculateBackoffDelay(2, initialDelayMs: 100);

        // Assert
        Assert.Equal(200, delay);
    }

    [Fact]
    public void CalculateBackoffDelay_ThirdAttempt_QuadruplesDelay()
    {
        // Act
        var delay = RetryHelper.CalculateBackoffDelay(3, initialDelayMs: 100);

        // Assert
        Assert.Equal(400, delay);
    }

    [Fact]
    public void CalculateBackoffDelay_RespectsMaxDelay()
    {
        // Act
        var delay = RetryHelper.CalculateBackoffDelay(10, initialDelayMs: 1000, maxDelayMs: 5000);

        // Assert
        Assert.Equal(5000, delay);
    }

    [Fact]
    public void CalculateBackoffDelay_WithDefaultValues()
    {
        // Act - use default parameters
        var delay = RetryHelper.CalculateBackoffDelay(1);

        // Assert
        Assert.Equal(RetryHelper.DefaultInitialDelayMs, delay);
    }

    [Fact]
    public void CalculateBackoffDelay_PreventOverflow_WithLargeAttempt()
    {
        // Act - large attempt number should not overflow
        var delay = RetryHelper.CalculateBackoffDelay(20, initialDelayMs: 100, maxDelayMs: 30000);

        // Assert - should be capped at max
        Assert.Equal(30000, delay);
    }

    [Fact]
    public async Task ExecuteWithBackoffAsync_ReturnsResult_OnSuccess()
    {
        // Arrange
        var attempts = 0;

        // Act
        var result = await RetryHelper.ExecuteWithBackoffAsync(
            async () =>
            {
                attempts++;
                await Task.CompletedTask;
                return 42;
            });

        // Assert
        Assert.Equal(42, result);
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task ExecuteWithBackoffAsync_RetriesOnFailure()
    {
        // Arrange
        var attempts = 0;

        // Act
        var result = await RetryHelper.ExecuteWithBackoffAsync(
            async () =>
            {
                attempts++;
                if (attempts < 3)
                    throw new InvalidOperationException("Transient error");
                await Task.CompletedTask;
                return "success";
            },
            maxRetries: 5,
            initialDelayMs: 10);

        // Assert
        Assert.Equal("success", result);
        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task ExecuteWithBackoffAsync_ThrowsAfterMaxRetries()
    {
        // Arrange
        var attempts = 0;

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await RetryHelper.ExecuteWithBackoffAsync<int>(
                async () =>
                {
                    attempts++;
                    await Task.CompletedTask;
                    throw new InvalidOperationException("Persistent error");
                },
                maxRetries: 3,
                initialDelayMs: 10);
        });

        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task ExecuteWithBackoffAsync_RethrowsCancellation()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await RetryHelper.ExecuteWithBackoffAsync<int>(
                async () =>
                {
                    await Task.CompletedTask;
                    throw new OperationCanceledException();
                },
                ct: cts.Token);
        });
    }

    [Fact]
    public async Task TryExecuteWithBackoffAsync_ReturnsTrue_OnSuccess()
    {
        // Arrange
        string? capturedResult = null;

        // Act
        var success = await RetryHelper.TryExecuteWithBackoffAsync(
            async () =>
            {
                await Task.CompletedTask;
                return "result";
            },
            result => capturedResult = result);

        // Assert
        Assert.True(success);
        Assert.Equal("result", capturedResult);
    }

    [Fact]
    public async Task TryExecuteWithBackoffAsync_ReturnsFalse_AfterMaxRetries()
    {
        // Arrange
        var attempts = 0;
        var retryAttempts = new List<int>();

        // Act
        var success = await RetryHelper.TryExecuteWithBackoffAsync<int>(
            async () =>
            {
                attempts++;
                await Task.CompletedTask;
                throw new InvalidOperationException("Error");
            },
            _ => { },
            (attempt, _) => retryAttempts.Add(attempt),
            maxRetries: 3,
            initialDelayMs: 10);

        // Assert
        Assert.False(success);
        Assert.Equal(3, attempts);
        Assert.Equal(2, retryAttempts.Count); // onRetry called for attempts 1 and 2 (not 3)
        Assert.Equal(1, retryAttempts[0]);
        Assert.Equal(2, retryAttempts[1]);
    }

    [Fact]
    public async Task TryExecuteWithBackoffAsync_CallsOnRetry_BeforeEachRetry()
    {
        // Arrange
        var retryExceptions = new List<Exception>();
        var attempts = 0;

        // Act
        await RetryHelper.TryExecuteWithBackoffAsync(
            async () =>
            {
                attempts++;
                if (attempts < 3)
                    throw new InvalidOperationException($"Attempt {attempts}");
                await Task.CompletedTask;
                return 42;
            },
            _ => { },
            (attempt, ex) => retryExceptions.Add(ex),
            maxRetries: 5,
            initialDelayMs: 10);

        // Assert
        Assert.Equal(2, retryExceptions.Count);
        Assert.Equal("Attempt 1", retryExceptions[0].Message);
        Assert.Equal("Attempt 2", retryExceptions[1].Message);
    }

    [Fact]
    public async Task TryExecuteWithBackoffAsync_ReturnsFalse_OnCancellation()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act
        var success = await RetryHelper.TryExecuteWithBackoffAsync<int>(
            async () =>
            {
                await Task.CompletedTask;
                throw new OperationCanceledException();
            },
            _ => { },
            ct: cts.Token);

        // Assert
        Assert.False(success);
    }

    [Fact]
    public void DefaultValues_AreCorrect()
    {
        // Assert
        Assert.Equal(50, RetryHelper.DefaultInitialDelayMs);
        Assert.Equal(3, RetryHelper.DefaultMaxRetries);
    }
}
