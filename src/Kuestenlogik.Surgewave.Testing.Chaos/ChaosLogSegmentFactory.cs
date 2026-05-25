using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// Wraps an <see cref="ILogSegmentFactory"/> to return <see cref="ChaosLogSegment"/> instances
/// that inject faults controlled by a <see cref="ChaosEngine"/>.
/// </summary>
public sealed class ChaosLogSegmentFactory : ILogSegmentFactory
{
    private readonly ILogSegmentFactory _inner;
    private readonly ChaosEngine _engine;
    private readonly int _brokerId;
    private readonly ILogger _logger;

    /// <summary>
    /// Creates a new chaos log segment factory wrapping the inner factory.
    /// </summary>
    /// <param name="inner">The actual factory to delegate segment creation to.</param>
    /// <param name="engine">The chaos engine controlling fault injection.</param>
    /// <param name="brokerId">The broker ID segments will belong to.</param>
    /// <param name="loggerFactory">Optional logger factory for diagnostics.</param>
    public ChaosLogSegmentFactory(ILogSegmentFactory inner, ChaosEngine engine, int brokerId, ILoggerFactory? loggerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(engine);

        _inner = inner;
        _engine = engine;
        _brokerId = brokerId;
        _logger = (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<ChaosLogSegmentFactory>();
    }

    /// <inheritdoc />
    public ILogSegment CreateSegment(string baseDirectory, long baseOffset, bool createNew, long maxSegmentSize = ILogSegment.DefaultMaxSegmentSize)
    {
        var innerSegment = _inner.CreateSegment(baseDirectory, baseOffset, createNew, maxSegmentSize);
        return new ChaosLogSegment(innerSegment, _engine, _brokerId, _logger);
    }

    /// <inheritdoc />
    public bool IsPersistent => _inner.IsPersistent;
}
