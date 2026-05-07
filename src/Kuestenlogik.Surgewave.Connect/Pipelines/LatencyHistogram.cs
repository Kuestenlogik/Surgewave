namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Simple percentile tracking via a sorted ring buffer (last 10,000 values).
/// Thread-safe via lock.
/// </summary>
public sealed class LatencyHistogram
{
    private const int MaxSamples = 10_000;
    private readonly double[] _samples = new double[MaxSamples];
    private int _count;
    private int _index;
    private readonly object _lock = new();

    public void Record(double latencyMs)
    {
        lock (_lock)
        {
            _samples[_index] = latencyMs;
            _index = (_index + 1) % MaxSamples;
            if (_count < MaxSamples)
                _count++;
        }
    }

    public double GetPercentile(double p)
    {
        lock (_lock)
        {
            if (_count == 0)
                return 0;

            var sorted = new double[_count];
            Array.Copy(_samples, sorted, _count);
            Array.Sort(sorted);

            var rank = (p / 100.0) * (_count - 1);
            var lower = (int)Math.Floor(rank);
            var upper = (int)Math.Ceiling(rank);

            if (lower == upper || upper >= _count)
                return sorted[lower];

            var fraction = rank - lower;
            return sorted[lower] + (sorted[upper] - sorted[lower]) * fraction;
        }
    }

    public double Average
    {
        get
        {
            lock (_lock)
            {
                if (_count == 0)
                    return 0;

                double sum = 0;
                for (var i = 0; i < _count; i++)
                    sum += _samples[i];
                return sum / _count;
            }
        }
    }

    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _count;
            }
        }
    }

    public void Reset()
    {
        lock (_lock)
        {
            _count = 0;
            _index = 0;
        }
    }
}
