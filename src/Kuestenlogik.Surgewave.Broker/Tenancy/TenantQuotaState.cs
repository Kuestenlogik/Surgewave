namespace Kuestenlogik.Surgewave.Broker.Tenancy;

internal sealed class TenantQuotaState
{
    public long ProduceTokens { get; set; }
    public long FetchTokens { get; set; }
    public long ProduceBytesTotal { get; set; }
    public long FetchBytesTotal { get; set; }
    public long LastRefillTicks { get; set; } = Environment.TickCount64;

    public void Refill(long produceRate, long fetchRate)
    {
        var now = Environment.TickCount64;
        var elapsedMs = now - LastRefillTicks;
        if (elapsedMs <= 0) return;

        if (produceRate > 0)
            ProduceTokens = Math.Min(produceRate, ProduceTokens + produceRate * elapsedMs / 1000);
        if (fetchRate > 0)
            FetchTokens = Math.Min(fetchRate, FetchTokens + fetchRate * elapsedMs / 1000);

        LastRefillTicks = now;
    }
}
