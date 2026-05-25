using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Tests for ListPool and PooledList - high-performance list pooling utilities.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ListPoolTests
{
    #region ListPool<T>.Rent Tests

    [Fact]
    public void Rent_ReturnsNonNullList()
    {
        var list = ListPool<int>.Rent();
        Assert.NotNull(list);
        ListPool<int>.Return(list);
    }

    [Fact]
    public void Rent_WithCapacity_ReturnsListWithAtLeastCapacity()
    {
        var list = ListPool<int>.Rent(100);
        Assert.True(list.Capacity >= 100);
        ListPool<int>.Return(list);
    }

    [Fact]
    public void Rent_ReturnsEmptyList()
    {
        var list = ListPool<int>.Rent();
        Assert.Empty(list);
        ListPool<int>.Return(list);
    }

    #endregion

    #region ListPool<T>.Return Tests

    [Fact]
    public void Return_ClearsList()
    {
        var list = ListPool<string>.Rent();
        list.Add("a");
        list.Add("b");
        ListPool<string>.Return(list);

        // After return, rent again - should be empty
        var list2 = ListPool<string>.Rent();
        Assert.Empty(list2);
        ListPool<string>.Return(list2);
    }

    [Fact]
    public void Return_NullList_DoesNotThrow()
    {
        ListPool<int>.Return(null!);
    }

    [Fact]
    public void RentAndReturn_MultipleTimes_Works()
    {
        for (int i = 0; i < 100; i++)
        {
            var list = ListPool<int>.Rent(32);
            list.Add(i);
            ListPool<int>.Return(list);
        }
    }

    #endregion

    #region PooledList<T> Tests

    [Fact]
    public void PooledList_Rent_ReturnsUsableList()
    {
        using var pooled = PooledList<int>.Rent();
        pooled.List.Add(1);
        pooled.List.Add(2);
        pooled.List.Add(3);

        Assert.Equal(3, pooled.List.Count);
    }

    [Fact]
    public void PooledList_ImplicitConversion_ToList()
    {
        using var pooled = PooledList<string>.Rent();
        pooled.List.Add("hello");

        List<string> list = pooled;
        Assert.Single(list);
        Assert.Equal("hello", list[0]);
    }

    [Fact]
    public void PooledList_Dispose_ReturnsToPool()
    {
        List<int> capturedList;

        using (var pooled = PooledList<int>.Rent())
        {
            pooled.List.Add(42);
            capturedList = pooled.List;
        }

        // The list should have been cleared by pool return
        Assert.Empty(capturedList);
    }

    [Fact]
    public void PooledList_WithCapacity_RespectsCapacity()
    {
        using var pooled = PooledList<double>.Rent(256);
        Assert.True(pooled.List.Capacity >= 256);
    }

    #endregion

    #region Concurrent Access Tests

    [Fact]
    public void ConcurrentRentReturn_DoesNotThrow()
    {
        var exceptions = new List<Exception>();

        Parallel.For(0, 200, i =>
        {
            try
            {
                var list = ListPool<int>.Rent(16);
                list.Add(i);
                list.Add(i * 2);
                ListPool<int>.Return(list);
            }
            catch (Exception ex)
            {
                lock (exceptions) exceptions.Add(ex);
            }
        });

        Assert.Empty(exceptions);
    }

    #endregion
}
