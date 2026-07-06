using System.Net.Http.Json;
using System.Text.Json;
using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client for the broker's transaction inspection APIs.
/// </summary>
public sealed class TransactionsApiClient : ITransactionsApiClient
{
    private readonly HttpClient _httpClient;
    private readonly JsonSerializerOptions _jsonOptions;

    public TransactionsApiClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
        _jsonOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
    }

    public async Task<IReadOnlyList<TxnListingModel>> ListKafkaTransactionsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<TxnListModel>(
                "/v3/transactions", _jsonOptions, cancellationToken);
            return response?.Transactions ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }

    public async Task<IReadOnlyList<TxnDescriptionModel>> DescribeKafkaTransactionsAsync(
        IReadOnlyList<string> transactionalIds, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.PostAsJsonAsync(
                "/v3/transactions/describe", new TxnDescribeRequest(transactionalIds), _jsonOptions, cancellationToken);
            if (!response.IsSuccessStatusCode)
                return [];

            var body = await response.Content.ReadFromJsonAsync<TxnDescribeModel>(_jsonOptions, cancellationToken);
            return body?.Transactions ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }

    public async Task<IReadOnlyList<CrossTopicTransactionModel>> ListCrossTopicTransactionsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var transactions = await _httpClient.GetFromJsonAsync<List<CrossTopicTransactionModel>>(
                "/api/transactions/", _jsonOptions, cancellationToken);
            return transactions ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }
}
