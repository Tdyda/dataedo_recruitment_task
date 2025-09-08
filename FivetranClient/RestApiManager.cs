using System.Net;
using FivetranClient.Fetchers;
using FivetranClient.Infrastructure;
using FivetranClient.Models;

namespace FivetranClient;

public class RestApiManager(HttpRequestHandler requestHandler) : IDisposable
{
    public static readonly Uri ApiBaseUrl = new("https://api.fivetran.com/v1/");

    // Indicates whether this instance owns the HttpClient and should dispose it.
    private readonly HttpClient? _createdClient;
    private readonly NonPaginatedFetcher _nonPaginatedFetcher = new(requestHandler);
    private readonly PaginatedFetcher _paginatedFetcher = new(requestHandler);

    public RestApiManager(string apiKey, string apiSecret, FivetranClientOptions? options = null)
        : this(ApiBaseUrl, apiKey, apiSecret, options)
    {
    }

    public RestApiManager(Uri baseUrl, string apiKey, string apiSecret, FivetranClientOptions? options = null)
        : this(new FivetranHttpClient(baseUrl, apiKey, apiSecret, options ?? new FivetranClientOptions()), true)
    {
    }

    private RestApiManager(HttpClient client, bool _) : this(new HttpRequestHandler(client))
    {
        _createdClient = client;
    }

    public RestApiManager(HttpClient client) : this(new HttpRequestHandler(client))
    {
    }

    public void Dispose()
    {
        _createdClient?.Dispose();
        GC.SuppressFinalize(this);
    }

    public IAsyncEnumerable<Group> GetGroupsAsync(CancellationToken cancellationToken)
    {
        var endpointPath = "groups";
        return _paginatedFetcher.FetchItemsAsync<Group>(endpointPath, cancellationToken);
    }

    public IAsyncEnumerable<Connector> GetConnectorsAsync(string groupId, CancellationToken cancellationToken)
    {
        var endpointPath = $"groups/{WebUtility.UrlEncode(groupId)}/connectors";
        return _paginatedFetcher.FetchItemsAsync<Connector>(endpointPath, cancellationToken);
    }

    public async Task<DataSchemas?> GetConnectorSchemasAsync(
        string connectorId,
        CancellationToken cancellationToken)
    {
        var endpointPath = $"connectors/{WebUtility.UrlEncode(connectorId)}/schemas";
        return await _nonPaginatedFetcher.FetchAsync<DataSchemas>(endpointPath, cancellationToken);
    }
}