using System.Text.Json;
using FivetranClient.Models;

namespace FivetranClient.Fetchers;

public sealed class NonPaginatedFetcher(HttpRequestHandler requestHandler) : BaseFetcher(requestHandler)
{
    public async Task<T?> FetchAsync<T>(string endpoint, CancellationToken cancellationToken)
    {
        var response = await RequestHandler.GetAsync(endpoint, cancellationToken);
        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsStringAsync(cancellationToken);
        try
        {
            var root = JsonSerializer.Deserialize<NonPaginatedRoot<T>>(content, SerializerOptions);
            return root is null ? default : root.Data;
        }
        catch (JsonException ex)
        {
            var preview = content.Length > 200 ? string.Concat(content.AsSpan(0, 200), "...") : content;

            throw new InvalidOperationException(
                $"Failed to deserialize response. Endpoint='{endpoint}', Target='NonPaginatedRoot<{typeof(T).Name}>', PayloadPreview=\"{preview}\"",
                ex);
        }
    }
}