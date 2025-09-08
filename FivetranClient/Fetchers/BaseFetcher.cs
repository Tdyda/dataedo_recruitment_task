using System.Text.Json;

namespace FivetranClient.Fetchers;

public abstract class BaseFetcher(HttpRequestHandler requestHandler)
{
    protected static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        PropertyNameCaseInsensitive = true
    };

    protected readonly HttpRequestHandler RequestHandler = requestHandler;
}