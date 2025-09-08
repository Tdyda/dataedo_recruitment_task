using System.Net;
using System.Text;
using FivetranClient.Infrastructure;
using Microsoft.Extensions.Logging;

namespace FivetranClient;

public class HttpRequestHandler
{
    private const int Max429Retries = 3;
    private static readonly TtlDictionary<string, string> PayloadCache = new();
    private readonly HttpClient _client;
    private readonly object _lock = new();
    private readonly ILogger<HttpRequestHandler>? _logger;
    private readonly FivetranClientOptions _options;
    private readonly SemaphoreSlim? _semaphore;
    private DateTime _retryAfterTime = DateTime.UtcNow;

    /// <summary>
    ///     Handles HttpTooManyRequests responses by limiting the number of concurrent requests and managing retry logic.
    ///     Also caches responses to avoid unnecessary network calls.
    /// </summary>
    /// <remarks>
    ///     Set <paramref name="maxConcurrentRequests" /> to 0 to disable concurrency limit.
    /// </remarks>
    public HttpRequestHandler(HttpClient client, FivetranClientOptions? options = null,
        ILogger<HttpRequestHandler>? logger = null)
    {
        _client = client;
        _options = options ?? new FivetranClientOptions();
        _logger = logger;

        if (_options.MaxConcurrentRequests > 0)
            _semaphore = new SemaphoreSlim(_options.MaxConcurrentRequests, _options.MaxConcurrentRequests);
    }


    public async Task<HttpResponseMessage> GetAsync(string url, CancellationToken cancellationToken)
    {
        for (var attempt = 0;; attempt++)
        {
            var response = await _GetOnceAsync(url, cancellationToken);

            if (response.StatusCode == HttpStatusCode.TooManyRequests)
            {
                if (attempt >= Max429Retries)
                {
                    response.Dispose();
                    throw new HttpRequestException(
                        $"Too Many Requests (429) for '{url}'. Retry limit ({Max429Retries}) exceeded.");
                }

                var retryAfter = response.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(60);
                _logger?.LogWarning("Received 429 for {Url}, attempt {Attempt}/{Max}. Retrying after {RetryAfter}s.",
                    url, attempt + 1, Max429Retries, retryAfter.TotalSeconds);

                lock (_lock)
                {
                    _retryAfterTime = DateTime.UtcNow.Add(retryAfter);
                }

                response.Dispose();

                await Task.Delay(retryAfter, cancellationToken);

                continue;
            }

            response.EnsureSuccessStatusCode();
            return response;
        }
    }


    private async Task<HttpResponseMessage> _GetOnceAsync(string url, CancellationToken cancellationToken)
    {
        if (_semaphore is not null) await _semaphore.WaitAsync(cancellationToken);

        try
        {
            if (PayloadCache.TryGetValue(url, out var cachedPayload)) return CreateOkJsonResponse(cachedPayload);

            TimeSpan timeToWait;
            lock (_lock)
            {
                timeToWait = _retryAfterTime - DateTime.UtcNow;
            }

            if (timeToWait > TimeSpan.Zero) await Task.Delay(timeToWait, cancellationToken);

            cancellationToken.ThrowIfCancellationRequested();

            _logger?.LogDebug("Starting GET request to {Url}", url);
            var response = await _client.GetAsync(new Uri(url, UriKind.Relative), cancellationToken);

            if (response.StatusCode == HttpStatusCode.TooManyRequests) return response;

            var payload = await response.Content.ReadAsStringAsync(cancellationToken);
            if (response.IsSuccessStatusCode)
                PayloadCache.GetOrAdd(url, () => payload, _options.CacheTtl ?? TimeSpan.FromSeconds(30));

            response.Content = new StringContent(payload, Encoding.UTF8, "application/json");
            return response;
        }
        finally
        {
            _semaphore?.Release();
        }
    }

    private static HttpResponseMessage CreateOkJsonResponse(string payload)
    {
        var msg = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(payload, Encoding.UTF8, "application/json")
        };
        return msg;
    }
}