using System.Net;
using FivetranClient.Infrastructure;

namespace FivetranClient;

public class HttpRequestHandler
{
    private const int Max429Retries = 3;
    private readonly HttpClient _client;
    private readonly SemaphoreSlim? _semaphore;
    private readonly object _lock = new();
    private DateTime _retryAfterTime = DateTime.UtcNow;
    private static readonly TtlDictionary<string, string> PayloadCache = new();
    private readonly FivetranClientOptions _options;

    /// <summary>
    /// Handles HttpTooManyRequests responses by limiting the number of concurrent requests and managing retry logic.
    /// Also caches responses to avoid unnecessary network calls.
    /// </summary>
    /// <remarks>
    /// Set <paramref name="maxConcurrentRequests"/> to 0 to disable concurrency limit.
    /// </remarks>
    public HttpRequestHandler(HttpClient client, FivetranClientOptions? options = null)
    {
        this._client = client;
        this._options = options ?? new FivetranClientOptions();

        if (this._options.MaxConcurrentRequests > 0)
        {
            this._semaphore = new SemaphoreSlim(this._options.MaxConcurrentRequests, this._options.MaxConcurrentRequests);
        }
    }

    public async Task<HttpResponseMessage> GetAsync(string url, CancellationToken cancellationToken)
    {
        for (var attempt = 0;; attempt++)
        {
            var response = await this._GetOnceAsync(url, cancellationToken);

            if (response.StatusCode == HttpStatusCode.TooManyRequests)
            {
                if (attempt >= Max429Retries)
                {
                    response.Dispose();
                    throw new HttpRequestException(
                        $"Too Many Requests (429) for '{url}'. Retry limit ({Max429Retries}) exceeded.");
                }

                var retryAfter = response.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(60);

                lock (this._lock)
                {
                    this._retryAfterTime = DateTime.UtcNow.Add(retryAfter);
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
        if (this._semaphore is not null)
        {
            await this._semaphore.WaitAsync(cancellationToken);
        }

        try
        {
            if (PayloadCache.TryGetValue(url, out var cachedPayload))
            {
                return CreateOkJsonResponse(cachedPayload);
            }

            TimeSpan timeToWait;
            lock (this._lock)
            {
                timeToWait = this._retryAfterTime - DateTime.UtcNow;
            }

            if (timeToWait > TimeSpan.Zero)
            {
                await Task.Delay(timeToWait, cancellationToken);
            }

            cancellationToken.ThrowIfCancellationRequested();

            var response = await this._client.GetAsync(new Uri(url, UriKind.Relative), cancellationToken);
            
            if (response.StatusCode == HttpStatusCode.TooManyRequests)
            {
                return response;
            }

            var payload = await response.Content.ReadAsStringAsync(cancellationToken);
            if (response.IsSuccessStatusCode)
            {
                PayloadCache.GetOrAdd(url, () => payload, _options.CacheTtl ?? TimeSpan.FromSeconds(30));
            }

            response.Content = new StringContent(payload, System.Text.Encoding.UTF8, "application/json");
            return response;
        }
        finally
        {
            this._semaphore?.Release();
        }
    }

    private static HttpResponseMessage CreateOkJsonResponse(string payload)
    {
        var msg = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(payload, System.Text.Encoding.UTF8, "application/json")
        };
        return msg;
    }
}