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
    private static TtlDictionary<string, HttpResponseMessage> _responseCache = new();

    /// <summary>
    /// Handles HttpTooManyRequests responses by limiting the number of concurrent requests and managing retry logic.
    /// Also caches responses to avoid unnecessary network calls.
    /// </summary>
    /// <remarks>
    /// Set <paramref name="maxConcurrentRequests"/> to 0 to disable concurrency limit.
    /// </remarks>
    public HttpRequestHandler(HttpClient client, ushort maxConcurrentRequests = 0)
    {
        this._client = client;
        if (maxConcurrentRequests > 0)
        {
            this._semaphore = new SemaphoreSlim(0, maxConcurrentRequests);
        }
    }

    public async Task<HttpResponseMessage> GetAsync(string url, CancellationToken cancellationToken)
    {
        for (var attempt = 0; ; attempt++)
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
                var retryAfter = response.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(60);
                lock (this._lock)
                {
                    this._retryAfterTime = DateTime.UtcNow.Add(retryAfter);
                }
            }
            return response;
        }
        finally
        {
            this._semaphore?.Release();
        }
    }

}