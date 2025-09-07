using System.Net;

namespace FivetranClient.Tests;

public class RetryTests
{
    [Fact]
    public async Task GetAsync_RetriesOn429AndSucceeds()
    {
        var calls = 0;
        var handler = new StubHttpMessageHandler(req =>
        {
            calls++;
            if (calls != 1)
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(
                        "{ \"data\": { \"items\": [\"ok\"], \"next_cursor\": null } }"
                    )
                };
            var resp = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
            resp.Headers.RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromMilliseconds(10));
            return resp;
        });

        var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://test.local/")
        };
        var requestHandler = new HttpRequestHandler(httpClient);

        var response = await requestHandler.GetAsync("testendpoint", CancellationToken.None);
        var payload = await response.Content.ReadAsStringAsync();

        Assert.Contains("ok", payload);
        Assert.Equal(2, calls);
    }
}