using FivetranClient.Fetchers;

namespace FivetranClient.Tests;

using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Models;

public class PaginationTests
{
    private static readonly string[] Expected = ["a", "b", "c"];

    [Fact]
    public async Task FetchItemsAsync_ReturnsAllItemsAcrossPages()
    {
        var handler = new StubHttpMessageHandler(req =>
        {
            if (!req.RequestUri!.ToString().Contains("cursor"))
            {
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(
                        "{ \"data\": { \"items\": [\"a\",\"b\"], \"next_cursor\": \"cursor1\" } }"
                    )
                };
            }

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    "{ \"data\": { \"items\": [\"c\"], \"next_cursor\": null } }"
                )
            };
        });

        var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://test.local/")
        };
        var requestHandler = new HttpRequestHandler(httpClient);
        var fetcher = new PaginatedFetcher(requestHandler);
        
        var results = new List<string>();
        await foreach (var item in fetcher.FetchItemsAsync<string>("testendpoint", CancellationToken.None))
        {
            results.Add(item);
        }
        
        Assert.Equal(Expected, results);
    }
}
