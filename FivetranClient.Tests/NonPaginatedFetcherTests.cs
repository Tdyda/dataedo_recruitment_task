using System.Net;
using FivetranClient.Fetchers;
using FivetranClient.Models;

namespace FivetranClient.Tests;

public class NonPaginatedFetcherTests
{
    [Fact]
    public async Task FetchAsync_DeserializesNonPaginatedResponse()
    {
        var handler = new StubHttpMessageHandler(req => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(
                "{ \"data\": { \"id\": \"123\", \"name\": \"TestGroup\" } }"
            )
        });

        var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://test.local/")
        };
        var requestHandler = new HttpRequestHandler(httpClient);
        var fetcher = new NonPaginatedFetcher(requestHandler);

        var result = await fetcher.FetchAsync<Group>("groups/123", CancellationToken.None);

        Assert.NotNull(result);
        Assert.Equal("123", result.Id);
        Assert.Equal("TestGroup", result.Name);
    }
}