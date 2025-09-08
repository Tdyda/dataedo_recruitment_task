using System.Net.Http.Headers;
using System.Text;

namespace FivetranClient.Infrastructure;

public class FivetranHttpClient : HttpClient
{
    public FivetranHttpClient(Uri baseAddress, string apiKey, string apiSecret, FivetranClientOptions options)
    {
        if (options.Timeout.Ticks <= 0)
            throw new ArgumentOutOfRangeException(nameof(options.Timeout), "Timeout must be a positive value");

        DefaultRequestHeaders.Clear();
        BaseAddress = baseAddress;
        DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Basic", CalculateToken(apiKey, apiSecret));
        DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        DefaultRequestHeaders.UserAgent.ParseAdd(options.UserAgent);
        Timeout = options.Timeout;
    }

    private static string CalculateToken(string apiKey, string apiSecret)
    {
        return Convert.ToBase64String(Encoding.ASCII.GetBytes($"{apiKey}:{apiSecret}"));
    }
}