namespace FivetranClient;

public class FivetranClientOptions
{
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(40);
    public ushort MaxConcurrentRequests { get; set; } = 0;
    public string UserAgent { get; set; } = "Dataedo-FivetranClient/1.0";
    public TimeSpan? CacheTtl { get; set; } = TimeSpan.FromSeconds(30);
}