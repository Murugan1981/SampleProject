using Newtonsoft.Json;
using DotNetEnv;
using System.Net;
using System.Net.Http;

public class TenantFetcher
{
    private readonly string RAW_PATH = Path.Combine("shared", "raw");

    public TenantFetcher()
    {
        Directory.CreateDirectory(RAW_PATH);
    }

    private HttpClient GetHttpClient(string username, string password)
    {
        if (string.IsNullOrWhiteSpace(username))
            throw new ArgumentNullException(nameof(username));

        if (string.IsNullOrWhiteSpace(password))
            throw new ArgumentNullException(nameof(password));

        // Split domain\username safely
        var parts = username.Split('\\');
        string domain = parts.Length > 1 ? parts[0] : "";
        string user = parts.Length > 1 ? parts[1] : parts[0];

        var handler = new HttpClientHandler
        {
            Credentials = new NetworkCredential(user, password, domain)
        };

        return new HttpClient(handler);
    }

    public async Task<List<TenantRecord>> Fetch(string url, string username, string password)
    {
        if (string.IsNullOrWhiteSpace(url))
            throw new ArgumentNullException(nameof(url));

        var client = GetHttpClient(username, password);

        var response = await client.GetAsync(url);
        response.EnsureSuccessStatusCode();

        string json = await response.Content.ReadAsStringAsync();

        // JSON can be null → safe deserialization
        var result = JsonConvert.DeserializeObject<List<TenantRecord>>(json)
                     ?? new List<TenantRecord>();

        return result;
    }

    public async Task Run()
    {
        Env.Load();

        string? prdUrl = Environment.GetEnvironmentVariable("PRD_URL");
        string? uatUrl = Environment.GetEnvironmentVariable("UAT_URL");
        string? username = Environment.GetEnvironmentVariable("USERNAME");
        string? password = Environment.GetEnvironmentVariable("PASSWORD");

        if (string.IsNullOrWhiteSpace(prdUrl) ||
            string.IsNullOrWhiteSpace(uatUrl) ||
            string.IsNullOrWhiteSpace(username) ||
            string.IsNullOrWhiteSpace(password))
        {
            throw new Exception("Missing values in .env file. Check PRD_URL, UAT_URL, USERNAME, PASSWORD.");
        }

        var prdData = await Fetch(prdUrl, username, password);
        var uatData = await Fetch(uatUrl, username, password);

        File.WriteAllText(Path.Combine(RAW_PATH, "tenant_prd.json"),
            JsonConvert.SerializeObject(prdData, Formatting.Indented));

        File.WriteAllText(Path.Combine(RAW_PATH, "tenant_uat.json"),
            JsonConvert.SerializeObject(uatData, Formatting.Indented));

        await ExcelWriter.Save(prdData, uatData, RAW_PATH);
    }
}
