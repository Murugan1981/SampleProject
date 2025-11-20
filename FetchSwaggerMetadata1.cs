#nullable enable

using System;
using System.IO;
using System.Net;
using System.Text.Json;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Http;
using System.Threading.Tasks;
using System.Text.Json.Nodes;
using Newtonsoft.Json.Linq;

namespace TenantExtractor.Services
{
    public static class FetchSwaggerMetadata
    {
        private static readonly string rawPath = Path.Combine("shared", "raw");
        private static readonly string reportPath = Path.Combine("shared", "reports");
        private static readonly string inputPath = Path.Combine("shared", "input");
        private static readonly string jsonFile = Path.Combine(inputPath, "ApiTestData.json");

        private static string? systemFilter;
        private static string? regionFilter;
        private static string? urlTypeFilter;
        private static string? username;
        private static string? password;

        public static async Task RunAsync()
        {
            Console.WriteLine("Executing Swagger Metadata Fetcher...");
            LoadEnv();
            await ProcessSheetAsync("PRD");
            await ProcessSheetAsync("UAT");
            Console.WriteLine("✅ Swagger metadata extraction completed.");
        }

        private static void LoadEnv()
        {
            DotNetEnv.Env.Load();
            username = Environment.GetEnvironmentVariable("USERNAME");
            password = Environment.GetEnvironmentVariable("PASSWORD");

            var json = File.ReadAllText(jsonFile);
            var config = JsonNode.Parse(json);

            systemFilter = config?["System"]?.ToString();
            regionFilter = config?["Region"]?.ToString();
            urlTypeFilter = config?["URLTYPE"]?.ToString();

            Directory.CreateDirectory(reportPath);
        }

        private static async Task ProcessSheetAsync(string env)
        {
            var inputCsv = Path.Combine(reportPath, $"tenant_{env}.csv");
            if (!File.Exists(inputCsv))
            {
                Console.WriteLine($"❌ Input file not found: {inputCsv}");
                return;
            }

            var lines = File.ReadAllLines(inputCsv);
            if (lines.Length < 2)
            {
                Console.WriteLine("❌ CSV has no data rows.");
                return;
            }

            var headers = lines[0].Split(',');
            int systemIndex = Array.FindIndex(headers, h => h.Trim().Equals("SYSTEM", StringComparison.OrdinalIgnoreCase));
            int regionIndex = Array.FindIndex(headers, h => h.Trim().Equals("REGION", StringComparison.OrdinalIgnoreCase));
            int urltypeIndex = Array.FindIndex(headers, h => h.Trim().Equals("URLTYPE", StringComparison.OrdinalIgnoreCase));

            int baseurlIndex = Array.FindIndex(headers, h =>
                h.Trim().ToLower().Contains("addonlinks_dataservice_url"));

            if (systemIndex == -1 || regionIndex == -1 || urltypeIndex == -1 || baseurlIndex == -1)
            {
                Console.WriteLine("❌ Required column(s) not found.");
                return;
            }

            var metadata = new List<Dictionary<string, string>>();
            var errors = new List<Dictionary<string, string>>();

            foreach (var line in lines.Skip(1))
            {
                var cols = line.Split(',');

                string system = cols[systemIndex].Trim();
                string region = cols[regionIndex].Trim();
                string urltype = cols[urltypeIndex].Trim();
                string baseurl = cols[baseurlIndex].Trim();

                if (system != systemFilter || region != regionFilter || urltype != urlTypeFilter)
                    continue;

                if (string.IsNullOrWhiteSpace(baseurl) || baseurl.ToLower() == "nan")
                {
                    Console.WriteLine($"⚠️ Skipping blank base URL for {system} | {region} | {urltype}");
                    continue;
                }

                string swaggerUrl = system.ToLower() == "jil"
                    ? $"{baseurl}/swagger/docs/v1"
                    : $"{baseurl}/swagger/v1/swagger.json";

                Console.WriteLine($"📥 Fetching Swagger for {system} | {region} | {urltype}");
                Console.WriteLine($"URL: {swaggerUrl}");

                var (results, error) = await FetchEndpoints(baseurl, swaggerUrl, system, region, env, urltype);
                if (error != null) errors.Add(error);
                else metadata.AddRange(results);
            }

            CsvWriter.WriteDictionaries(Path.Combine(reportPath, $"{env}_Metadata.csv"), metadata);
            if (errors.Any())
                CsvWriter.WriteDictionaries(Path.Combine(reportPath, $"{env}_Metadata_Error.csv"), errors);
        }

        private static async Task<(List<Dictionary<string, string>>, Dictionary<string, string>? error)> FetchEndpoints(
            string baseurl,
            string swaggerUrl,
            string system,
            string region,
            string env,
            string urltype)
        {
            try
            {
                using var handler = new HttpClientHandler { Credentials = new NetworkCredential(username, password) };
                using var client = new HttpClient(handler);
                var response = await client.GetAsync(swaggerUrl);
                if (!response.IsSuccessStatusCode)
                {
                    return (new(), new Dictionary<string, string>
                    {
                        ["System"] = system,
                        ["Region"] = region,
                        ["Env"] = env,
                        ["SwaggerURL"] = swaggerUrl,
                        ["URLTYPE"] = urltype,
                        ["Error"] = $"HTTP {(int)response.StatusCode}"
                    });
                }

                var json = await response.Content.ReadAsStringAsync();
                var root = JObject.Parse(json);
                var results = new List<Dictionary<string, string>>();
                var paths = root["paths"]?.ToObject<Dictionary<string, JObject>>() ?? new();

                foreach (var (endpoint, methods) in paths)
                {
                    foreach (var (method, detail) in methods)
                    {
                        string tag = detail["tags"]?.FirstOrDefault()?.ToString() ?? "";
                        string responseCode = detail["responses"]?.First?.Path.Split('.').Last() ?? "";
                        string responseDesc = detail["responses"]?.First?.First?["description"]?.ToString() ?? "";
                        var parameters = detail["parameters"]?.ToObject<List<JObject>>() ?? new();

                        string paramStr = string.Join("; ",
                            parameters.Select(p =>
                                $"{p["name"]} (in:{p["in"]},type:{p["schema"]?["type"] ?? "object"},required{p["required"] ?? false})"));

                        results.Add(new Dictionary<string, string>
                        {
                            ["System"] = system,
                            ["Region"] = region,
                            ["Env"] = env,
                            ["BASEURL"] = baseurl,
                            ["SwaggerURL"] = swaggerUrl,
                            ["URLTYPE"] = urltype,
                            ["Method"] = method.ToUpper(),
                            ["Endpoint"] = endpoint,
                            ["Tags"] = tag,
                            ["Response_Code"] = responseCode,
                            ["Response_Description"] = responseDesc,
                            ["Parameters"] = paramStr
                        });
                    }
                }

                return (results, null);
            }
            catch (Exception ex)
            {
                return (new(), new Dictionary<string, string>
                {
                    ["System"] = system,
                    ["Region"] = region,
                    ["Env"] = env,
                    ["SwaggerURL"] = swaggerUrl,
                    ["URLTYPE"] = urltype,
                    ["Error"] = ex.Message
                });
            }
        }
    }
}
