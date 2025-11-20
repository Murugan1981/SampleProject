using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using TenantExtractor.Helpers;

namespace TenantExtractor.Services
{
    public static class FetchSwaggerMetadata
    {
        private static readonly string reportPath = Path.Combine("shared", "reports");

        public static async Task RunAsync()
        {
            Console.WriteLine("Executing Swagger Metadata Fetcher...");

            await ProcessSheetAsync("PRD");
            await ProcessSheetAsync("UAT");

            Console.WriteLine("Swagger metadata extraction completed.");
        }

        private static async Task ProcessSheetAsync(string env)
        {
            var inputCsvPath = Path.Combine(reportPath, $"tenant_{env.ToLower()}.csv");

            if (!File.Exists(inputCsvPath))
            {
                Console.WriteLine($"❌ CSV file not found: {inputCsvPath}");
                return;
            }

            var lines = File.ReadAllLines(inputCsvPath).ToList();
            if (lines.Count < 2)
            {
                Console.WriteLine("❌ CSV file is empty or has no data rows.");
                return;
            }

            // Normalize headers
            var headers = lines[0].Split(',').Select(h => h.Trim().ToLower()).ToList();

            // Detect required columns dynamically
            int systemIdx = headers.FindIndex(h => h == "system");
            int regionIdx = headers.FindIndex(h => h == "region");
            int urlTypeIdx = headers.FindIndex(h => h == "urltype");
            int baseUrlIdx = headers.FindIndex(h => h.Contains("dataservice_url"));

            if (systemIdx == -1 || regionIdx == -1 || urlTypeIdx == -1 || baseUrlIdx == -1)
            {
                Console.WriteLine("❌ Required column(s) not found. Check header names.");
                return;
            }

            var allMetadata = new List<Dictionary<string, string>>();

            for (int i = 1; i < lines.Count; i++)
            {
                var cols = lines[i].Split(',');

                if (cols.Length <= Math.Max(Math.Max(systemIdx, regionIdx), baseUrlIdx))
                {
                    Console.WriteLine($"⚠️ Skipping row {i + 1} due to missing columns.");
                    continue;
                }

                string system = cols[systemIdx].Trim();
                string region = cols[regionIdx].Trim();
                string urltype = cols[urlTypeIdx].Trim();
                string baseurl = cols[baseUrlIdx].Trim();

                if (string.IsNullOrWhiteSpace(baseurl))
                    continue;

                string swaggerUrl = baseurl.EndsWith("/") ? baseurl : baseurl + "/";
                swaggerUrl += system.ToLower() == "jil" ? "swagger/docs/v1" : "swagger/v1/swagger.json";

                Console.WriteLine($"📦 Fetching Swagger for: {system} | {region} | {urltype} -> {swaggerUrl}");

                try
                {
                    var result = await ExtractEndpointsFromSwagger(swaggerUrl);
                    foreach (var entry in result)
                    {
                        entry["System"] = system;
                        entry["Region"] = region;
                        entry["URLTYPE"] = urltype;
                        entry["SwaggerURL"] = swaggerUrl;
                    }

                    allMetadata.AddRange(result);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Failed to fetch: {swaggerUrl} - {ex.Message}");
                }
            }

            var outputPath = Path.Combine(reportPath, $"{env}_Metadata.csv");
            CsvWriter.WriteDictionaries(outputPath, allMetadata);
        }

        private static async Task<List<Dictionary<string, string>>> ExtractEndpointsFromSwagger(string swaggerUrl)
        {
            using var httpClient = new HttpClient();
            var json = await httpClient.GetStringAsync(swaggerUrl);

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            var results = new List<Dictionary<string, string>>();

            if (!root.TryGetProperty("paths", out var paths))
                return results;

            foreach (var path in paths.EnumerateObject())
            {
                string endpoint = path.Name;

                foreach (var method in path.Value.EnumerateObject())
                {
                    var row = new Dictionary<string, string>
                    {
                        ["Endpoint"] = endpoint,
                        ["Method"] = method.Name.ToUpper()
                    };

                    if (method.Value.TryGetProperty("tags", out var tags))
                        row["Tags"] = string.Join(";", tags.EnumerateArray().Select(t => t.GetString()));

                    if (method.Value.TryGetProperty("summary", out var summary))
                        row["Summary"] = summary.GetString() ?? "";

                    results.Add(row);
                }
            }

            return results;
        }
    }
}
