#nullable enable

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using TenantExtractor.Models;

namespace TenantExtractor.Services
{
    public static class CsvWriter
    {
        // ✅ For TenantRecord flattening (used by TenantFetcher)
        public static void SaveCsv(List<TenantRecord> data, string outputPath)
        {
            var flattened = data.Select(TenantFlattener.Flatten).ToList();

            if (!flattened.Any())
            {
                File.WriteAllText(outputPath, "NO DATA");
                return;
            }

            var headers = flattened.SelectMany(d => d.Keys).Distinct().ToList();

            var sb = new StringBuilder();

            // Header
            sb.AppendLine(string.Join(",", headers));

            // Rows
            foreach (var row in flattened)
            {
                var values = headers.Select(h => EscapeCsv(row.ContainsKey(h) ? row[h] : ""));
                sb.AppendLine(string.Join(",", values));
            }

            File.WriteAllText(outputPath, sb.ToString());
        }

        // ✅ For Swagger Metadata output (generic dictionaries)
        public static void WriteDictionaries(string outputPath, List<Dictionary<string, string>> rows)
        {
            if (!rows.Any())
            {
                File.WriteAllText(outputPath, "NO DATA");
                return;
            }

            var headers = rows.SelectMany(r => r.Keys).Distinct().ToList();
            var sb = new StringBuilder();

            sb.AppendLine(string.Join(",", headers));

            foreach (var row in rows)
            {
                var values = headers.Select(h => EscapeCsv(row.TryGetValue(h, out var val) ? val : ""));
                sb.AppendLine(string.Join(",", values));
            }

            File.WriteAllText(outputPath, sb.ToString());
        }

        private static string EscapeCsv(object? value)
        {
            if (value == null) return "";

            var str = value.ToString() ?? "";

            if (str.Contains(",") || str.Contains("""))
                str = $""{str.Replace(""", """")}"";

            return str;
        }
    }
}
