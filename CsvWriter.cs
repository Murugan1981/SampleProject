#nullable enable  // Enable nullable reference type warnings (C# 8+)

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using TenantExtractor.Models;  // Reference to TenantRecord model and flattener

namespace TenantExtractor.Services
{
    // Static utility class for writing CSV files
    public static class CsvWriter
    {
        // ✅ Method 1: SaveCsv
        // Used for saving flattened TenantRecord objects into CSV
        public static void SaveCsv(List<TenantRecord> data, string outputPath)
        {
            // Flatten each TenantRecord into a dictionary of {columnName -> value}
            var flattened = data.Select(TenantFlattener.Flatten).ToList();

            // If nothing to write, create a placeholder CSV
            if (!flattened.Any())
            {
                File.WriteAllText(outputPath, "NO DATA");
                return;
            }

            // Extract all unique headers (column names) across all rows
            var headers = flattened.SelectMany(d => d.Keys).Distinct().ToList();

            // StringBuilder is more efficient than string concatenation
            var sb = new StringBuilder();

            // Write header row
            sb.AppendLine(string.Join(",", headers));

            // Write data rows
            foreach (var row in flattened)
            {
                // For each header, write value from the row (or blank if missing)
                var values = headers.Select(h => EscapeCsv(row.ContainsKey(h) ? row[h] : ""));
                sb.AppendLine(string.Join(",", values));
            }

            // Write entire content to file
            File.WriteAllText(outputPath, sb.ToString());
        }

        // ✅ Method 2: WriteDictionaries
        // Used for writing generic dictionary-based data (like Swagger metadata)
        public static void WriteDictionaries(string outputPath, List<Dictionary<string, string>> rows)
        {
            // Empty check
            if (!rows.Any())
            {
                File.WriteAllText(outputPath, "NO DATA");
                return;
            }

            // Build combined header list from all keys
            var headers = rows.SelectMany(r => r.Keys).Distinct().ToList();
            var sb = new StringBuilder();

            // Header row
            sb.AppendLine(string.Join(",", headers));

            // Each row of data
            foreach (var row in rows)
            {
                // Try to get each value from the dictionary (or blank if not present)
                var values = headers.Select(h => EscapeCsv(row.TryGetValue(h, out var val) ? val : ""));
                sb.AppendLine(string.Join(",", values));
            }

            // Final write to CSV
            File.WriteAllText(outputPath, sb.ToString());
        }

        // ✅ Helper Method: EscapeCsv
        // Escapes CSV values by wrapping them in quotes if needed, and doubling internal quotes
        private static string EscapeCsv(object? value)
        {
            if (value == null) return "";

            var str = value.ToString() ?? "";

            // If the value contains a comma or a quote, wrap in quotes and escape quotes
            if (str.Contains(",") || str.Contains("\""))
                str = "\"" + str.Replace("\"", "\"\"") + "\"";

            return str;
        }
    }
}
