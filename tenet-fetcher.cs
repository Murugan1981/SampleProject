#nullable enable

using System;
using System.Threading.Tasks;
using OfficeOpenXml;
using TenantExtractor.Services;

namespace TenantExtractor
{
    internal static class Program
    {
        private static async Task Main(string[] args)
        {
            // ✔ EPPlus 8 — Correct License Configuration
            ExcelPackage.License = new ExcelLicense
            {
                LicenseContext = LicenseContext.NonCommercial
            };

            Console.WriteLine("Executing Tenant Extractor...");
            var fetcher = new TenantFetcher();

            await fetcher.Run();
            Console.WriteLine("Completed");
        }
    }
}
