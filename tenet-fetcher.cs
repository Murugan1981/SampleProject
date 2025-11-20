#nullable enable

using System.IO;
using System.Linq;
using System.Collections.Generic;
using OfficeOpenXml;
using TenantExtractor.Models;
// Optional alias to avoid any LicenseContext ambiguity:
// using EPPLicenseContext = OfficeOpenXml.LicenseContext;

namespace TenantExtractor.Services
{
    public static class ExcelWriter
    {
        public static void Save(List<TenantRecord> prd, List<TenantRecord> uat, string rawPath)
        {
            // Either keep the fully qualified form:
            OfficeOpenXml.ExcelPackage.LicenseContext = OfficeOpenXml.LicenseContext.NonCommercial;
            // Or, if you used the alias above:
            // ExcelPackage.LicenseContext = EPPLicenseContext.NonCommercial;

            string excelPath = Path.Combine(rawPath, "tenant_data.xlsx");

            using (var package = new ExcelPackage())
            {
                var prdSheet = package.Workbook.Worksheets.Add("PRD");
                var uatSheet = package.Workbook.Worksheets.Add("UAT");

                WriteSheet(prdSheet, prd);
                WriteSheet(uatSheet, uat);

                package.SaveAs(new FileInfo(excelPath));
            }
        }

        private static void WriteSheet(OfficeOpenXml.ExcelWorksheet sheet, List<TenantRecord> data)
        {
            var flattened = data.Select(TenantFlattener.Flatten).ToList();
            var headers = flattened.SelectMany(x => x.Keys).Distinct().ToList();

            for (int col = 0; col < headers.Count; col++)
                sheet.Cells[1, col + 1].Value = headers[col];

            for (int row = 0; row < flattened.Count; row++)
            {
                for (int col = 0; col < headers.Count; col++)
                {
                    flattened[row].TryGetValue(headers[col], out var value);
                    sheet.Cells[row + 2, col + 1].Value = value;
                }
            }
        }
    }
}
