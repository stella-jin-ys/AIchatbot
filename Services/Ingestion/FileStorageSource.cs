using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.SemanticKernel.Text;
using UglyToad.PdfPig;
using UglyToad.PdfPig.DocumentLayoutAnalysis.PageSegmenter;
using UglyToad.PdfPig.DocumentLayoutAnalysis.WordExtractor;
using UglyToad.PdfPig.Content;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using System.IO;
using System.Text;

namespace MyAIChatApp.Services.Ingestion;

public class AzureFileStorageSource(string connectionString, string containerName, string directoryPrefix):IIngestionSource
{
    private readonly BlobContainerClient _containerClient = new(connectionString, containerName);
    private readonly string _directoryPrefix = directoryPrefix.EndsWith("/") ? directoryPrefix : directoryPrefix+"/";
    public string SourceId => $"{nameof(AzureFileStorageSource)}:{_containerClient.Name}";
    public static string SourceFileId(string blobName) => blobName;
    public static string SourceFileVersion(BlobItem blobItem) => blobItem.Properties.LastModified?.ToString("o") ?? "";
    private static readonly HashSet<string> SupportedExtensions = new(StringComparer.OrdinalIgnoreCase){
        ".pdf", ".docx", ".doc", ".xlsx", ".xls", ".ppt", ".png", ".jpg", ".jpeg", ".gif"
    };

    public async Task<IEnumerable<IngestedDocument>> GetNewOrModifiedDocumentsAsync(IReadOnlyList<IngestedDocument> existingDocuments)
    {
        var results = new List<IngestedDocument>();
        var filteredExistingDocuments = existingDocuments
        .Where(d => d.DocumentId.StartsWith(_directoryPrefix, StringComparison.OrdinalIgnoreCase))
        .ToDictionary(d => d.DocumentId);

        await foreach(var blobItem in _containerClient.GetBlobsAsync(prefix:_directoryPrefix))
        {     
            if(!SupportedExtensions.Contains(Path.GetExtension(blobItem.Name)))
                continue;

            var blobClient = _containerClient.GetBlobClient(blobItem.Name);
            var sourceFileId = blobItem.Name;
            var sourceFileVersion = SourceFileVersion(blobItem);
            var existingDocumentVersion = filteredExistingDocuments.TryGetValue(sourceFileId, out var existingDocument) ? existingDocument.DocumentVersion : null;
            
            if(existingDocumentVersion != sourceFileVersion)
            {
                results.Add(new IngestedDocument
                {
                    Key = Guid.CreateVersion7().ToString(),
                    SourceId = SourceId,
                    DocumentId = sourceFileId,
                    DocumentVersion = sourceFileVersion
                });
            }
        }
        return results;
    }
    public async Task<IEnumerable<IngestedDocument>> GetDeletedDocumentsAsync(IReadOnlyList<IngestedDocument> existingDocuments)
    {
        var currentBlobNames = new HashSet<string>();

        await foreach (var blobItem in _containerClient.GetBlobsAsync(prefix:_directoryPrefix))
        {
            var relativePath = blobItem.Name[_directoryPrefix.Length..];
            if(!SupportedExtensions.Contains(Path.GetExtension(blobItem.Name)))
                continue;

            currentBlobNames.Add(blobItem.Name);
        }

        var filteredExistingDocuments= existingDocuments.Where(d=>d.DocumentId.StartsWith(_directoryPrefix, StringComparison.OrdinalIgnoreCase));

        return filteredExistingDocuments.Where(d => !currentBlobNames.Contains(d.DocumentId));
    }
    public async Task<IEnumerable<IngestedChunk>> CreateChunksForDocumentAsync(IngestedDocument document)
    {
        if (!document.DocumentId.StartsWith(_directoryPrefix, StringComparison.OrdinalIgnoreCase))
            {
                throw new UnauthorizedAccessException($"Access to document '{document.DocumentId}' is denied. Outside directory '{_directoryPrefix}'.");
            }
        var blobClient = _containerClient.GetBlobClient(document.DocumentId);

        using var fileStream = new MemoryStream();
        await blobClient.DownloadToAsync(fileStream);
        fileStream.Position = 0;

        List<(int PageNumber, int IndexOnPage, string Text)> paragraphs;
        var extension = Path.GetExtension(document.DocumentId).ToLowerInvariant();
        switch (extension)
        {
            case ".pdf":
                paragraphs = ExtractPdfParagraphs(fileStream);
                break;
            case ".docx":
            case ".doc":
                paragraphs = ExtractWordParagraphs(fileStream);
                break;
           /*  case ".xlsx":
            case ".xls":
                paragraphs = ExtractExcelParagraphs(fileStream);
                break;
            case ".ppt":
                paragraphs = ExtractPowerPointParagraphs(fileStream);
                break; */
            default:
                throw new InvalidOperationException($"Unsupported file type: {extension}");
        }

        return paragraphs.Select(p => new IngestedChunk
        {
            Key = Guid.CreateVersion7().ToString(),
            DocumentId = document.DocumentId,
            PageNumber = p.PageNumber,
            Text = p.Text
        });
    }
     private static List<(int PageNumber, int IndexOnPage, string Text)> ExtractPdfParagraphs(Stream pdfStream)
    {
        using var pdf = PdfDocument.Open(pdfStream);
        return pdf.GetPages().SelectMany(GetPageParagraphs).ToList();
    }

    private static IEnumerable<(int PageNumber, int IndexOnPage, string Text)> GetPageParagraphs(UglyToad.PdfPig.Content.Page pdfPage)
    {
        var letters = pdfPage.Letters;
        var words = NearestNeighbourWordExtractor.Instance.GetWords(letters);
        var textBlocks = DocstrumBoundingBoxes.Instance.GetBlocks(words);
        var pageText = string.Join(Environment.NewLine + Environment.NewLine,
            textBlocks.Select(t => t.Text.ReplaceLineEndings(" ")));
        return ChunkPlainText(pageText, 200, pdfPage.Number);
    }
    private static List<(int PageNumber, int IndexOnPage, string Text)> ExtractWordParagraphs(Stream wordStream)
    {
        using var wordDoc = WordprocessingDocument.Open(wordStream, false);
        var body = wordDoc.MainDocumentPart?.Document.Body;
        if (body == null) return [];

        var textBuilder = new StringBuilder();
        foreach (var para in body.Elements<DocumentFormat.OpenXml.Wordprocessing.Paragraph>())
        {
            var text = para.InnerText;
            if (!string.IsNullOrWhiteSpace(text))
            {
                textBuilder.AppendLine(text.Trim());
            }
        }

        var allText = textBuilder.ToString().ReplaceLineEndings(" ");
        return ChunkPlainText(allText, 200, 1);
    }
   private static List<(int PageNumber, int IndexOnPage, string Text)> ExtractExcelParagraphs(Stream excelStream)
    {
        using var document = SpreadsheetDocument.Open(excelStream, false);
        var workbookPart = document.WorkbookPart;
        if (workbookPart == null) return [];

        var sheets = workbookPart.Workbook.Sheets.Cast<Sheet>();
        var textBuilder = new StringBuilder();
        int sheetIndex = 1;

        foreach(var sheet in sheets)
        {
            if (sheet.Id == null) continue;
            var worksheetPart = (WorksheetPart)workbookPart.GetPartById(sheet.Id);
             var sheetData = worksheetPart.Worksheet.Elements<SheetData>().FirstOrDefault();
            if (sheetData == null) continue;

            foreach (var row in sheetData.Elements<Row>())
            {
                foreach (var cell in row.Elements<Cell>())
                {
                    var cellValue = GetCellValue(document, cell);
                    if (!string.IsNullOrWhiteSpace(cellValue))
                        textBuilder.Append(cellValue + " ");
                }
            }
            textBuilder.AppendLine();
            sheetIndex++;
        }
        var allText = textBuilder.ToString().ReplaceLineEndings(" ");
        return ChunkPlainText(allText, 200, 1);
    }
    private static string? GetCellValue(SpreadsheetDocument document, Cell cell)
    {
        var value = cell.CellValue?.InnerText;
        if (value == null) return null;

        if (cell.DataType != null && cell.DataType == DocumentFormat.OpenXml.Spreadsheet.CellValues.SharedString)
        {
            var stringTable = document.WorkbookPart.SharedStringTablePart?.SharedStringTable;
            if (stringTable != null)
            {
                int id = int.Parse(value);
                if (id >= 0 && id < stringTable.Count())
                    return stringTable.ElementAt(id).InnerText;
            }
        }
        return value;
    }
    private static List<(int PageNumber, int IndexOnPage, string Text)> ExtractPowerPointParagraphs(Stream pptStream)
    {
        using var presentationDocument = DocumentFormat.OpenXml.Packaging.PresentationDocument.Open(pptStream, false);
        var slides = presentationDocument.PresentationPart?.SlideParts;
        if (slides == null) return new List<(int, int, string)>();

        var textBuilder = new StringBuilder();
        int slideNumber = 1;

        foreach (var slidePart in slides)
        {
            var texts = slidePart.Slide.Descendants<DocumentFormat.OpenXml.Drawing.Text>().Select(t => t.Text);
            foreach (var text in texts)
            {
                if (!string.IsNullOrWhiteSpace(text))
                    textBuilder.AppendLine(text.Trim());
            }
            slideNumber++;
        }

        var allText = textBuilder.ToString().ReplaceLineEndings(" ");
        return ChunkPlainText(allText, 200, 1);
    } 
    private static List<(int PageNumber, int IndexOnPage, string Text)> ChunkPlainText(string input, int maxCharsPerChunk, int pageNumber = 1)
    {
        var chunks = new List<(int PageNumber, int IndexOnPage, string Text)>();
        var words = input.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var sb = new StringBuilder();
        int index = 0;

        foreach (var word in words)
        {
            if (sb.Length + word.Length + 1 > maxCharsPerChunk)
            {
                chunks.Add((pageNumber, index++, sb.ToString().Trim()));
                sb.Clear();
            }
            sb.Append(word + " ");
        }

        if (sb.Length > 0)
        {
            chunks.Add((pageNumber, index, sb.ToString().Trim()));
        }

        return chunks;
    }    
}