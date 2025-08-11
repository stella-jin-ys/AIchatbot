using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MyAIChatApp.Services.Ingestion;
using MyAIChatApp.Services;
using Microsoft.SemanticKernel.Text;
using System.Security.Cryptography;
using System.Text;
using Elasticsearch.Net;
using Nest;

namespace MyAIChatApp.Services.Ingestion;

public class ElasticsearchSource : IIngestionSource
{
    private readonly ILogger<ElasticsearchSource> _logger;
    private readonly ElasticClient _client;
    private readonly string _indexName;

    public ElasticsearchSource(ILogger<ElasticsearchSource> logger, IConfiguration config, string index)
    {
        _logger = logger;
        var url = config["Elasticsearch:Url"];
        var username = config["Elasticsearch:Username"];
        var password = config["Elasticsearch:Password"];
        _indexName = index;

        var settings = new ConnectionSettings(new Uri(url)).BasicAuthentication(username,password).DefaultIndex(index);
        _client = new ElasticClient(settings);
    }

    public string SourceId => "Elasticsearch";

    public async Task<IEnumerable<IngestedDocument>> GetNewOrModifiedDocumentsAsync(IReadOnlyList<IngestedDocument> existingDocuments)
    {
        var newDocs = new List<IngestedDocument>();
        var scrollTimeout = "2m";

        var searchResponse = await _client.SearchAsync<dynamic>(s => s
            .Index(_indexName)
            .Size(1000) // Adjust as needed
            .Scroll(scrollTimeout)
            .Query(q => q.MatchAll())
            .Source(src => src.Includes(i => i.Fields("Name","Status","Type","OrderNumber","CreatedAt","CustomerName","TotalPrice","OrderStatus","OrderCurrency","PaymentStatus","Id","ProductId", "CustomerIds" )))
        );
        if (!searchResponse.IsValid)
        {
            _logger.LogError("Initial scroll search failed: {Error}", searchResponse.OriginalException.Message);
            return newDocs;
        }
        var scrollId = searchResponse.ScrollId;
        while(true)
        {
             foreach (var hit in searchResponse.Hits)
                {
                    var id = hit.Id;
                    var json = hit.Source.ToString();

                 // Generate a SHA256 hash of the document content
                    using var sha = SHA256.Create();
                    var hashBytes = sha.ComputeHash(Encoding.UTF8.GetBytes(json));
                    var version = Convert.ToHexString(hashBytes);

                    bool exists = existingDocuments.Any(e => e.DocumentId == id && e.DocumentVersion == version);

                    // If no existing docs, treat all as new
                     if (existingDocuments.Count == 0 || !exists)
                        {
                           newDocs.Add(new IngestedDocument
                            {
                            Key = Guid.NewGuid().ToString(),
                            SourceId = SourceId,
                            DocumentId = id,
                            DocumentVersion = version
                            });
                        }
                    else
                        {
                          _logger.LogInformation("⏭️ Skipping already ingested doc: {docId}", id);
                         }
                }
                // Fetch next batch
                searchResponse = await _client.ScrollAsync<dynamic>(scrollTimeout, scrollId);
                if (!searchResponse.IsValid || searchResponse.Hits.Count == 0)
                {
                    break;
                }

                scrollId = searchResponse.ScrollId;
        }
        // Clear scroll context
        await _client.ClearScrollAsync(c => c.ScrollId(scrollId));
        return newDocs;
    } 

    public Task<IEnumerable<IngestedDocument>> GetDeletedDocumentsAsync(IReadOnlyList<IngestedDocument> existingDocuments)
    {
        // No delete tracking for now
        return Task.FromResult<IEnumerable<IngestedDocument>>(Array.Empty<IngestedDocument>());
    }

    public async Task<IEnumerable<IngestedChunk>> CreateChunksForDocumentAsync(IngestedDocument document)
    {
        var getResponse = await _client.GetAsync<dynamic>(document.DocumentId, g => g.Index(_indexName));
        if (!getResponse.Found)
        {
            return Array.Empty<IngestedChunk>();
        }

        var source = getResponse.Source as IDictionary<string, object>;
        var stringBuilder = new StringBuilder();

        foreach (var kvp in source)
        {
            var value = kvp.Value?.ToString();
            if (value?.Length > 3000)
                 {
                  stringBuilder.AppendLine($"{kvp.Key}: {value.Substring(0, 3000)}... [truncated]");
                  }
            else
                 {
                stringBuilder.AppendLine($"{kvp.Key}: {value}");
                 }
        }

        int ExtractVariantIndex(string field)
        {
            var match = System.Text.RegularExpressions.Regex.Match(field, @"Variant\[(\d+)\]");
             return (match.Success && int.TryParse(match.Groups[1].Value, out var idx)) ? idx : -1 ;
        }
        var content = stringBuilder.ToString();
           // Truncate final document if needed
        if (content.Length > 20000)
            {
            content = content.Substring(0, 20000) + "... [truncated]";
            }

        return new[]
        {
            new IngestedChunk
            {
                Key = Guid.NewGuid().ToString(),
                DocumentId = document.DocumentId,
                PageNumber = 1,
                Text = content
            }
        };
    }
}