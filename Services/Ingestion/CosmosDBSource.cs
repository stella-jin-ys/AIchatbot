using Microsoft.Azure.Cosmos;
using MyAIChatApp.Services.Ingestion;
using MyAIChatApp.Services;
using Microsoft.SemanticKernel.Text;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System.Text;
using System.Security.Cryptography;


namespace MyAIChatApp.Services.Ingestion;

public class CosmosDBSource : IIngestionSource
{
    private readonly IMongoCollection<BsonDocument> _collection;

    public CosmosDBSource(MongoClient client, string databaseId, string containerId)
    {
        var database = client.GetDatabase(databaseId);
        _collection = database.GetCollection<BsonDocument>(containerId);
    }
    

    public string SourceId => "CosmosDB-Mongo-Combined";

       public async Task<IEnumerable<IngestedDocument>> GetNewOrModifiedDocumentsAsync(IReadOnlyList<IngestedDocument> existingDocuments)
{
    Console.WriteLine("CosmosDB: Fetching all documents...");

    var docs = await _collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();

    var newDocs = new List<IngestedDocument>();

    foreach (var d in docs)
    {
        var id = d["_id"].ToString();
        var json = d.ToJson();

        // ✅ Generate a SHA256 hash of the document content
        using var sha = SHA256.Create();
        var hashBytes = sha.ComputeHash(Encoding.UTF8.GetBytes(json));
        var version = Convert.ToHexString(hashBytes); // e.g., "A3B34C..."

        var exists = existingDocuments.Any(e => e.DocumentId == id && e.DocumentVersion == version);
        if (!exists)
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
            Console.WriteLine($"Skipping already ingested doc: {id}");
        }
    }

    return newDocs;
}

   public Task<IEnumerable<IngestedDocument>> GetDeletedDocumentsAsync(IReadOnlyList<IngestedDocument> existingDocuments)
    {
        // No delete tracking for now
        return Task.FromResult<IEnumerable<IngestedDocument>>(Array.Empty<IngestedDocument>());
    }

    public async Task<IEnumerable<IngestedChunk>> CreateChunksForDocumentAsync(IngestedDocument document)
    {
         ObjectId objectId;
        try
        {
            objectId = ObjectId.Parse(document.DocumentId);
        }
        catch (FormatException)
        {
            Console.WriteLine($"Invalid ObjectId format: {document.DocumentId}");
            return Array.Empty<IngestedChunk>();
        }

        var filter = Builders<BsonDocument>.Filter.Eq("_id", objectId);
        var result = await _collection.Find(filter).FirstOrDefaultAsync();

        if (result == null)
        {
            return Array.Empty<IngestedChunk>();
        }

        // Format each field as "key: value"
        var stringBuilder = new StringBuilder();
        foreach (var element in result.Elements)
        {
            stringBuilder.AppendLine($"{element.Name}: {element.Value}");
        }

        var content = stringBuilder.ToString(); 

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