using Microsoft.Extensions.AI;
using MyAIChatApp.Components;
using MyAIChatApp.Services;
using MyAIChatApp.Services.Ingestion;
using Azure;
using Azure.Identity;
using Azure.AI.OpenAI;
using System.ClientModel;
using Microsoft.Azure.Cosmos;
using MongoDB.Driver;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddRazorComponents().AddInteractiveServerComponents();

var azureOpenAi = new AzureOpenAIClient(
    new Uri(builder.Configuration["AzureOpenAI:Endpoint"] ?? throw new InvalidOperationException("Missing configuration: AzureOpenAi:Endpoint. See the README for details.")),
    new DefaultAzureCredential());
var chatClient = azureOpenAi.GetChatClient("gpt-4o-mini").AsIChatClient();
var embeddingGenerator = azureOpenAi.GetEmbeddingClient("text-embedding-3-small").AsIEmbeddingGenerator();

var vectorStorePath = Path.Combine(AppContext.BaseDirectory, "vector-store.db");
var vectorStoreConnectionString = $"Data Source={vectorStorePath}";
if (File.Exists(vectorStorePath))
{
    File.Delete(vectorStorePath);
}

builder.Services.AddSqliteCollection<string, IngestedChunk>("data-myaichatapp-chunks", vectorStoreConnectionString);
builder.Services.AddSqliteCollection<string, IngestedDocument>("data-myaichatapp-documents", vectorStoreConnectionString);

builder.Services.AddScoped<DataIngestor>();
builder.Services.AddSingleton<SemanticSearch>();
builder.Services.AddChatClient(chatClient).UseFunctionInvocation().UseLogging();
builder.Services.AddEmbeddingGenerator(embeddingGenerator);


var elasticsearchUrl = builder.Configuration["Elasticsearch:Url"] ?? throw new InvalidOperationException("Missing ElasticSearch url");
var ElasticsearchIndex = builder.Configuration["Elasticsearch:Index"] ?? throw new InvalidOperationException("Missing Elastic Search Index");
var username = builder.Configuration["Elasticsearch:Username"] ?? throw new InvalidOperationException("Missing Elastic search username");
var password = builder.Configuration["Elasticsearch:Password"] ?? throw new InvalidOperationException("Missing Elastic search password");
var connectionString = builder.Configuration["Azure:BlobStorage:ConnectionString"] ?? throw new InvalidOperationException("Missing Azure Blob Storage connection string");

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseAntiforgery();
app.UseStaticFiles();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

Console.WriteLine("Starting data ingestion...");
using(var scope = app.Services.CreateScope())
{
    try
    {
       var logger = scope.ServiceProvider.GetRequiredService<ILogger<ElasticsearchSource>>();
        var config = scope.ServiceProvider.GetRequiredService<IConfiguration>();
        var indices = new[]
        {
            $"{ElasticsearchIndex}-allcustomers",
            $"{ElasticsearchIndex}-allorders",
            $"{ElasticsearchIndex}-allproducts"
        };
        foreach (var index in indices)
        {
            var ingestionSource = new ElasticsearchSource(logger, config, index);
            await DataIngestor.IngestDataAsync(app.Services, ingestionSource);
        }
        
        }
        catch (Exception ex)
        {
        Console.WriteLine("Elasticsearch ingestion failed: " + ex.Message);
        }
        var azureFileLogger = scope.ServiceProvider.GetRequiredService<ILogger<AzureFileStorageSource>>();
        var azureFileSource = new AzureFileStorageSource(
        connectionString,
        containerName:"lexicon-chatbot-test",
        directoryPrefix:"0000cfa77c2ee828bec499993eb"
    );
    await DataIngestor.IngestDataAsync(app.Services, azureFileSource); 
}
Console.WriteLine("Finished data ingestion.");

app.Run();