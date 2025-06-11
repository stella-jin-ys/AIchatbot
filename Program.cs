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

string mongoConnectionString = builder.Configuration["CosmosDB:MongoConnectionString"];
;
string databaseId = builder.Configuration["CosmosDB:DatabaseId"] ;
string containerId = builder.Configuration["CosmosDB:ContainerId"] ;

var mongoClient = new MongoClient(mongoConnectionString);
builder.Services.AddSingleton(mongoClient);

var elasticsearchUrl = builder.Configuration["Elasticsearch:Url"] ;
var ElasticsearchIndex = builder.Configuration["Elasticsearch:Index"];
var username = builder.Configuration["Elasticsearch:Username"];
var password = builder.Configuration["Elasticsearch:Password"];

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
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<ElasticsearchSource>>();
    var config = scope.ServiceProvider.GetRequiredService<IConfiguration>();

    var indices = new[]
    {
        $"{ElasticsearchIndex}-allcustomers",
        $"{ElasticsearchIndex}-allorders"
    };

    foreach (var index in indices)
    {
        var ingestionSource = new ElasticsearchSource(logger, config, index);
        await DataIngestor.IngestDataAsync(app.Services, ingestionSource);
    }
}
Console.WriteLine("Finished CosmosDB ingestion.");

app.Run();
