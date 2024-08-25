using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;

public class Repository
{
    CosmosClient client { get; set; }
    Container container { get; set; }
    Database database { get; set; }

    public Repository(IConfigurationSection configurationSection)
    {
        client = new CosmosClient(configurationSection["ConnectionString"],
                                  new CosmosClientOptions()
                                  {
                                      AllowBulkExecution = true,
                                      MaxRetryAttemptsOnRateLimitedRequests = 5
                                  });
        database = client.GetDatabase(configurationSection["DatabaseId"]);
        container = database.GetContainer(configurationSection["Container"]);
    }

    public async Task<bool> Upsert(List<Dictionary<string, object>> items)
    {
        List<Task> concurrentTasks = new List<Task>();
        try
        {
            items.ForEach(item =>
            {
                item.Add("id", item["Id"].ToString()!);
                concurrentTasks.Add(container.UpsertItemAsync(item, new PartitionKey(item["Name"].ToString())));
            });
            await Task.WhenAll(concurrentTasks);
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            return false;
        }
    }
}