using System.Text.Json;
using Microsoft.Azure.Cosmos;

public class Repository
{
    CosmosClient client { get; set; }

    public Repository(string connectionString)
    {
        client = new CosmosClient(connectionString, new CosmosClientOptions() { AllowBulkExecution = true, MaxRetryAttemptsOnRateLimitedRequests = 5 });
    }

    public async Task<bool> Upsert(List<Dictionary<string, object>> items)
    {
        Container container = client.GetDatabase("clients").GetContainer("names");
        List<Task> concurrentTasks = new List<Task>();
        try
        {
            foreach (var item in items)
            {
                item.Add("id", item["Id"].ToString());
                //var res = await container.CreateItemAsync(item, new PartitionKey(item["Name"].ToString()));
                //return true;
                concurrentTasks.Add(container.UpsertItemAsync(item, new PartitionKey(item["Name"].ToString())));
            }
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