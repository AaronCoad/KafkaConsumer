using System.Net.Http.Headers;
using Azure.Core.Pipeline;
using Confluent.Kafka;

public class Application
{
    Consumer Consumer { get; set; }
    Repository Repository { get; set; }

    public Application(Consumer consumer, Repository repository)
    {
        Consumer = consumer;
        Repository = repository;
    }

    public void Run(string topic)
    {
        using (var consumer = Consumer.Build())
        {
            consumer.Subscribe(topic);
            List<Dictionary<string, object>> items = new List<Dictionary<string, object>>();
            while (true)
            {
                GenerateList(consumer, items);
                StoreMessages(consumer, items);
            }
        }
    }

    private void StoreMessages(IConsumer<string, Dictionary<string, object>> consumer, List<Dictionary<string, object>> items)
    {
        int retryCount = 0;
        while (items.Count > 0 && retryCount < 5)
        {
            Thread.Sleep(1000);
            retryCount++;

            if (Repository.Upsert(items).Result == false)
                break;

            consumer.Commit();
            items.Clear();
        }
    }

    private static void GenerateList(IConsumer<string, Dictionary<string, object>> consumer, List<Dictionary<string, object>> items)
    {
        for (int i = 0; i < 500; i++)
        {
            var res = consumer.Consume(5000);
            if (res == null)
                break;

            items.Add(res.Message.Value);
            consumer.StoreOffset(res);
        }
    }
}