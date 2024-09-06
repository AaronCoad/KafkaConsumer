using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.SyncOverAsync;
using Microsoft.Extensions.Configuration;
using System.Text;

public class Consumer
{
    ConsumerBuilder<string, Dictionary<string, object>> ConsumerBuilder { get; set; }
    ConsumerConfig consumerConfig { get; set; }
    private IConsumer<string, Dictionary<string, object>>? consumer { get; set; }

    public Consumer(IConfigurationSection configurationSection)
    {
        consumerConfig = new ConsumerConfig()
        {
            BootstrapServers = configurationSection["Broker"],
            GroupId = configurationSection["GroupId"],
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        var deserializer = new JsonDeserializer<Dictionary<string, object>>().AsSyncOverAsync();

        ConsumerBuilder = new ConsumerBuilder<string, Dictionary<string, object>>(consumerConfig).SetValueDeserializer(deserializer);
    }

    public IConsumer<string, Dictionary<string, object>> Build() => ConsumerBuilder.Build();

    public void Run(string topic, Repository repository)
    {
        this.consumer = this.Build();
        consumer.Subscribe(topic);
        Queue<Dictionary<string, object>> items = new Queue<Dictionary<string, object>>();
        while (true)
        {
            GenerateList(items);
            StoreMessages(items, repository);
        }
    }

    private void StoreMessages(Queue<Dictionary<string, object>> items, Repository repository)
    {
        int retryCount = 0;
        while (items.Count > 0 && retryCount < 5)
        {
            Thread.Sleep(1000);
            retryCount++;

            if (repository.Upsert(items).Result == false)
                break;

            consumer!.Commit();
            items.Clear();
        }
    }

    private void GenerateList(Queue<Dictionary<string, object>> items)
    {
        for (int i = 0; i < 500; i++)
        {
            var res = consumer!.Consume(5000);
            if (res == null)
                break;
            var m = res.Message.Value;
            items.Enqueue(m);
            consumer.StoreOffset(res);
        }
    }
}