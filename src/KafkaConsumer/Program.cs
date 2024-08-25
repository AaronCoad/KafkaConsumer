using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;

Console.WriteLine("Hello, World!");

ConsumerConfig config = new ConsumerConfig()
{
    BootstrapServers = "localhost:9092",
    GroupId = "consumer",
    EnableAutoCommit = false,
    EnableAutoOffsetStore = false,
    AutoOffsetReset = AutoOffsetReset.Earliest
};
var deserializer = new JsonDeserializer<Dictionary<string, object>>().AsSyncOverAsync();
ConsumerBuilder<string, Dictionary<string, object>> consumerBuilder = new ConsumerBuilder<string, Dictionary<string, object>>(config).SetValueDeserializer(deserializer);
Repository repository = new Repository("AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==");
using (var consumer = consumerBuilder.Build())
{
    consumer.Subscribe("names5");
    List<Dictionary<string, object>> items = new List<Dictionary<string, object>>();
    while (true)
    {
        for (int i = 0; i < 500; i++)
        {
            var res = consumer.Consume(5000);
            if (res == null)
            {
                Console.WriteLine("No message, exiting");
                break;
            }
            //Console.WriteLine(res.TopicPartitionOffset);
            //Console.WriteLine(res.Message.Value);
            items.Add(res.Message.Value);
            consumer.StoreOffset(res);
        }
        var r = repository.Upsert(items).Result;
        if (r && items.Count > 0)
        {
            consumer.Commit();
            items.Clear();
        }
        Console.WriteLine("Round complete!");
        Thread.Sleep(1000);
        //consumer.Commit();
    }
}
Console.ReadLine();

public class Model
{
    public int id { get; set; }
    public string Name { get; set; }
}