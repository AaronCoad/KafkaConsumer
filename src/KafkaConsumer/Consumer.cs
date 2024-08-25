using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.SyncOverAsync;
using Microsoft.Extensions.Configuration;

public class Consumer
{
    ConsumerBuilder<string, Dictionary<string, object>> ConsumerBuilder { get; set; }
    ConsumerConfig consumerConfig { get; set; }

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
}