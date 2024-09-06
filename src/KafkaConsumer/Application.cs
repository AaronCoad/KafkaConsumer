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

    public async Task Run(string topic, int numberOfConsumers = 1)
    {
        List<Consumer> consumers = new List<Consumer>(numberOfConsumers) { this.Consumer };
        List<Task> tasks = new List<Task>();
        consumers.ForEach(consumer =>
        {
            tasks.Add(Task.Run(() => consumer.Run(topic, Repository)));
        });

        await Task.WhenAll(tasks);
    }    
}