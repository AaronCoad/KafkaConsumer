using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;

var globalConfig = new ConfigurationBuilder().AddJsonFile($"{Environment.CurrentDirectory}\\appsettings.json").Build();

Application application = new Application(new Consumer(globalConfig.GetSection("Kafka")), new Repository(globalConfig.GetSection("Cosmos")));
await application.Run(globalConfig.GetSection("Kafka")["Topic"]!);