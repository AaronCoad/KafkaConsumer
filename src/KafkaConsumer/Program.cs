using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;

var globalConfig = new ConfigurationBuilder().AddJsonFile($"{Environment.CurrentDirectory}\\appsettings.json").Build();
var kafkaConfig = globalConfig.GetSection("Kafka");
var cosmosConfig = globalConfig.GetSection("Cosmos");

Consumer consumer = new Consumer(kafkaConfig);
Repository repository = new Repository(cosmosConfig);
Application application = new Application(consumer, repository);
application.Run(kafkaConfig["Topic"]!);