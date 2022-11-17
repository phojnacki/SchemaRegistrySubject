using Confluent.Kafka;
using System;
using com.mycompany;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;

class InductionProducer
{
    static void Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
        };
        
        var schemaRegistryConfig = new SchemaRegistryConfig();
        schemaRegistryConfig.Set("schema.registry.url", "localhost:8082");
        var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        
        const string topic = "parcel-induction";

        using (var producer = new ProducerBuilder<string, MyRecord>(config)
                   .SetValueSerializer(new AvroSerializer<MyRecord>(
                       schemaRegistry,
                       new AvroSerializerConfig() { AutoRegisterSchemas = false, SubjectNameStrategy = SubjectNameStrategy.Record }).AsSyncOverAsync())
                   .Build())
        {
            var message = new MyRecord() { id = 1 };
            
            producer.Produce(topic, new Message<string, MyRecord> { Key = "1", Value = message },
            (deliveryReport) =>
            {
                if (deliveryReport.Error.Code != ErrorCode.NoError)
                {
                    Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                }
                else
                {
                    Console.WriteLine($"Produced event to topic {topic}.");
                }
            });

            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine($"1 message was produced to topic {topic}");
        }
    }
}