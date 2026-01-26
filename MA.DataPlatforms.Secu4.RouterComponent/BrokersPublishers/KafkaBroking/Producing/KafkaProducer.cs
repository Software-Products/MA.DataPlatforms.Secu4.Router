// <copyright file="KafkaProducer.cs" company="Motion Applied Ltd.">
//
// Copyright 2025 Motion Applied Ltd
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>

using Confluent.Kafka;

using MA.Common;
using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions.Broking.KafkaBroking;

namespace MA.DataPlatforms.Secu4.RouterComponent.BrokersPublishers.KafkaBroking.Producing;

public class KafkaProducer : IKafkaProducer
{
    private readonly ILogger logger;
    private readonly IRoutingConfigurationProvider routingConfigurationProvider;
    private readonly KafkaQueueFullRetryExecutor kafkaQueueFullRetryExecutor;
    private IProducer<string?, byte[]>? producer;
    private bool disposed;

    internal KafkaProducer(
        ILogger logger,
        IRoutingConfigurationProvider routingConfigurationProvider)
    {
        this.logger = logger;
        this.routingConfigurationProvider = routingConfigurationProvider;
        this.kafkaQueueFullRetryExecutor = new KafkaQueueFullRetryExecutor(logger, this);
    }

    public void Dispose()
    {
        this.Dispose(true);
        GC.SuppressFinalize(this);
    }

    public void Initiate()
    {
        var kafkaConfig = this.routingConfigurationProvider.Provide().KafkaRoutingConfig.KafkaPublishingConfig;
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = kafkaConfig.Server,
            CompressionType = (CompressionType)kafkaConfig.CompressionType,
            CompressionLevel = kafkaConfig.CompressionLevel,
            QueueBufferingMaxKbytes = kafkaConfig.QueueMaxSize > 0
                ? kafkaConfig.QueueMaxSize
                : null,
            QueueBufferingMaxMessages = kafkaConfig.QueueMaxItem > 0
                ? kafkaConfig.QueueMaxItem
                : null,
            LingerMs = kafkaConfig.Linger > 0 ? kafkaConfig.Linger : null
        };
        this.producer = new ProducerBuilder<string?, byte[]>(producerConfig).Build();
        this.logger.Debug(
            $"Initiating Connection to Kafka server: {kafkaConfig.Server} with SessionKey publishing is complete");
    }

    public void Produce(byte[] message, string topic, int? partition = null, string? key = null)
    {
        if (this.producer is null)
        {
            throw new BusinessException("the producer is not initialized");
        }

        try
        {
            if (partition.HasValue)
            {
                var topicPartition = new TopicPartition(topic, new Partition(partition.Value));
                this.producer.Produce(
                    topicPartition,
                    new Message<string?, byte[]>
                    {
                        Key = key,
                        Value = message
                    });
            }
            else
            {
                this.producer.Produce(
                    topic,
                    new Message<string?, byte[]>
                    {
                        Key = key,
                        Value = message
                    });
            }
        }
        catch (Exception ex)
            when (ex is ProduceException<Null, byte[]> or ProduceException<string, byte[]>)
        {
            if (((KafkaException)ex).Error.Code == ErrorCode.Local_QueueFull)
            {
                this.kafkaQueueFullRetryExecutor.Execute(message, topic, partition, key);
            }
        }
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!disposing)
        {
            return;
        }

        if (this.disposed)
        {
            return;
        }

        this.producer?.Flush();
        this.producer?.Dispose();
        this.disposed = true;
    }
}
