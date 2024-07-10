// <copyright file="KafkaConsumer.cs" company="McLaren Applied Ltd.">
//
// Copyright 2024 McLaren Applied Ltd
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
using MA.DataPlatform.Secu4.Routing.Contracts;
using MA.DataPlatform.Secu4.Routing.Contracts.Abstractions;
using MA.DataPlatform.Secu4.Routing.Shared.Abstractions;
using MA.DataPlatform.Secu4.Routing.Shared.Core;

namespace MA.DataPlatform.Secu4.Routing.Shared.Consuming;

public abstract class KafkaConsumer : IKafkaConsumer
{
    private readonly IConsumingConfigurationProvider consumingConfigurationProvider;
    private readonly ICancellationTokenSourceProvider cancellationTokenSourceProvider;
    protected IConsumer<string?, byte[]>? Consumer;
    private CancellationTokenSource? cancellationTokenSource;
    private bool stopped = false;

    protected KafkaConsumer(IConsumingConfigurationProvider consumingConfigurationProvider, ICancellationTokenSourceProvider cancellationTokenSourceProvider)
    {
        this.consumingConfigurationProvider = consumingConfigurationProvider;
        this.cancellationTokenSourceProvider = cancellationTokenSourceProvider;
    }

    public event EventHandler<RoutingDataPacket>? MessageReceived;

    public IRoute Route { get; private set; } = new EmptyRoute();

    public void Dispose()
    {
        this.Dispose(true);
        GC.SuppressFinalize(this);
    }

    public void StartListening(IKafkaRoute kafkaRoute)
    {
        this.Route = kafkaRoute;
        var subscriptionConfiguration = this.consumingConfigurationProvider.Provide();
        var kafkaConfiguration = subscriptionConfiguration.KafkaConsumingConfigs.FirstOrDefault(i => i.KafkaRoutes.Name == kafkaRoute.Name);

        if (kafkaConfiguration != null)
        {
            var kafkaBrokerConfig = new ConsumerConfig
            {
                BootstrapServers = kafkaConfiguration.KafkaListeningConfig.Server,
                GroupId = kafkaConfiguration.KafkaListeningConfig.GroupId,
                AutoOffsetReset = GetTheOffsetMode(kafkaConfiguration.KafkaListeningConfig.AutoOffsetResetMode),
            };

            this.Consumer = new ConsumerBuilder<string?, byte[]>(kafkaBrokerConfig).Build();

            var lstPartitions = new List<TopicPartition>();
            if (kafkaRoute.Partition.HasValue)
            {
                var topicPartition = new TopicPartition(kafkaRoute.Topic, kafkaRoute.Partition.Value);
                lstPartitions.Add(topicPartition);
            }
            else
            {
                lstPartitions = GetPartitions(kafkaConfiguration, kafkaRoute.Topic);
            }

            this.cancellationTokenSource = this.cancellationTokenSourceProvider.Provide();
            var token = this.cancellationTokenSource.Token;

            _ = Task.Run(() => this.StartReadingPartitions(kafkaConfiguration, lstPartitions, token), token);
        }
        else
        {
            throw new BusinessException("the kafkaRoute configuration not found for defined kafkaRoute name");
        }
    }

    public void Stop()
    {
        this.stopped = true;
        this.cancellationTokenSource?.Cancel();
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            this.Consumer?.Dispose();
        }
    }

    protected abstract void StartReadingPartitions(KafkaConsumingConfig kafkaConfiguration, IReadOnlyList<TopicPartition> lstPartitions, CancellationToken token);

    protected void OnMessagedReceived(RoutingDataPacket routingDataPacket)
    {
        if (this.stopped)
        {
            return;
        }

        this.MessageReceived?.Invoke(this, routingDataPacket);
    }

    protected static Offset GetOffset(KafkaConsumingConfig kafkaConfiguration)
    {
        return (kafkaConfiguration.KafkaListeningConfig.Offset ?? 0) >= 0 ? new Offset(kafkaConfiguration.KafkaListeningConfig.Offset ?? 0) : Offset.End;
    }

    private static List<TopicPartition> GetPartitions(KafkaConsumingConfig kafkaConfiguration, string topic)
    {
        var adminClientConfig = new AdminClientConfig
        {
            BootstrapServers = kafkaConfiguration.KafkaListeningConfig.Server
        };
        var adminClient = new AdminClientBuilder(adminClientConfig).Build();
        var metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(10));
        return metadata.Topics[0].Partitions.Select(p => new TopicPartition(topic, new Partition(p.PartitionId))).ToList();
    }

    private static AutoOffsetReset GetTheOffsetMode(AutoOffsetResetMode configurationAutoOffsetResetMode)
    {
        return configurationAutoOffsetResetMode == AutoOffsetResetMode.Earliest ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest;
    }
}
