// <copyright file="KafkaTopicHelper.cs" company="Motion Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Core;

namespace MA.DataPlatforms.Secu4.KafkaMetadataComponent;

public class KafkaTopicHelper : IKafkaTopicHelper
{
    private readonly ILogger logger;

    public KafkaTopicHelper(ILogger logger)
    {
        this.logger = logger;
    }

    public IReadOnlyList<TopicInfo> GetInfoByTopicContains(string brokerUrl, string nameSubString, KafkaSecurityConfig? security = null)
    {
        var result = new List<TopicInfo>();
        var topicsMetadata = GetTopicsMetaDataByCondition(brokerUrl, t => t.Topic.Contains(nameSubString, StringComparison.OrdinalIgnoreCase), security);
        return this.ExtractTopicInfo(brokerUrl, topicsMetadata, result, security);
    }

    public IReadOnlyList<TopicInfo> GetInfoByTopicPrefix(string brokerUrl, string namePrefix, KafkaSecurityConfig? security = null)
    {
        var result = new List<TopicInfo>();
        var topicsMetadata = GetTopicsMetaDataByCondition(brokerUrl, t => t.Topic.StartsWith(namePrefix, StringComparison.OrdinalIgnoreCase), security);
        return this.ExtractTopicInfo(brokerUrl, topicsMetadata, result, security);
    }

    public IReadOnlyList<TopicInfo> GetInfoByTopicSuffix(string brokerUrl, string nameSuffix, KafkaSecurityConfig? security = null)
    {
        var result = new List<TopicInfo>();
        var topicsMetadata = GetTopicsMetaDataByCondition(brokerUrl, t => t.Topic.EndsWith(nameSuffix, StringComparison.OrdinalIgnoreCase), security);
        return this.ExtractTopicInfo(brokerUrl, topicsMetadata, result, security);
    }

    private static List<TopicMetadata> GetTopicsMetaDataByCondition(string brokerUrl, Func<TopicMetadata, bool> predicate, KafkaSecurityConfig? security)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = brokerUrl
        };
        KafkaSecurityApplier.Apply(config, security);

        using var adminClient = new AdminClientBuilder(config).Build();
        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        return metadata.Topics.Where(predicate).ToList();
    }

    private List<TopicInfo> ExtractTopicInfo(string brokerUrl, List<TopicMetadata> topicsMetadata, List<TopicInfo> result, KafkaSecurityConfig? security)
    {
        if (topicsMetadata.Count == 0)
        {
            return result;
        }

        var consumerConfig = new ConsumerConfig
        {
            GroupId = Guid.NewGuid().ToString(),
            BootstrapServers = brokerUrl,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            // Metadata-only consumer: it never consumes or commits, so keep the
            // group-coordinator machinery out of the picture entirely.
            EnableAutoCommit = false,
        };
        KafkaSecurityApplier.Apply(consumerConfig, security);

        using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig)
            .SetLogHandler((_, log) => KafkaClientLogRouter.Route(this.logger, "Kafka metadata consumer", log))
            .SetErrorHandler((_, error) => KafkaClientLogRouter.Route(this.logger, "Kafka metadata consumer", error))
            .Build();

        foreach (var topicMetadata in topicsMetadata)
        {
            result.AddRange(this.GetOffsetInfo(topicMetadata.Partitions, topicMetadata, consumer));
        }

        try
        {
            consumer.Close();
        }
        catch (KafkaException ex)
        {
            this.logger.Warning($"Closing the Kafka metadata consumer failed: {ex.Message}");
        }

        return result;
    }

    private List<TopicInfo> GetOffsetInfo(
        IEnumerable<PartitionMetadata> partitions,
        TopicMetadata topicMetadata,
        IConsumer<Ignore, Ignore> consumer)
    {
        var topicPartitions = partitions
            .Select(p => new TopicPartition(topicMetadata.Topic, new Partition(p.PartitionId)))
            .ToList();
        consumer.Assign(topicPartitions);
        var infos = new List<TopicInfo>();
        foreach (var tp in topicPartitions)
        {
            long high = 0;
            for (var attempt = 0; attempt < 3; attempt++)
            {
                try
                {
                    high = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(15)).High;
                    break;
                }
                catch (KafkaException ex)
                {
                    if (attempt == 2)
                    {
                        this.logger.Error(
                            $"Failed to query watermark offsets for {tp.Topic}[{tp.Partition.Value}] after 3 attempts, defaulting to 0: {ex.Message}");
                    }
                }
            }

            infos.Add(new TopicInfo(tp.Topic, tp.Partition.Value, high));
        }

        return infos;
    }
}
