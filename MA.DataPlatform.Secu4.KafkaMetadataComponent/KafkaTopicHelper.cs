// <copyright file="KafkaTopicHelper.cs" company="McLaren Applied Ltd.">
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

namespace MA.DataPlatform.Secu4.KafkaMetadataComponent;

public class KafkaTopicHelper : IKafkaTopicHelper
{
    public IReadOnlyList<TopicInfo> GetInfoByTopicContains(string brokerUrl, string nameSubString)
    {
        var result = new List<TopicInfo>();
        var topicsMetadata = GetTopicsMetaDataContainsSubString(brokerUrl, nameSubString);
        return ExtractTopicInfo(brokerUrl, topicsMetadata, result);
    }

    public IReadOnlyList<TopicInfo> GetInfoByTopicPrefix(string brokerUrl, string namePrefix)
    {
        var result = new List<TopicInfo>();
        var topicsMetadata = GetTopicsMetaDataWithPrefix(brokerUrl, namePrefix);
        return ExtractTopicInfo(brokerUrl, topicsMetadata, result);
    }

    public IReadOnlyList<TopicInfo> GetInfoByTopicSuffix(string brokerUrl, string nameSuffix)
    {
        var result = new List<TopicInfo>();
        var topicsMetadata = GetTopicsMetaDataWithSuffix(brokerUrl, nameSuffix);
        return ExtractTopicInfo(brokerUrl, topicsMetadata, result);
    }

    private static IReadOnlyList<TopicInfo> ExtractTopicInfo(string brokerUrl, List<TopicMetadata> topicsMetadata, List<TopicInfo> result)
    {
        foreach (var topicMetadata in topicsMetadata)
        {
            var partitions = topicMetadata.Partitions;
            using var consumer = new ConsumerBuilder<Ignore, Ignore>(
                new ConsumerConfig
                {
                    GroupId = Guid.NewGuid().ToString(),
                    BootstrapServers = brokerUrl,
                    AutoOffsetReset = AutoOffsetReset.Earliest
                }).Build();

            result.AddRange(GeOffsetInfo(partitions, topicMetadata, consumer));
        }

        return result;
    }

    private static List<TopicMetadata> GetTopicsMetaDataContainsSubString(string brokerUrl, string nameSubString)
    {
        return GetTopicsMetaDataByCondition(
            brokerUrl,
            (t)
                => t.Topic.Contains(nameSubString, StringComparison.OrdinalIgnoreCase));
    }

    private static List<TopicMetadata> GetTopicsMetaDataByCondition(string brokerUrl, Func<TopicMetadata, bool> predicate)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = brokerUrl
        };

        using var adminClient = new AdminClientBuilder(config).Build();
        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        var topicMetadata = metadata.Topics
            .Where(predicate)
            .ToList();
        return topicMetadata;
    }

    private static List<TopicMetadata> GetTopicsMetaDataWithPrefix(string brokerUrl, string namePrefix)
    {
        return GetTopicsMetaDataByCondition(
            brokerUrl,
            (t)
                => t.Topic.StartsWith(namePrefix, StringComparison.OrdinalIgnoreCase));
    }

    private static List<TopicMetadata> GetTopicsMetaDataWithSuffix(string brokerUrl, string namePostfix)
    {
        return GetTopicsMetaDataByCondition(
            brokerUrl,
            (t)
                => t.Topic.EndsWith(namePostfix, StringComparison.OrdinalIgnoreCase));
    }

    private static IEnumerable<TopicInfo> GeOffsetInfo(
        IEnumerable<PartitionMetadata> partitions,
        TopicMetadata topicMetadata,
        IConsumer<Ignore, Ignore> consumer)
    {
        var topicPartitions = partitions.Select(partition => new TopicPartition(topicMetadata.Topic, new Partition(partition.PartitionId))).ToList();
        consumer.Assign(topicPartitions);

        return (from topicPartition in topicPartitions
            let endOffsets = consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(10))
            select new TopicInfo(topicPartition.Topic, topicPartition.Partition.Value, endOffsets.High)).ToList();
    }
}
