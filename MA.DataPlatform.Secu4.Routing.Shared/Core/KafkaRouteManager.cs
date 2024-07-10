// <copyright file="KafkaRouteManager.cs" company="McLaren Applied Ltd.">
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
using Confluent.Kafka.Admin;

using MA.Common.Abstractions;
using MA.DataPlatform.Secu4.Routing.Contracts;
using MA.DataPlatform.Secu4.Routing.Contracts.Abstractions;
using MA.DataPlatform.Secu4.Routing.Shared.Abstractions;

namespace MA.DataPlatform.Secu4.Routing.Shared.Core;

public class KafkaRouteManager : IRouteManager
{
    private readonly ILogger logger;
    private readonly IBrokerUrlProvider brokerUrlProvider;
    private readonly IKafkaRouteRepository kafkaRouteRepository;
    private readonly IKafkaTopicMetaDataRepository kafkaTopicMetaDataRepository;

    private IAdminClient? adminClient;

    public KafkaRouteManager(
        ILogger logger,
        IBrokerUrlProvider brokerUrlProvider,
        IKafkaRouteRepository kafkaRouteRepository,
        IKafkaTopicMetaDataRepository kafkaTopicMetaDataRepository)
    {
        this.logger = logger;
        this.brokerUrlProvider = brokerUrlProvider;
        this.kafkaRouteRepository = kafkaRouteRepository;
        this.kafkaTopicMetaDataRepository = kafkaTopicMetaDataRepository;
    }

    public void CheckRoutes()
    {
        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = this.brokerUrlProvider.Provide()
        };
        this.adminClient = new AdminClientBuilder(adminConfig).Build();
        var kafkaRouteMetaData = this.kafkaTopicMetaDataRepository.GetAllMetaData().ToList();
        var kafkaRoutes = this.kafkaRouteRepository.GetRoutes().Cast<IKafkaRoute>().ToList();

        var configRouteInfos = kafkaRoutes.GroupJoin(
            kafkaRouteMetaData,
            router => router.Topic,
            metaData => metaData.Topic,
            (o, i) => new KafkaConfigRouteInfo(o, i.ToList())).ToList();

        this.ValidateConfigs(configRouteInfos);
        try
        {
            var topicMetadata = this.adminClient?.GetMetadata(TimeSpan.FromSeconds(5));
            foreach (var configuredRoute in configRouteInfos)
            {
                var foundInfo = topicMetadata?.Topics.Find(t => t.Topic.Equals(configuredRoute.Route.Name));
                if (foundInfo is not null)
                {
                    this.CheckConsistency(foundInfo, configuredRoute);
                }
                else
                {
                    this.CreateTopic(configuredRoute);
                }
            }
        }
        catch (Exception ex)
        {
            this.logger.Error(ex.ToString());
        }
    }

    private void CreateTopic(KafkaConfigRouteInfo kafkaConfigRouteInfo)
    {
        try
        {
            var kafkaTopicMetaData = kafkaConfigRouteInfo.MetaData[0];
            var topicSpecification = new TopicSpecification
            {
                Name = kafkaTopicMetaData.Topic,
                NumPartitions = kafkaTopicMetaData.NumberOfPartitions ?? -1,
                ReplicationFactor = kafkaTopicMetaData.ReplicationFactor ?? -1
            };
            this.adminClient?.CreateTopicsAsync(
                new[]
                {
                    topicSpecification
                }).Wait();
        }
        catch (AggregateException ex)
        {
            if (ex.InnerException?.Message.Contains("already exists") ?? false)
            {
                this.logger.Warning($"topic {kafkaConfigRouteInfo.Route.Topic} already exist");
            }
            else
            {
                this.logger.Error(ex.ToString());
            }
        }
    }

    private void ValidateConfigs(IReadOnlyCollection<KafkaConfigRouteInfo> joinedTopicInfo)
    {
        foreach (var inconsistentDataInNumber in joinedTopicInfo.Where(i => i.MetaData.Count > 1))
        {
            this.logger.Warning($"there is multiple metadata for one topic which should be at most one. topic:{inconsistentDataInNumber.Route.Topic}");
        }

        foreach (var foundMetaData in joinedTopicInfo.Where(i => i.MetaData.Count == 1)
                     .Where(foundMetaData => (foundMetaData.MetaData[0].NumberOfPartitions ?? 1) < (foundMetaData.Route.Partition ?? 1)))
        {
            var routeMetaData = foundMetaData.MetaData[0];
            this.logger.Warning(
                $"the partition defined in the route is not consistent with the number of partitions in the metadata. route:{foundMetaData.Route.Name} metadata-number-of-partitions:{routeMetaData.NumberOfPartitions} route-partition:{foundMetaData.Route.Partition} ");
        }

        foreach (var foundMetaData in joinedTopicInfo.Where(i => !i.MetaData.Any()))
        {
            foundMetaData.MetaData.Add(new KafkaTopicMetaData(foundMetaData.Route.Topic));
        }
    }

    private void CheckConsistency(TopicMetadata foundInfo, KafkaConfigRouteInfo configRouteInfo)
    {
        var partitionsCount = foundInfo.Partitions.Count;
        if ((configRouteInfo.MetaData[0].NumberOfPartitions ?? 1) != partitionsCount)
        {
            this.logger.Warning(
                $"The partition number for topic:{configRouteInfo.Route.Topic} is not based on expectation. expected:{configRouteInfo.MetaData[0].NumberOfPartitions ?? 1} actual:{partitionsCount}");
        }

        var replicasLength = foundInfo.Partitions[0].Replicas.Length;
        if ((configRouteInfo.MetaData[0].ReplicationFactor ?? 1) != replicasLength)
        {
            this.logger.Warning(
                $"The replication factor for topic:{configRouteInfo.Route.Topic} is not based on expectation. expected:{configRouteInfo.MetaData[0].ReplicationFactor ?? 1} actual:{replicasLength}");
        }
    }
}
