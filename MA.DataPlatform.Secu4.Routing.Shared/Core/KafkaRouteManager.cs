// <copyright file="KafkaRouteManager.cs" company="Motion Applied Ltd.">
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
using Confluent.Kafka.Admin;

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

namespace MA.DataPlatforms.Secu4.Routing.Shared.Core;

public class KafkaRouteManager : IRouteManager
{
    private static readonly SemaphoreSlim Semaphore = new(1, 1);
    private readonly Dictionary<string, IAdminClient> adminClients = new();
    private readonly HashSet<(string Url, string Topic)> processedTopic = [];
    private readonly ILogger logger;

    public KafkaRouteManager(ILogger logger)
    {
        this.logger = logger;
    }

    public void CheckRoutes(KafkaRoutingManagementInfo routingManagementInfo)
    {
        var autoResetEvent = new AutoResetEvent(false);
        Task.Run(() => this.StartChecking(routingManagementInfo, autoResetEvent));
        autoResetEvent.WaitOne();
    }

    private async Task StartChecking(KafkaRoutingManagementInfo routingManagementInfo, AutoResetEvent autoResetEvent)
    {
        await Semaphore.WaitAsync();
        try
        {
            if (!this.adminClients.TryGetValue(routingManagementInfo.Url, out var adminClient))
            {
                var adminConfig = new AdminClientConfig
                {
                    BootstrapServers = routingManagementInfo.Url
                };
                adminClient = new AdminClientBuilder(adminConfig).Build();
                this.adminClients.Add(routingManagementInfo.Url, adminClient);
            }

            var configRouteInfos = routingManagementInfo.Routes.GroupJoin(
                routingManagementInfo.MetaData,
                router => router.Topic,
                metaData => metaData.Topic,
                (o, i) => new KafkaConfigRouteInfo(o, i.ToList())).ToList();

            this.ValidateConfigs(configRouteInfos);
            foreach (var configuredRoute in configRouteInfos)
            {
                if (this.processedTopic.Contains((routingManagementInfo.Url, configuredRoute.Route.Topic)))
                {
                    continue;
                }

                var metadata = this.adminClients[routingManagementInfo.Url].GetMetadata(TimeSpan.FromSeconds(3));
                var foundInfo = metadata.Topics.FirstOrDefault(i => i.Topic == configuredRoute.Route.Topic);
                if (foundInfo is not null)
                {
                    this.CheckConsistency(foundInfo, configuredRoute);
                    this.logger.Info($"topic {configuredRoute.Route.Topic} already exist");
                }
                else
                {
                    await this.CreateTopic(routingManagementInfo.Url, configuredRoute);
                }

                this.processedTopic.Add((routingManagementInfo.Url, configuredRoute.Route.Topic));
            }
        }
        catch (Exception ex)
        {
            this.logger.Error(ex.ToString());
        }
        finally
        {
            autoResetEvent.Set();
            Semaphore.Release();
        }
    }

    private async Task CreateTopic(string url, KafkaConfigRouteInfo kafkaConfigRouteInfo)
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

            this.logger.Info($"creating topic {topicSpecification.Name}:{topicSpecification.NumPartitions}({topicSpecification.ReplicationFactor})");

            await this.adminClients[url].CreateTopicsAsync(
            [
                topicSpecification
            ]);

            this.logger.Info($"topic {topicSpecification.Name}:{topicSpecification.NumPartitions}({topicSpecification.ReplicationFactor}) created");
        }
        catch (Exception ex)
        {
            if (ex.Message.Contains("already exists"))
            {
                this.logger.Info($"topic {kafkaConfigRouteInfo.Route.Topic} already exist");
            }
            else
            {
                this.logger.Error(ex.ToString());
            }
        }
    }

    private void ValidateConfigs(IReadOnlyList<KafkaConfigRouteInfo> joinedTopicInfo)
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
            this.logger.Info(
                $"The replication factor for topic:{configRouteInfo.Route.Topic} is not based on expectation. expected:{configRouteInfo.MetaData[0].ReplicationFactor ?? 1} actual:{replicasLength}");
        }
    }
}
