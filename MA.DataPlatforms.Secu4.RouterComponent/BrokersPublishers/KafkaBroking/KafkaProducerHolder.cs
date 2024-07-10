// <copyright file="KafkaProducerHolder.cs" company="McLaren Applied Ltd.">
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

using MA.DataPlatform.Secu4.RouterComponent.Abstractions;
using MA.DataPlatform.Secu4.RouterComponent.Abstractions.Broking.KafkaBroking;
using MA.DataPlatform.Secu4.Routing.Contracts;
using MA.DataPlatform.Secu4.Routing.Shared.Abstractions;

namespace MA.DataPlatform.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;

public sealed class KafkaProducerHolder : IKafkaProducerHolder
{
    private readonly IRouteRepository routeRepository;
    private readonly IRouteManager routeManager;
    private readonly string deadLetterTopicName;

    public KafkaProducerHolder(
        IRouteRepository routeRepository,
        IRouteManager routeManager,
        IKafkaProducer producer,
        IRoutingConfigurationProvider routingConfigurationProvider)
    {
        this.routeRepository = routeRepository;
        this.routeManager = routeManager;
        this.Producer = producer;
        this.deadLetterTopicName = routingConfigurationProvider.Provide().KafkaRoutingConfig.DeadLetterTopic;
    }

    public IKafkaProducer Producer { get; }

    public void Dispose()
    {
        this.Producer.Dispose();
    }

    public void Initiate()
    {
        this.routeManager.CheckRoutes();
        this.Producer.Initiate();
    }

    public void Publish(RoutingDataPacket dataPacket)
    {
        if (this.routeRepository.Find(dataPacket.Route) is not KafkaRoute route)
        {
            var deadLetterKey = $"_{dataPacket.Route}:_{dataPacket.Key}";
            this.Producer.Produce(dataPacket.Message, this.deadLetterTopicName, null, deadLetterKey);
        }
        else
        {
            this.Producer.Produce(dataPacket.Message, route.Topic, route.Partition, dataPacket.Key);
        }
    }
}
