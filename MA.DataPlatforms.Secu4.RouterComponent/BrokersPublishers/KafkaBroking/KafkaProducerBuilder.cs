// <copyright file="KafkaProducerBuilder.cs" company="Motion Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions.Broking.KafkaBroking;
using MA.DataPlatforms.Secu4.RouterComponent.BrokersPublishers.KafkaBroking.Producing;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Shared.Core;

namespace MA.DataPlatforms.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;

public class KafkaProducerBuilder : IKafkaProducerHolderBuilder
{
    private readonly ILogger logger;
    private readonly IRoutingConfigurationProvider routingConfigurationProvider;
    private readonly IRouteManager routeManager;

    public KafkaProducerBuilder(
        ILogger logger,
        IRoutingConfigurationProvider routingConfigurationProvider,
        IRouteManager routeManager)
    {
        this.logger = logger;
        this.routingConfigurationProvider = routingConfigurationProvider;
        this.routeManager = routeManager;
    }

    public IKafkaProducerHolder Build()
    {
        var config = this.routingConfigurationProvider.Provide();
        
        var kafkaRoutes = config.KafkaRoutingConfig.KafkaRoutes;
        kafkaRoutes.Add(new KafkaRoute("dead-letter", config.KafkaRoutingConfig.DeadLetterTopic));
        var kafkaRouteRepository = new KafkaRouteRepository(kafkaRoutes);

        var kafkaTopicsMetaData = config.KafkaRoutingConfig.RoutesMetaData;
        kafkaTopicsMetaData.Add(new KafkaTopicMetaData(config.KafkaRoutingConfig.DeadLetterTopic));

        return new KafkaProducerHolder(
            kafkaRouteRepository,
            this.routeManager,
            new KafkaProducer(this.logger, this.routingConfigurationProvider),
            this.routingConfigurationProvider);
    }
}
