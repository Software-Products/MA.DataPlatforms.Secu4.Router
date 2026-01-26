// <copyright file="KafkaRouter.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

namespace MA.DataPlatforms.Secu4.RouterComponent;

public class KafkaRouter : IRouter
{
    private readonly ILogger logger;
    private readonly IRouteReadingWritingComponentRepository<KafkaRouter> routerRepository;
    private readonly KafkaBrokerPublisher brokerPublisher;

    public KafkaRouter(ILogger logger, IKafkaProducerHolderBuilder kafkaProducerBuilder, IRouteReadingWritingComponentRepository<KafkaRouter> routerRepository, string id)
    {
        this.logger = logger;
        this.routerRepository = routerRepository;
        var kafkaProducerHolder = kafkaProducerBuilder.Build();
        this.brokerPublisher = new KafkaBrokerPublisher(this.logger, kafkaProducerHolder);
        this.Id = id;
        this.logger.Info($"Router created with ID: {this.Id}");
        this.routerRepository.Add(this);
    }

    public string Id { get; }

    public void Initiate()
    {
        this.brokerPublisher.Initiate();
        this.logger.Info($"Router initiated with ID: {this.Id}");
    }

    public void Route(RoutingDataPacket dataPacket)
    {
        this.brokerPublisher.Publish(dataPacket);
    }

    public void ShutDown()
    {
        this.brokerPublisher.Shutdown();
        this.logger.Info($"Router shutdown with ID: {this.Id}");
        this.routerRepository.Remove(this.Id);
    }
}
