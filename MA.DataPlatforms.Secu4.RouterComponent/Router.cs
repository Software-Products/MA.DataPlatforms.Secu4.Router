// <copyright file="Router.cs" company="McLaren Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions.Broking;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions.Broking.KafkaBroking;
using MA.DataPlatforms.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;
using MA.DataPlatforms.Secu4.Routing.Contracts;

namespace MA.DataPlatforms.Secu4.RouterComponent;

public class Router : IRouter
{
    private readonly ILogger logger;

    private readonly IKafkaProducerHolderBuilder kafkaProducerBuilder;
    private IBrokerPublisher brokerPublisher;

    public Router(ILogger logger, IKafkaProducerHolderBuilder kafkaProducerBuilder)
    {
        this.logger = logger;
        this.kafkaProducerBuilder = kafkaProducerBuilder;
        this.brokerPublisher = new EmptyBrokerPublisher(logger);
    }

    public void Initiate()
    {
        var kafkaProducerHolder = this.kafkaProducerBuilder.Build();
        this.brokerPublisher = new KafkaBrokerPublisher(this.logger, kafkaProducerHolder);
        this.brokerPublisher.Initiate();
    }

    public void Route(RoutingDataPacket dataPacket)
    {
        this.brokerPublisher.Publish(dataPacket);
    }

    public void ShutDown()
    {
        this.brokerPublisher.Shutdown();
    }
}
