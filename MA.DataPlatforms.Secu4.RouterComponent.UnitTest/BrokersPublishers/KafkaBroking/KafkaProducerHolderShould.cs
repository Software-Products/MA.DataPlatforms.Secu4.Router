// <copyright file="KafkaProducerHolderShould.cs" company="McLaren Applied Ltd.">
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

using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions.Broking.KafkaBroking;
using MA.DataPlatforms.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

using NSubstitute;

using Xunit;

namespace MA.DataPlatforms.Secu4.RouterComponent.UnitTest.BrokersPublishers.KafkaBroking;

public class KafkaProducerHolderShould
{
    const string Route = "test";
    const string DeadLetterTopic = "dead_letter";
    private readonly IRouteRepository routeRepository;
    private readonly IRouteManager routeManager;
    private readonly IKafkaProducer producer;
    private readonly KafkaProducerHolder kafkaProducerHolder;
    private readonly RoutingDataPacket routingDataPacket;

    public KafkaProducerHolderShould()
    {
        this.routeRepository = Substitute.For<IRouteRepository>();
        this.routeManager = Substitute.For<IRouteManager>();
        this.producer = Substitute.For<IKafkaProducer>();
        var routingConfigurationProvider = Substitute.For<IRoutingConfigurationProvider>();

        routingConfigurationProvider.Provide().Returns(new RoutingConfiguration(new KafkaRoutingConfig(new KafkaPublishingConfig(), [], [], DeadLetterTopic)));
        this.kafkaProducerHolder = new KafkaProducerHolder(this.routeRepository, this.routeManager, this.producer, routingConfigurationProvider);

        this.routingDataPacket = new RoutingDataPacket([], Route, DateTime.UtcNow);
    }

    [Fact]
    public void Call_Producer_Dispose_Upon_Dispose()
    {
        // arrange done in ctor

        //act
        this.kafkaProducerHolder.Dispose();

        //assert
        this.producer.Received(1).Dispose();
    }

    [Fact]
    public void Call_Producer_And_CheckRoute_Initiate_Upon_Initiate()
    {
        //act
        this.kafkaProducerHolder.Initiate();

        //assert
        this.producer.Received(1).Initiate();
        this.routeManager.Received(1).CheckRoutes();
    }

    [Fact]
    public void Call_Methods_Produce_Of_Producer()
    {
        // arrange
        const string TopicName = "sample_topic";
        var route = new KafkaRoute(Route, TopicName);
        this.routeRepository.Find(Route).Returns(route);

        //act
        this.kafkaProducerHolder.Publish(this.routingDataPacket);

        //assert
        this.producer.Received(1).Produce(this.routingDataPacket.Message, TopicName, route.Partition, this.routingDataPacket.Key);
        this.routeRepository.Received(1).Find(Route);
    }
}
