// <copyright file="KafkaBrokerPublishersShould.cs" company="McLaren Applied Ltd.">
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
using MA.DataPlatform.Secu4.RouterComponent.Abstractions;
using MA.DataPlatform.Secu4.RouterComponent.Abstractions.Broking.KafkaBroking;
using MA.DataPlatform.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;
using MA.DataPlatform.Secu4.Routing.Contracts;

using NSubstitute;

using Xunit;

namespace MA.DataPlatform.Secu4.RouterComponent.UnitTest.BrokersPublishers.KafkaBroking;

public class KafkaBrokerPublishersShould
{
    private readonly ILogger logger;
    private readonly IKafkaProducerHolder kafkaProducerHolder;

    public KafkaBrokerPublishersShould()
    {
        this.logger = Substitute.For<ILogger>();
        this.kafkaProducerHolder = Substitute.For<IKafkaProducerHolder>();
        var routingConfigurationProvider = Substitute.For<IRoutingConfigurationProvider>();
        routingConfigurationProvider.Provide().Returns(new RoutingConfiguration(new KafkaRoutingConfig(new KafkaPublishingConfig(), [], [], "dead-letter")));
    }

    [Fact]
    public void Call_Expected_Logger_Methods_And_Producer_Holder_Initiate_Upon_Initiate()
    {
        //arrange done in constructor
        var kafkaBrokerPublisher = new KafkaBrokerPublisher(this.logger, this.kafkaProducerHolder);

        //act
        kafkaBrokerPublisher.Initiate();

        //assert
        this.logger.Received(1).Debug("Initiating the producer");
        this.logger.Received(1).Debug("initiation of the producer complete");
        this.kafkaProducerHolder.Received(1).Initiate();
    }

    [Fact]
    public void Call_Expected_Logger_Methods_And_Producer_Holder_Publish_Upon_Publish()
    {
        //arrange done in constructor
        var kafkaBrokerPublisher = new KafkaBrokerPublisher(this.logger, this.kafkaProducerHolder);
        var data = new RoutingDataPacket([], "test");

        //act
        kafkaBrokerPublisher.Publish(data);

        //assert
        this.logger.Received(1).Debug("publishing data");
        this.logger.Received(1).Debug("publishing data completed");
        this.kafkaProducerHolder.Received(1).Publish(data);
    }

    [Fact]
    public void Call_Expected_Logger_Methods_And_Producer_Holder_Dispose_Upon_Shutdown()
    {
        //arrange done in constructor
        var kafkaBrokerPublisher = new KafkaBrokerPublisher(this.logger, this.kafkaProducerHolder);

        //act
        kafkaBrokerPublisher.Shutdown();

        //assert
        this.logger.Received(1).Debug("shutting down the producer");
        this.logger.Received(1).Debug("the producer shutdown complete");
        this.kafkaProducerHolder.Received(1).Dispose();
    }
}
