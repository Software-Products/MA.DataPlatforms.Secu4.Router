// <copyright file="KafkaProducerBuilderShould.cs" company="McLaren Applied Ltd.">
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

using FluentAssertions;

using MA.Common.Abstractions;
using MA.DataPlatform.Secu4.RouterComponent.Abstractions;
using MA.DataPlatform.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;
using MA.DataPlatform.Secu4.RouterComponent.BrokersPublishers.KafkaBroking.Producing;
using MA.DataPlatform.Secu4.Routing.Contracts;

using NSubstitute;

using Xunit;

namespace MA.DataPlatform.Secu4.RouterComponent.UnitTest.BrokersPublishers.KafkaBroking;

public class KafkaProducerBuilderShould
{
    private readonly IRoutingConfigurationProvider routingConfigurationProvider;
    private readonly KafkaProducerBuilder kafkaProducerBuilder;

    public KafkaProducerBuilderShould()
    {
        var routerLogger = Substitute.For<ILogger>();

        this.routingConfigurationProvider = Substitute.For<IRoutingConfigurationProvider>();
        this.kafkaProducerBuilder = new KafkaProducerBuilder(routerLogger, this.routingConfigurationProvider);
    }

    [Fact]
    public void Return_Kafka_Producer_After_Build()
    {
        //arrange
        this.routingConfigurationProvider.Provide().Returns(
            new RoutingConfiguration(
                new KafkaRoutingConfig(
                    new KafkaPublishingConfig(),
                    [new KafkaRoute("test", "test")],
                    [new KafkaTopicMetaData("test")],
                    "dead-letter")));

        //act
        var kafkaProducer = this.kafkaProducerBuilder.Build();

        //assert
        kafkaProducer.Producer.GetType().Should().Be<KafkaProducer>();
    }
}
