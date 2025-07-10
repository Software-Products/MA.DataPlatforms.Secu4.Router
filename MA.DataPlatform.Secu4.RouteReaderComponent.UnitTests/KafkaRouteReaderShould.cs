// <copyright file="KafkaRouteReaderShould.cs" company="McLaren Applied Ltd.">
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

using MA.DataPlatforms.Secu4.RouteReaderComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

using NSubstitute;

namespace MA.DataPlatforms.Secu4.RouteReaderComponent.UnitTests;

public class KafkaRouteReaderShould
{
    private readonly IKafkaReaderFactory readerFactory = Substitute.For<IKafkaReaderFactory>();

    [Fact]
    public void Raise_Event_On_Listener_MessageReceived_Event()
    {
        //arrange
        var configurationProvider = Substitute.For<IConsumingConfigurationProvider>();
        var routeManager = Substitute.For<IRouteManager>();
        var routeReader = new KafkaRouteReader(this.readerFactory, configurationProvider, routeManager);
        var listenMessageReceived = false;
        const string TopicName = "test";
        var kafkaRoute = new KafkaRoute(TopicName, TopicName);
        var reader = Substitute.For<IKafkaReader>();

        configurationProvider.Provide().Returns(
            new ConsumingConfiguration(
            [
                new KafkaConsumingConfig(
                    new KafkaListeningConfig(),
                    kafkaRoute,
                    new KafkaTopicMetaData(TopicName))
            ]));
        this.readerFactory.Create().Returns(reader);
        routeReader.PacketReceived += (_, e) =>
        {
            if (e.Route == kafkaRoute.Name)
            {
                listenMessageReceived = true;
            }
        };

        reader.When(i => i.StartListening(kafkaRoute)).Do(
            _ => reader.MessageReceived += Raise.Event<EventHandler<RoutingDataPacket>>(
                this,
                new RoutingDataPacket(
                    [
                        1, 2, 3
                    ],
                    kafkaRoute.Name,
                    DateTime.UtcNow)));

        //act
        routeReader.StartReading();

        //assert
        listenMessageReceived.Should().BeTrue();
    }
}
