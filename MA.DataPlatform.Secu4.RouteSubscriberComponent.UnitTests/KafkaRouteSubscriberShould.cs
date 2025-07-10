// <copyright file="KafkaRouteSubscriberShould.cs" company="McLaren Applied Ltd.">
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

using MA.DataPlatforms.Secu4.RouteSubscriberComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

using NSubstitute;

namespace MA.DataPlatforms.Secu4.RouteSubscriberComponent.UnitTests;

public class KafkaRouteSubscriberShould
{
    private readonly IKafkaListenerFactory listenerFactory = Substitute.For<IKafkaListenerFactory>();

    [Fact]
    public void Raise_Event_On_Listener_MessageReceived_Event()
    {
        //arrange
        var configurationProvider = Substitute.For<IConsumingConfigurationProvider>();

        var routeManager = Substitute.For<IRouteManager>();
        var subscriber = new KafkaRouteSubscriber(this.listenerFactory, configurationProvider, routeManager);
        var listenMessageReceived = false;
        const string TopicName = "test";
        var kafkaRoute = new KafkaRoute(TopicName, TopicName);
        var listener = Substitute.For<IKafkaListener>();

        configurationProvider.Provide().Returns(
            new ConsumingConfiguration(
            [
                new KafkaConsumingConfig(
                    new KafkaListeningConfig(),
                    kafkaRoute,
                    new KafkaTopicMetaData(TopicName))
            ]));
        this.listenerFactory.Create().Returns(listener);
        subscriber.PacketReceived += (_, e) =>
        {
            if (e.Route == kafkaRoute.Name)
            {
                listenMessageReceived = true;
            }
        };

        listener.When(i => i.StartListening(kafkaRoute)).Do(
            _ => listener.MessageReceived += Raise.Event<EventHandler<RoutingDataPacket>>(
                this,
                new RoutingDataPacket(
                    [
                        1, 2, 3
                    ],
                    kafkaRoute.Name,
                    DateTime.UtcNow)));

        //act
        subscriber.Subscribe();

        //assert
        listenMessageReceived.Should().BeTrue();
    }

    [Fact]
    public void Call_Listener_Stop_On_Unsubscribe()
    {
        //arrange
        var configurationProvider = Substitute.For<IConsumingConfigurationProvider>();

        var routeManager = Substitute.For<IRouteManager>();
        var subscriber = new KafkaRouteSubscriber(this.listenerFactory, configurationProvider, routeManager);
        const string TopicName = "test";
        var kafkaRoute = new KafkaRoute(TopicName, TopicName);
        var listener = Substitute.For<IKafkaListener>();

        configurationProvider.Provide().Returns(
            new ConsumingConfiguration(
            [
                new KafkaConsumingConfig(
                    new KafkaListeningConfig(),
                    kafkaRoute,
                    new KafkaTopicMetaData(TopicName))
            ]));
        this.listenerFactory.Create().Returns(listener);
        subscriber.Subscribe();

        //act
        subscriber.Unsubscribe();

        //assert
        listener.Received(1).Stop();
    }

    [Fact]
    public void Not_Call_Listener_Stop_On_Unsubscribe_When_Not_Subscribed()
    {
        //arrange
        var configurationProvider = Substitute.For<IConsumingConfigurationProvider>();

        var routeManager = Substitute.For<IRouteManager>();
        var subscriber = new KafkaRouteSubscriber(this.listenerFactory, configurationProvider, routeManager);
        const string TopicName = "test";
        var kafkaRoute = new KafkaRoute(TopicName, TopicName);
        var listener = Substitute.For<IKafkaListener>();

        configurationProvider.Provide().Returns(
            new ConsumingConfiguration(
            [
                new KafkaConsumingConfig(
                    new KafkaListeningConfig(),
                    kafkaRoute,
                    new KafkaTopicMetaData(TopicName))
            ]));
        this.listenerFactory.Create().Returns(listener);

        //act
        subscriber.Unsubscribe();

        //assert
        listener.Received(0).Stop();
    }
}
