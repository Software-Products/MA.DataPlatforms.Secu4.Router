// <copyright file="KafkaListenerShould.cs" company="Motion Applied Ltd.">
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

using FluentAssertions;

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouteSubscriberComponent.IntegrationTest.Kafka.Base;
using MA.DataPlatforms.Secu4.RouteSubscriberComponent.IntegrationTest.Kafka.Helper;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

using NSubstitute;

namespace MA.DataPlatforms.Secu4.RouteSubscriberComponent.IntegrationTest.Kafka.Tests;

[Collection(nameof(RunKafkaDockerComposeCollectionFixture))]
public class KafkaListenerShould
{
    private const string Server = "localhost:9094";
    private const int DataBytesLength = 20;
    private readonly KafkaHelperProducer kafkaProducer;
    private readonly IConsumingConfigurationProvider subscriberConfigurationProvider;

    private readonly CancellationTokenSource cancellationTokenSource = new();

    private readonly Dictionary<string, AutoResetEvent> autoResetEvents = [];
    private readonly Dictionary<string, byte[]?> results = [];
    private readonly Dictionary<string, string?> resultKeys = [];
    private readonly ICancellationTokenSourceProvider cancellationTokenSourceProvider;
    private readonly ILogger logger;

    public KafkaListenerShould(RunKafkaDockerComposeFixture dockerComposeFixture)
    {
        this.DockerComposeFixture = dockerComposeFixture;
        this.kafkaProducer = new KafkaHelperProducer(Server);
        this.subscriberConfigurationProvider = Substitute.For<IConsumingConfigurationProvider>();
        this.cancellationTokenSourceProvider = Substitute.For<ICancellationTokenSourceProvider>();
        this.logger = Substitute.For<ILogger>();
        this.cancellationTokenSourceProvider.Provide().Returns(this.cancellationTokenSource);
    }

    public RunKafkaDockerComposeFixture DockerComposeFixture { get; }

    [Fact]
    public void Producer_Produce_Data_With_key_and_Check_the_Receive_Data_To_See_If_Is_Same_As_Publishing_With_Earliest_Offset_Starting()
    {
        //arrange
        var kafkaListener = new KafkaListener(this.subscriberConfigurationProvider, this.cancellationTokenSourceProvider, this.logger);
        var topicName = Guid.NewGuid().ToString();
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topicName));
        var kafkaRoute = new KafkaRoute("test1", topicName);
        this.subscriberConfigurationProvider.Provide().Returns(
            new ConsumingConfiguration(
            [
                new KafkaConsumingConfig(
                    new KafkaListeningConfig(Server, Guid.NewGuid().ToString(), AutoOffsetResetMode.Earliest, null),
                    kafkaRoute,
                    new KafkaTopicMetaData(topicName))
            ]));
        kafkaListener.MessageReceived += this.KafkaListener_MessageReceived;

        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);

        var autoResetEvent = new AutoResetEvent(false);
        this.autoResetEvents.Add(kafkaRoute.Name, autoResetEvent);
        var key = "testKey" + Guid.NewGuid();

        //act
        this.kafkaProducer.Produce(data, kafkaRoute, key);
        kafkaListener.StartListening(kafkaRoute);

        //assert
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(30));
        this.cancellationTokenSource.Cancel();
        this.results[kafkaRoute.Name].Should().NotBeNull().And.HaveCount(DataBytesLength).And.ContainInOrder(data).And.ContainItemsAssignableTo<byte>();
        this.resultKeys[kafkaRoute.Name].Should().Be(key);
    }

    [Fact]
    public void Producer_Produce_Data_Without_key_and_Check_the_Receive_Data_To_See_If_Is_Same_As_Publishing()
    {
        //arrange
        var kafkaListener = new KafkaListener(this.subscriberConfigurationProvider, this.cancellationTokenSourceProvider, this.logger);
        var topicName = Guid.NewGuid().ToString();
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topicName));

        var kafkaRoute = new KafkaRoute("test2", topicName);

        for (var i = 0; i < 3; i++)
        {
            this.kafkaProducer.Produce([], kafkaRoute);
        }

        this.subscriberConfigurationProvider.Provide().Returns(
            new ConsumingConfiguration(
            [
                new KafkaConsumingConfig(
                    new KafkaListeningConfig(Server, Guid.NewGuid().ToString(), AutoOffsetResetMode.Latest, null),
                    kafkaRoute,
                    new KafkaTopicMetaData(topicName))
            ]));
        kafkaListener.MessageReceived += this.KafkaListener_MessageReceived;
        kafkaListener.StartListening(kafkaRoute);

        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);
        var autoResetEvent = new AutoResetEvent(false);
        this.autoResetEvents.Add(kafkaRoute.Name, autoResetEvent);
        Task.Delay(TimeSpan.FromMilliseconds(6000)).Wait();

        //act
        this.kafkaProducer.Produce(data, kafkaRoute);

        //assert
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(30));
        this.cancellationTokenSource.Cancel();
        this.results[kafkaRoute.Name].Should().NotBeNull().And.HaveCount(DataBytesLength).And.ContainInOrder(data).And.ContainItemsAssignableTo<byte>();
        this.resultKeys[kafkaRoute.Name].Should().Be(null);
    }

    [Fact]
    public void Producer_Produce_Data_With_Offset_and_Check_the_Receive_Data_To_See_If_Is_Same_As_Publishing()
    {
        //arrange
        var kafkaListener = new KafkaListener(this.subscriberConfigurationProvider, this.cancellationTokenSourceProvider, this.logger);
        var topicName = Guid.NewGuid().ToString();
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topicName));
        var kafkaRoute = new KafkaRoute("test3", topicName, 0);
        this.subscriberConfigurationProvider.Provide().Returns(
            new ConsumingConfiguration(
            [
                new KafkaConsumingConfig(
                    new KafkaListeningConfig(Server, Guid.NewGuid().ToString(), AutoOffsetResetMode.FromOffset, 0),
                    kafkaRoute,
                    new KafkaTopicMetaData(topicName))
            ]));
        kafkaListener.MessageReceived += this.KafkaListener_MessageReceived;
        kafkaListener.StartListening(kafkaRoute);

        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);

        var autoResetEvent = new AutoResetEvent(false);
        this.autoResetEvents.Add(kafkaRoute.Name, autoResetEvent);

        Task.Delay(TimeSpan.FromMilliseconds(5000)).Wait();

        //act
        this.kafkaProducer.Produce(data, kafkaRoute);

        //assert
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(30));
        this.cancellationTokenSource.Cancel();
        this.results[kafkaRoute.Name].Should().NotBeNull().And.HaveCount(DataBytesLength).And.ContainInOrder(data).And.ContainItemsAssignableTo<byte>();
        this.resultKeys[kafkaRoute.Name].Should().Be(null);
    }

    [Fact]
    public void Producer_Produce_Data_With_Offset_And_Check_The_Receive_Data_To_See_If_Only_Data_After_That_Offset_Is_Read()
    {
        //arrange
        var kafkaListener = new KafkaListener(this.subscriberConfigurationProvider, this.cancellationTokenSourceProvider, this.logger);
        var topicName = Guid.NewGuid().ToString();
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topicName));
        var kafkaRoute = new KafkaRoute("test4", topicName, 0);
        this.subscriberConfigurationProvider.Provide().Returns(
            new ConsumingConfiguration(
            [
                new KafkaConsumingConfig(
                    new KafkaListeningConfig(Server, Guid.NewGuid().ToString(), AutoOffsetResetMode.FromOffset, 10),
                    kafkaRoute,
                    new KafkaTopicMetaData(topicName))
            ]));
        var counter = 0;
        var autoResetEvent = new AutoResetEvent(false);
        var keys = new List<string?>();
        kafkaListener.MessageReceived += (_, e) =>
        {
            counter++;
            keys.Add(e.Key);
            if (counter == 5)
            {
                autoResetEvent.Set();
            }
        };

        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);

        Task.Delay(TimeSpan.FromMilliseconds(5000)).Wait();
        //act
        for (var i = 0; i < 15; i++)
        {
            this.kafkaProducer.Produce(data, kafkaRoute, i.ToString());
        }

        Task.Delay(TimeSpan.FromMilliseconds(5000)).Wait();
        kafkaListener.StartListening(kafkaRoute);

        //assert
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(30));
        keys.Should().BeEquivalentTo(
            new List<string>
            {
                "10",
                "11",
                "12",
                "13",
                "14"
            });
    }

    [Fact]
    public void NotDeliverDataAfterStopCalled()
    {
        //arrange
        var kafkaListener = new KafkaListener(this.subscriberConfigurationProvider, this.cancellationTokenSourceProvider, this.logger);
        var topicName = Guid.NewGuid().ToString();
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topicName));
        var kafkaRoute = new KafkaRoute("test3", topicName, 0);
        var counter = 0;
        this.subscriberConfigurationProvider.Provide().Returns(
            new ConsumingConfiguration(
            [
                new KafkaConsumingConfig(
                    new KafkaListeningConfig(Server, Guid.NewGuid().ToString(), AutoOffsetResetMode.FromOffset, 0),
                    kafkaRoute,
                    new KafkaTopicMetaData(topicName))
            ]));
        kafkaListener.MessageReceived += (s, e) =>
        {
            this.KafkaListener_MessageReceived(s, e);
            counter++;
        };
        kafkaListener.StartListening(kafkaRoute);

        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);

        var autoResetEvent = new AutoResetEvent(false);
        this.autoResetEvents.Add(kafkaRoute.Name, autoResetEvent);
        //act
        this.kafkaProducer.Produce(data, kafkaRoute);

        //assert
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(30));
        this.results[kafkaRoute.Name].Should().NotBeNull().And.HaveCount(DataBytesLength).And.ContainInOrder(data).And.ContainItemsAssignableTo<byte>();
        this.resultKeys[kafkaRoute.Name].Should().Be(null);
        counter.Should().Be(1);

        //arrange
        kafkaListener.Stop();
        var newAutoResetEvent = new AutoResetEvent(false);

        //act
        this.kafkaProducer.Produce(data, kafkaRoute);
        newAutoResetEvent.WaitOne(TimeSpan.FromSeconds(10));

        //assert
        counter.Should().Be(1);
    }

    private void KafkaListener_MessageReceived(object? sender, RoutingDataPacket e)
    {
        if (e.Message.Length != DataBytesLength)
        {
            return;
        }

        this.results.Add(e.Route, e.Message);
        this.resultKeys.Add(e.Route, e.Key);
        this.autoResetEvents[e.Route].Set();
    }
}
