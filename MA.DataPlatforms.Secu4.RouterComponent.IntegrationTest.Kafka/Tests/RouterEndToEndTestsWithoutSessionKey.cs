// <copyright file="RouterEndToEndTestsWithoutSessionKey.cs" company="McLaren Applied Ltd.">
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

using System.Collections.Concurrent;

using Confluent.Kafka;

using FluentAssertions;

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;
using MA.DataPlatforms.Secu4.RouterComponent.IntegrationTest.Kafka.Base;
using MA.DataPlatforms.Secu4.RouterComponent.IntegrationTest.Kafka.Helper;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Core;

using NSubstitute;

using Xunit;

namespace MA.DataPlatforms.Secu4.RouterComponent.IntegrationTest.Kafka.Tests;

[Collection(nameof(RunKafkaDockerComposeCollectionFixture))]
public class RouterEndToEndTests
{
    private const string Server = "localhost:9092";
    private readonly ConcurrentDictionary<string, AutoResetEvent> autoResetEvents = [];
    private readonly ConcurrentDictionary<string, byte[]?> results = [];
    private readonly Dictionary<string, string?> resultKeys = [];

    public RouterEndToEndTests(RunKafkaDockerComposeFixture dockerComposeFixture)
    {
        this.DockerComposeFixture = dockerComposeFixture;
        Task.Delay(TimeSpan.FromMilliseconds(5000)).Wait();
    }

    public RunKafkaDockerComposeFixture DockerComposeFixture { get; }

    [Fact]
    public void Test_Stream_Send_Data_Without_Key()
    {
        //arrange
        var logger = Substitute.For<ILogger>();
        var routingConfigurationProvider = Substitute.For<IRoutingConfigurationProvider>();
        var topicName1 = Guid.NewGuid().ToString();
        var topicName2 = Guid.NewGuid().ToString();
        var topicName3 = Guid.NewGuid().ToString();
        var route1 = new KafkaRoute("route1", topicName1);
        var route2 = new KafkaRoute("route2", topicName2);
        var route3 = new KafkaRoute("route3", topicName3);
        var configuration = new RoutingConfiguration(
            new KafkaRoutingConfig(
                new KafkaPublishingConfig(),
                [route1, route2, route3],
                [new KafkaTopicMetaData(topicName1), new KafkaTopicMetaData(topicName2), new KafkaTopicMetaData(topicName3)],
                "dead-letter"));

        routingConfigurationProvider.Provide().Returns(configuration);
        var router = new Router(
            logger,
            new KafkaProducerBuilder(
                logger,
                routingConfigurationProvider,
                new KafkaRouteManager(logger)));

        using var cancellationTokenSource = new CancellationTokenSource();
        var token = cancellationTokenSource.Token;

        var data1 = new byte[20];
        var data2 = new byte[20];
        var data3 = new byte[20];

        var random = new Random();

        random.NextBytes(data1);
        random.NextBytes(data2);
        random.NextBytes(data3);

        var autoResetEvent1 = new AutoResetEvent(false);
        var autoResetEvent2 = new AutoResetEvent(false);
        var autoResetEvent3 = new AutoResetEvent(false);

        this.autoResetEvents.TryAdd(route1.Topic, autoResetEvent1);
        this.autoResetEvents.TryAdd(route2.Topic, autoResetEvent2);
        this.autoResetEvents.TryAdd(route3.Topic, autoResetEvent3);

        router.Initiate();

        // act
        router.Route(new RoutingDataPacket(data1, route1.Name, DateTime.UtcNow));
        router.Route(new RoutingDataPacket(data2, route2.Name, DateTime.UtcNow));
        router.Route(new RoutingDataPacket(data3, route3.Name, DateTime.UtcNow));

        var listener1 = CreateListener(route1.Topic, token);
        var listener2 = CreateListener(route2.Topic, token);
        var listener3 = CreateListener(route3.Topic, token);

        listener1.OnReceived += this.KafkaListener_OnReceived;
        listener2.OnReceived += this.KafkaListener_OnReceived;
        listener3.OnReceived += this.KafkaListener_OnReceived;

        _ = Task.Run(() => listener1.Start(), token);
        _ = Task.Run(() => listener2.Start(), token);
        _ = Task.Run(() => listener3.Start(), token);

        //assert
        autoResetEvent1.WaitOne(60000);
        autoResetEvent2.WaitOne(60000);
        autoResetEvent3.WaitOne(60000);

        this.results[route1.Topic].Should().NotBeNull().And.HaveCount(20).And.ContainInOrder(data1).And.ContainItemsAssignableTo<byte>();
        this.results[route2.Topic].Should().NotBeNull().And.HaveCount(20).And.ContainInOrder(data2).And.ContainItemsAssignableTo<byte>();
        this.results[route3.Topic].Should().NotBeNull().And.HaveCount(20).And.ContainInOrder(data3).And.ContainItemsAssignableTo<byte>();
    }

    [Fact]
    public void Test_Stream_Send_Data_With_Key()
    {
        //arrange
        var logger = Substitute.For<ILogger>();
        var routingConfigurationProvider = Substitute.For<IRoutingConfigurationProvider>();
        var topicName1 = Guid.NewGuid().ToString();
        var topicName2 = Guid.NewGuid().ToString();
        var topicName3 = Guid.NewGuid().ToString();
        var route1 = new KafkaRoute("route1", topicName1);
        var route2 = new KafkaRoute("route2", topicName2);
        var route3 = new KafkaRoute("route3", topicName3);
        var configuration = new RoutingConfiguration(
            new KafkaRoutingConfig(
                new KafkaPublishingConfig(),
                [route1, route2, route3],
                [new KafkaTopicMetaData(topicName1), new KafkaTopicMetaData(topicName2), new KafkaTopicMetaData(topicName3)],
                "dead-letter"));

        routingConfigurationProvider.Provide().Returns(configuration);
        var router = new Router(
            logger,
            new KafkaProducerBuilder(
                logger,
                routingConfigurationProvider,
                new KafkaRouteManager(logger)));

        using var cancellationTokenSource = new CancellationTokenSource();
        var token = cancellationTokenSource.Token;

        const string Key1 = "key1";
        const string Key2 = "key2";
        const string Key3 = "key3";

        var data1 = new byte[20];
        var data2 = new byte[20];
        var data3 = new byte[20];

        var random = new Random();

        random.NextBytes(data1);
        random.NextBytes(data2);
        random.NextBytes(data3);

        var autoResetEvent1 = new AutoResetEvent(false);
        var autoResetEvent2 = new AutoResetEvent(false);
        var autoResetEvent3 = new AutoResetEvent(false);

        this.autoResetEvents.TryAdd(route1.Topic, autoResetEvent1);
        this.autoResetEvents.TryAdd(route2.Topic, autoResetEvent2);
        this.autoResetEvents.TryAdd(route3.Topic, autoResetEvent3);

        router.Initiate();

        // act
        router.Route(new RoutingDataPacket(data1, route1.Name, DateTime.UtcNow, Key1));
        router.Route(new RoutingDataPacket(data2, route2.Name, DateTime.UtcNow, Key2));
        router.Route(new RoutingDataPacket(data3, route3.Name, DateTime.UtcNow, Key3));

        var listener1 = CreateListener(route1.Topic, token);
        var listener2 = CreateListener(route2.Topic, token);
        var listener3 = CreateListener(route3.Topic, token);

        listener1.OnReceived += this.KafkaListener_OnReceived;
        listener2.OnReceived += this.KafkaListener_OnReceived;
        listener3.OnReceived += this.KafkaListener_OnReceived;

        _ = Task.Run(() => listener1.Start(), token);
        _ = Task.Run(() => listener2.Start(), token);
        _ = Task.Run(() => listener3.Start(), token);

        //assert
        autoResetEvent1.WaitOne(60000);
        autoResetEvent2.WaitOne(60000);
        autoResetEvent3.WaitOne(60000);

        this.results[route1.Topic].Should().NotBeNull().And.HaveCount(20).And.ContainInOrder(data1).And.ContainItemsAssignableTo<byte>();
        this.results[route2.Topic].Should().NotBeNull().And.HaveCount(20).And.ContainInOrder(data2).And.ContainItemsAssignableTo<byte>();
        this.results[route3.Topic].Should().NotBeNull().And.HaveCount(20).And.ContainInOrder(data3).And.ContainItemsAssignableTo<byte>();
        this.resultKeys[route1.Topic].Should().Be(Key1);
        this.resultKeys[route2.Topic].Should().Be(Key2);
        this.resultKeys[route3.Topic].Should().Be(Key3);
    }

    [Fact]
    public void Test_Stream_Send_Data_To_DeadLetter_When_Route_NotFound()
    {
        //arrange
        var logger = Substitute.For<ILogger>();
        var routingConfigurationProvider = Substitute.For<IRoutingConfigurationProvider>();
        var topicName1 = Guid.NewGuid().ToString();
        var route1 = new KafkaRoute("route1", topicName1);
        const string DeadLetterTopic = "dead-letter";
        var configuration = new RoutingConfiguration(
            new KafkaRoutingConfig(
                new KafkaPublishingConfig(),
                [],
                [],
                DeadLetterTopic));

        routingConfigurationProvider.Provide().Returns(configuration);
        var router = new Router(
            logger,
            new KafkaProducerBuilder(
                logger,
                routingConfigurationProvider,
                new KafkaRouteManager(logger)));

        using var cancellationTokenSource = new CancellationTokenSource();
        var token = cancellationTokenSource.Token;

        const string Key1 = "key1";
        var data1 = new byte[20];
        var random = new Random();

        random.NextBytes(data1);
        var autoResetEvent1 = new AutoResetEvent(false);
        this.autoResetEvents.TryAdd(DeadLetterTopic, autoResetEvent1);
        router.Initiate();

        // act
        router.Route(new RoutingDataPacket(data1, route1.Name, DateTime.UtcNow, Key1));
        var listener1 = CreateListener(DeadLetterTopic, token);
        listener1.OnReceived += this.KafkaListener_OnReceived;
        _ = Task.Run(() => listener1.Start(), token);

        //assert
        autoResetEvent1.WaitOne(10000);
        this.results[DeadLetterTopic].Should().NotBeNull().And.HaveCount(20).And.ContainInOrder(data1).And.ContainItemsAssignableTo<byte>();
        this.resultKeys[DeadLetterTopic].Should().Be($"_{route1.Name}:_{Key1}");
    }

    [Fact]
    public void Kafka_Route_Manager_Should_Work_Concurrently()
    {
        //arrange
        var logger = Substitute.For<ILogger>();
        var routingInfo = Enumerable.Range(1, 30).Select(i => new KafkaRoutingManagementInfo(
            Server,
            [
                new KafkaRoute($"route{i}", $"route_{i}_topic{i}")
            ],
            [])).ToList();

        var routeManager = new KafkaRouteManager(logger);
        // act
        var lstTask = new List<Task>();
        for (var i = 0; i < 30; i++)
        {
            var kafKaRoutingManagementInfo = routingInfo[i];
            lstTask.Add(
                Task.Run(() =>
                {
                    routeManager.CheckRoutes(kafKaRoutingManagementInfo);
                }));
        }

        Task.WaitAll([.. lstTask]);
        // act
        lstTask = [];
        for (var i = 0; i < 30; i++)
        {
            var kafKaRoutingManagementInfo = routingInfo[i];
            lstTask.Add(
                Task.Run(() =>
                {
                    routeManager.CheckRoutes(kafKaRoutingManagementInfo);
                }));
        }

        Task.WaitAll([.. lstTask]);

        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = Server
        };
        var adminClient = new AdminClientBuilder(adminConfig).Build();

        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(3));
        foreach (var kafKaRoutingManagementInfo in routingInfo)
        {
            Assert.Contains(kafKaRoutingManagementInfo.Routes[0].Topic, metadata.Topics.Select(i => i.Topic));
        }
    }

    [Fact]
    public void Kafka_Route_Manager_Should_Create_Route_When_SomeRoute_exist_Some_HaveTo_Create()
    {
        //arrange
        var logger = Substitute.For<ILogger>();
        var routingManagementInfo1 = new KafkaRoutingManagementInfo(
            Server,
            [new KafkaRoute("sample_route_1", "sample_route_1_topic1")],
            [new KafkaTopicMetaData("sample_route_1_topic1", 3)]);

        var routingManagementInfo2 = new KafkaRoutingManagementInfo(
            Server,
            [
                new KafkaRoute("sample_route_1", "sample_route_1_topic1"),
                new KafkaRoute("sample_route_2", "sample_route_2_topic2"),
                new KafkaRoute("sample_route_3", "sample_route_3_topic3")
            ],
            [
                new KafkaTopicMetaData("sample_route_1_topic1", 1),
                new KafkaTopicMetaData("sample_route_2_topic2", 2),
                new KafkaTopicMetaData("sample_route_3_topic3", 3),
            ]);
        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = Server
        };
        var adminClient = new AdminClientBuilder(adminConfig).Build();

        var routeManager = new KafkaRouteManager(logger);

        // act
        routeManager.CheckRoutes(routingManagementInfo1);
      
        //assert
        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(3));
        var found = metadata.Topics.FirstOrDefault(i => i.Topic == "sample_route_1_topic1");
        found.Should().NotBeNull();
        found?.Partitions.Count.Should().Be(3);

        // act
        routeManager.CheckRoutes(routingManagementInfo2);

        //assert
        metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(3));
        found = metadata.Topics.FirstOrDefault(i => i.Topic == "sample_route_2_topic2");
        found.Should().NotBeNull();
        found?.Partitions.Count.Should().Be(2);

        found = metadata.Topics.FirstOrDefault(i => i.Topic == "sample_route_3_topic3");
        found.Should().NotBeNull();
        found?.Partitions.Count.Should().Be(3);
    }

    private static KafkaHelperListener<string?, byte[]> CreateListener(string topicName, CancellationToken token)
    {
        return new KafkaHelperListener<string?, byte[]>(Server, topicName, token);
    }

    private void KafkaListener_OnReceived(object? sender, MessageReceived<string?, byte[]> e)
    {
        AddDataToDictionary(this.results, e.Topic, e.Data);
        this.resultKeys.Add(e.Topic, e.Key);
        this.autoResetEvents[e.Topic].Set();
    }

    private static void AddDataToDictionary<TKey, TValue>(ConcurrentDictionary<TKey, TValue> dictionary, TKey key, TValue value)
        where TKey : notnull
    {
        var success = false;
        while (!success)
        {
            success = dictionary.TryAdd(key, value);
            if (!success)
            {
                Task.Delay(5).Wait();
            }
        }
    }
}
