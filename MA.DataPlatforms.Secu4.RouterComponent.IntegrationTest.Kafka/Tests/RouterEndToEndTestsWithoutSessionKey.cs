// <copyright file="RouterEndToEndTestsWithoutSessionKey.cs" company="Motion Applied Ltd.">
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

using Confluent.Kafka;

using FluentAssertions;

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;
using MA.DataPlatforms.Secu4.RouterComponent.IntegrationTest.Kafka.Base;
using MA.DataPlatforms.Secu4.RouterComponent.IntegrationTest.Kafka.Helper;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Shared.Core;

using NSubstitute;

using Xunit;

namespace MA.DataPlatforms.Secu4.RouterComponent.IntegrationTest.Kafka.Tests;

[Collection(nameof(RunKafkaDockerComposeCollectionFixture))]
public class RouterEndToEndTests
{
    private const string Server = "localhost:9092";
    private readonly IRouteReadingWritingComponentRepository<KafkaRouter> routerRepository = Substitute.For<IRouteReadingWritingComponentRepository<KafkaRouter>>();

    public RouterEndToEndTests(RunKafkaDockerComposeFixture dockerComposeFixture)
    {
        this.DockerComposeFixture = dockerComposeFixture;
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
        var router = new KafkaRouter(
            logger,
            new KafkaProducerBuilder(
                logger,
                routingConfigurationProvider,
                new KafkaRouteManager(logger)),
            this.routerRepository,
            "TestRouter");

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

        router.Initiate();

        // act

        var listener1 = CreateListener(route1.Topic, token);
        var listener2 = CreateListener(route2.Topic, token);
        var listener3 = CreateListener(route3.Topic, token);

        byte[] topic1Data = [];
        byte[] topic2Data = [];
        byte[] topic3Data = [];

        listener1.OnReceived += (_, e) =>
        {
            topic1Data = e.Data ?? [];
            autoResetEvent1.Set();
        };
        listener2.OnReceived += (_, e) =>
        {
            topic2Data = e.Data ?? [];
            autoResetEvent2.Set();
        };
        listener3.OnReceived += (_, e) =>
        {
            topic3Data = e.Data ?? [];
            autoResetEvent3.Set();
        };

        _ = Task.Run(() => listener1.Start(), token);
        _ = Task.Run(() => listener2.Start(), token);
        _ = Task.Run(() => listener3.Start(), token);

        // Give listeners time to start and subscribe
        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(5));

        router.Route(new RoutingDataPacket(data1, route1.Name, DateTime.UtcNow));
        router.Route(new RoutingDataPacket(data2, route2.Name, DateTime.UtcNow));
        router.Route(new RoutingDataPacket(data3, route3.Name, DateTime.UtcNow));

        autoResetEvent1.WaitOne(TimeSpan.FromSeconds(10));
        autoResetEvent2.WaitOne(TimeSpan.FromSeconds(10));
        autoResetEvent3.WaitOne(TimeSpan.FromSeconds(10));

        //assert
        topic1Data.Should().NotBeNull().And.HaveCount(20).And.ContainInOrder(data1).And.ContainItemsAssignableTo<byte>();
        topic2Data.Should().NotBeNull().And.HaveCount(20).And.ContainInOrder(data2).And.ContainItemsAssignableTo<byte>();
        topic3Data.Should().NotBeNull().And.HaveCount(20).And.ContainInOrder(data3).And.ContainItemsAssignableTo<byte>();
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
        var router = new KafkaRouter(
            logger,
            new KafkaProducerBuilder(
                logger,
                routingConfigurationProvider,
                new KafkaRouteManager(logger)),
            this.routerRepository,
            "TestRouter"
        );

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

        router.Initiate();

        // act
        var listener1 = CreateListener(route1.Topic, token);
        var listener2 = CreateListener(route2.Topic, token);
        var listener3 = CreateListener(route3.Topic, token);
        byte[] topic1Data = [];
        byte[] topic2Data = [];
        byte[] topic3Data = [];

        var topic1Key = "";
        var topic2Key = "";
        var topic3Key = "";

        listener1.OnReceived += (_, e) =>
        {
            topic1Data = e.Data ?? [];
            topic1Key = e.Key ?? "";
            autoResetEvent1.Set();
        };
        listener2.OnReceived += (_, e) =>
        {
            topic2Data = e.Data ?? [];
            topic2Key = e.Key ?? "";
            autoResetEvent2.Set();
        };
        listener3.OnReceived += (_, e) =>
        {
            topic3Data = e.Data ?? [];
            topic3Key = e.Key ?? "";
            autoResetEvent3.Set();
        };

        _ = Task.Run(() => listener1.Start(), token);
        _ = Task.Run(() => listener2.Start(), token);
        _ = Task.Run(() => listener3.Start(), token);

        // Give listeners time to start and subscribe
        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(5));

        router.Route(new RoutingDataPacket(data1, route1.Name, DateTime.UtcNow, Key1));
        router.Route(new RoutingDataPacket(data2, route2.Name, DateTime.UtcNow, Key2));
        router.Route(new RoutingDataPacket(data3, route3.Name, DateTime.UtcNow, Key3));

        autoResetEvent1.WaitOne(TimeSpan.FromSeconds(10));
        autoResetEvent2.WaitOne(TimeSpan.FromSeconds(10));
        autoResetEvent3.WaitOne(TimeSpan.FromSeconds(10));

        //assert
        topic1Data.Should().NotBeNull().And.HaveCount(20).And.ContainInOrder(data1).And.ContainItemsAssignableTo<byte>();
        topic2Data.Should().NotBeNull().And.HaveCount(20).And.ContainInOrder(data2).And.ContainItemsAssignableTo<byte>();
        topic3Data.Should().NotBeNull().And.HaveCount(20).And.ContainInOrder(data3).And.ContainItemsAssignableTo<byte>();

        topic1Key.Should().Be(Key1);
        topic2Key.Should().Be(Key2);
        topic3Key.Should().Be(Key3);
    }

    [Fact]
    public void Test_Stream_Send_Data_To_DeadLetter_When_Route_NotFound()
    {
        //arrange
        var logger = Substitute.For<ILogger>();
        var routingConfigurationProvider = Substitute.For<IRoutingConfigurationProvider>();
        const string DeadLetterTopic = "dead-letter";
        var configuration = new RoutingConfiguration(
            new KafkaRoutingConfig(
                new KafkaPublishingConfig(),
                [],
                [],
                DeadLetterTopic));

        routingConfigurationProvider.Provide().Returns(configuration);
        var router = new KafkaRouter(
            logger,
            new KafkaProducerBuilder(
                logger,
                routingConfigurationProvider,
                new KafkaRouteManager(logger)),
            this.routerRepository,
            "TestRouter");

        using var cancellationTokenSource = new CancellationTokenSource();
        var token = cancellationTokenSource.Token;

        const string Key1 = "key1";
        var data1 = new byte[20];
        var random = new Random();

        random.NextBytes(data1);
        var autoResetEvent1 = new AutoResetEvent(false);
        router.Initiate();

        // act

        var listener1 = CreateListener(DeadLetterTopic, token);

        byte[] topic1Data = [];

        var topic1Key = "";

        listener1.OnReceived += (_, e) =>
        {
            topic1Data = e.Data ?? [];
            topic1Key = e.Key ?? "";
            autoResetEvent1.Set();
        };
        _ = Task.Run(() => listener1.Start(), token);

        // Give listener time to start and subscribe
        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(2));

        router.Route(new RoutingDataPacket(data1, "missing_route", DateTime.UtcNow, Key1));
        //assert
        autoResetEvent1.WaitOne(TimeSpan.FromSeconds(10));
        topic1Data.Should().NotBeNull().And.HaveCount(20).And.ContainInOrder(data1).And.ContainItemsAssignableTo<byte>();
        topic1Key.Should().Be($"_missing_route:_{Key1}");
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

        Task.Delay(1000).Wait();
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
        var testId = Guid.NewGuid().ToString("N").Substring(0, 8);
        var routingManagementInfo1 = new KafkaRoutingManagementInfo(
            Server,
            [new KafkaRoute($"route_{testId}_1", $"topic_{testId}_1")],
            [new KafkaTopicMetaData($"topic_{testId}_1", 3)]);

        var routingManagementInfo2 = new KafkaRoutingManagementInfo(
            Server,
            [
                new KafkaRoute($"route_{testId}_1", $"topic_{testId}_1"),
                new KafkaRoute($"route_{testId}_2", $"topic_{testId}_2"),
                new KafkaRoute($"route_{testId}_3", $"topic_{testId}_3")
            ],
            [
                new KafkaTopicMetaData($"topic_{testId}_1", 1),
                new KafkaTopicMetaData($"topic_{testId}_2", 2),
                new KafkaTopicMetaData($"topic_{testId}_3", 3),
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
        var found = metadata.Topics.FirstOrDefault(i => i.Topic == $"topic_{testId}_1");
        found.Should().NotBeNull();
        found?.Partitions.Count.Should().Be(3);

        // act
        routeManager.CheckRoutes(routingManagementInfo2);

        //assert
        Task.Delay(1000).Wait();

        metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(3));

        found = metadata.Topics.FirstOrDefault(i => i.Topic == $"topic_{testId}_2");
        found.Should().NotBeNull();
        found?.Partitions.Count.Should().Be(2);

        found = metadata.Topics.FirstOrDefault(i => i.Topic == $"topic_{testId}_3");
        found.Should().NotBeNull();
        found?.Partitions.Count.Should().Be(3);
    }

    private static KafkaHelperListener<string?, byte[]> CreateListener(string topicName, CancellationToken token)
    {
        return new KafkaHelperListener<string?, byte[]>(Server, topicName, token);
    }
}
