// <copyright file="KafkaProducerShould.cs" company="McLaren Applied Ltd.">
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
using MA.DataPlatform.Secu4.RouterComponent.BrokersPublishers.KafkaBroking.Producing;
using MA.DataPlatform.Secu4.RouterComponent.IntegrationTest.Kafka.Base;
using MA.DataPlatform.Secu4.RouterComponent.IntegrationTest.Kafka.Helper;
using MA.DataPlatform.Secu4.Routing.Contracts;
using MA.DataPlatform.Secu4.Routing.Shared.Abstractions;
using MA.DataPlatform.Secu4.Routing.Shared.Core;

using NSubstitute;

using Xunit;

namespace MA.DataPlatform.Secu4.RouterComponent.IntegrationTest.Kafka.Tests;

[Collection(nameof(RunKafkaDockerComposeCollectionFixture))]
public class KafkaProducerShould
{
    private const string Server = "localhost:9092";
    private readonly KafkaProducer kafkaProducer;
    private readonly KafkaRouteManager kafkaRouteManager;
    private readonly Dictionary<string, AutoResetEvent> autoResetEvents = [];
    private readonly Dictionary<string, byte[]?> results = [];
    private readonly Dictionary<string, string?> resultKeys = [];

    private readonly IKafkaRouteRepository kafkaRouteRepository;

    private readonly IRoutingConfigurationProvider routingConfigurationProvider;

    public KafkaProducerShould(RunKafkaDockerComposeFixture dockerComposeFixture)
    {
        this.DockerComposeFixture = dockerComposeFixture;
        this.routingConfigurationProvider = Substitute.For<IRoutingConfigurationProvider>();
        var logger = Substitute.For<ILogger>();
        this.kafkaProducer = new KafkaProducer(logger, this.routingConfigurationProvider);
        this.kafkaRouteRepository = Substitute.For<IKafkaRouteRepository>();
        var kafkaTopicMetaDataRepository = Substitute.For<IKafkaTopicMetaDataRepository>();
        var brokerUrlProvider = Substitute.For<IBrokerUrlProvider>();
        brokerUrlProvider.Provide().Returns(new KafkaPublishingConfig().Server);
        this.kafkaRouteManager = new KafkaRouteManager(logger, brokerUrlProvider, this.kafkaRouteRepository, kafkaTopicMetaDataRepository);
    }

    public RunKafkaDockerComposeFixture DockerComposeFixture { get; }

    [Fact]
    public void Producer_Produce_Data_With_key_and_Check_the_Receive_Data_To_See_If_Is_Same_As_Publishing()
    {
        //arrange done in ctor

        var topicName = Guid.NewGuid().ToString();
        var kafkaRoutes = new List<KafkaRoute>
        {
            new("test", topicName)
        };
        this.routingConfigurationProvider.Provide().Returns(
            new RoutingConfiguration(
                new KafkaRoutingConfig(
                    new KafkaPublishingConfig(),
                    kafkaRoutes,
                    [new KafkaTopicMetaData(topicName)],
                    "deadLetter")));
        this.kafkaRouteRepository.GetRoutes().Returns(kafkaRoutes);
        this.kafkaProducer.Initiate();
        this.kafkaRouteManager.CheckRoutes();
        const int DataBytesLength = 20;
        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);
        using var cancellationTokenSource = new CancellationTokenSource();
        var token = cancellationTokenSource.Token;
        var kafkaListener = new KafkaHelperListener<string?, byte[]>(Server, topicName, token);
        var autoResetEvent = new AutoResetEvent(false);
        this.autoResetEvents.Add(topicName, autoResetEvent);
        kafkaListener.OnReceived += this.KafkaListener_OnReceived;
        _ = Task.Run(() => kafkaListener.Start(), token);
        var key = "testKey" + Guid.NewGuid();
        Task.Delay(TimeSpan.FromMilliseconds(500), token).Wait(token);

        //act
        this.kafkaProducer.Produce(data, topicName, key: key);

        //assert
        autoResetEvent.WaitOne();
        this.results[topicName].Should().NotBeNull().And.HaveCount(DataBytesLength).And.ContainInOrder(data).And.ContainItemsAssignableTo<byte>();
        this.resultKeys[topicName].Should().Be(key);
        cancellationTokenSource.Cancel();
        this.kafkaProducer.Dispose();
    }

    [Fact]
    public void Producer_Produce_Data_Without_Key_and_Check_the_Receive_Data_To_See_If_Is_Same_As_Publishing()
    {
        //arrange done in ctor
        var topicName = Guid.NewGuid().ToString();
        var kafkaRoutes = new List<KafkaRoute>
        {
            new("test", topicName)
        };
        this.routingConfigurationProvider.Provide().Returns(
            new RoutingConfiguration(
                new KafkaRoutingConfig(
                    new KafkaPublishingConfig(),
                    kafkaRoutes,
                    [new KafkaTopicMetaData(topicName)],
                    "deadLetter")));
        this.kafkaRouteRepository.GetRoutes().Returns(kafkaRoutes);
        this.kafkaProducer.Initiate();
        this.kafkaRouteManager.CheckRoutes();
        const int DataBytesLength = 20;
        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);
        using var cancellationTokenSource = new CancellationTokenSource();
        var token = cancellationTokenSource.Token;
        var kafkaListener = new KafkaHelperListener<string?, byte[]>(Server, topicName, token);
        var autoResetEvent = new AutoResetEvent(false);
        this.autoResetEvents.Add(topicName, autoResetEvent);
        kafkaListener.OnReceived += this.KafkaListener_OnReceived;
        _ = Task.Run(() => kafkaListener.Start(), token);
        Task.Delay(TimeSpan.FromMilliseconds(500), token).Wait(token);

        //act
        this.kafkaProducer.Produce(data, topicName);

        //assert
        autoResetEvent.WaitOne();
        this.results[topicName].Should().NotBeNull().And.HaveCount(DataBytesLength).And.ContainInOrder(data).And.ContainItemsAssignableTo<byte>();
        this.resultKeys[topicName].Should().Be(null);
        cancellationTokenSource.Cancel();
        this.kafkaProducer.Dispose();
    }

    private void KafkaListener_OnReceived(object? sender, MessageReceived<string?, byte[]> e)
    {
        this.results.Add(e.Topic, e.Data);
        this.resultKeys.Add(e.Topic, e.Key);
        this.autoResetEvents[e.Topic].Set();
    }
}
