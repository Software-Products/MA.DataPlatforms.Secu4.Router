// <copyright file="KafkaRouteReaderShould.cs" company="Motion Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouteReaderComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Shared.Core;

using NSubstitute;

namespace MA.DataPlatforms.Secu4.RouteReaderComponent.UnitTests;

public class KafkaRouteReaderShould
{
    private readonly IKafkaReaderFactory kafkaReaderFactory = Substitute.For<IKafkaReaderFactory>();
    private readonly IRouteManager routeManager = Substitute.For<IRouteManager>();
    private readonly ILogger logger = Substitute.For<ILogger>();
    private readonly IRouteReadingWritingComponentRepository<KafkaRouteReader> routeReaderRepository =
        Substitute.For<IRouteReadingWritingComponentRepository<KafkaRouteReader>>();
    private readonly IKafkaRoutingManagementInfoCreator kafkaRoutingManagementInfoCreator = Substitute.For<IKafkaRoutingManagementInfoCreator>();

    private const string TestId = "test-id";

    [Fact]
    public void Constructor_ShouldInitializeProperties()
    {
        // Act
        var sut = new KafkaRouteReader(
            this.kafkaReaderFactory,
            this.routeManager,
            this.logger,
            this.routeReaderRepository,
            this.kafkaRoutingManagementInfoCreator,
            TestId);

        // Assert
        Assert.Equal(TestId, sut.Id);
    }

    [Fact]
    public void Constructor_ShouldLogCreation()
    {
        // Act
        var _ = new KafkaRouteReader(
            this.kafkaReaderFactory,
            this.routeManager,
            this.logger,
            this.routeReaderRepository,
            this.kafkaRoutingManagementInfoCreator,
            TestId);

        // Assert
        this.logger.Received(1).Info($"Created Kafka route reader With ID: {TestId}");
    }

    [Fact]
    public void Constructor_ShouldAddToRepository()
    {
        // Act
        var sut = new KafkaRouteReader(
            this.kafkaReaderFactory,
            this.routeManager,
            this.logger,
            this.routeReaderRepository,
            this.kafkaRoutingManagementInfoCreator,
            TestId);

        // Assert
        this.routeReaderRepository.Received(1).Add(sut);
    }

    [Fact]
    public void StartReading_ShouldLogStartMessage()
    {
        // Arrange
        var sut = this.CreateSut();
        this.SetupEmptyRoutes();

        // Act
        sut.StartReading();

        // Assert
        this.logger.Received(1).Info($"Kafka route reader With ID: {TestId} has started reading.");
    }

    [Fact]
    public void StartReading_ShouldCheckRoutesForEachManagementInfo()
    {
        // Arrange
        var sut = this.CreateSut();
        var managementInfo1 = CreateKafkaRoutingManagementInfo("url1", new List<KafkaRoute>());
        var managementInfo2 = CreateKafkaRoutingManagementInfo("url2", new List<KafkaRoute>());
        var managementInfos = new List<KafkaRoutingManagementInfo>
        {
            managementInfo1,
            managementInfo2
        };
        this.SetupRouteManagementInfos(managementInfos);

        // Act
        sut.StartReading();

        // Assert
        this.routeManager.Received(1).CheckRoutes(managementInfo1);
        this.routeManager.Received(1).CheckRoutes(managementInfo2);
    }

    [Fact]
    public void StartReading_ShouldCreateKafkaReaderForEachRoute()
    {
        // Arrange
        var sut = this.CreateSut();
        var route1 = CreateKafkaRoute("route1", "topic1");
        var route2 = CreateKafkaRoute("route2", "topic2");
        var managementInfo = CreateKafkaRoutingManagementInfo(
            "url",
            new List<KafkaRoute>
            {
                route1,
                route2
            });
        this.SetupRouteManagementInfos([managementInfo]);

        var kafkaReader1 = Substitute.For<IKafkaReader>();
        var kafkaReader2 = Substitute.For<IKafkaReader>();
        this.kafkaReaderFactory.Create().Returns(kafkaReader1, kafkaReader2);

        // Act
        sut.StartReading();

        // Assert
        this.kafkaReaderFactory.Received(2).Create();
    }

    [Fact]
    public void StartReading_ShouldLogForEachRoute()
    {
        // Arrange
        var sut = this.CreateSut();
        var route1 = CreateKafkaRoute("route1", "topic1");
        var route2 = CreateKafkaRoute("route2", "topic2");
        var managementInfo = CreateKafkaRoutingManagementInfo(
            "url",
            new List<KafkaRoute>
            {
                route1,
                route2
            });
        this.SetupRouteManagementInfos([managementInfo]);

        var kafkaReader = Substitute.For<IKafkaReader>();
        this.kafkaReaderFactory.Create().Returns(kafkaReader);

        // Act
        sut.StartReading();

        // Assert
        this.logger.Received(1).Info("Starting Kafka reader for route: route1 topic: topic1");
        this.logger.Received(1).Info("Starting Kafka reader for route: route2 topic: topic2");
    }

    [Fact]
    public void StartReading_ShouldSubscribeToKafkaReaderEvents()
    {
        // Arrange
        var sut = this.CreateSut();
        var route = CreateKafkaRoute("route1", "topic1");
        var managementInfo = CreateKafkaRoutingManagementInfo(
            "url",
            new List<KafkaRoute>
            {
                route
            });
        this.SetupRouteManagementInfos([managementInfo]);

        var kafkaReader = Substitute.For<IKafkaReader>();
        this.kafkaReaderFactory.Create().Returns(kafkaReader);

        // Act
        sut.StartReading();

        // Assert
        kafkaReader.Received(1).MessageReceived += Arg.Any<EventHandler<RoutingDataPacket>>();
        kafkaReader.Received(1).ReadingCompleted += Arg.Any<EventHandler<ReadingCompletedEventArgs>>();
    }

    [Fact]
    public void StartReading_ShouldStartListeningOnKafkaReader()
    {
        // Arrange
        var sut = this.CreateSut();
        var route = CreateKafkaRoute("route1", "topic1");
        var managementInfo = CreateKafkaRoutingManagementInfo(
            "url",
            new List<KafkaRoute>
            {
                route
            });
        this.SetupRouteManagementInfos([managementInfo]);

        var kafkaReader = Substitute.For<IKafkaReader>();
        this.kafkaReaderFactory.Create().Returns(kafkaReader);

        // Act
        sut.StartReading();

        // Assert
        kafkaReader.Received(1).StartListening(route);
    }

    [Fact]
    public void KafkaReader_MessageReceived_ShouldRaisePacketReceivedEvent()
    {
        // Arrange
        var sut = this.CreateSut();
        var route = CreateKafkaRoute("route1", "topic1");
        var managementInfo = CreateKafkaRoutingManagementInfo(
            "url",
            new List<KafkaRoute>
            {
                route
            });
        this.SetupRouteManagementInfos([managementInfo]);

        var kafkaReader = Substitute.For<IKafkaReader>();
        this.kafkaReaderFactory.Create().Returns(kafkaReader);

        RoutingDataPacket? receivedPacket = null;
        sut.PacketReceived += (_, packet) => receivedPacket = packet;

        sut.StartReading();

        var expectedPacket = new RoutingDataPacket([1, 2, 3], "route1", DateTime.UtcNow);

        // Act
        // Manually invoke the event handler since RoutingDataPacket does not inherit from EventArgs
        kafkaReader.MessageReceived += Raise.Event<EventHandler<RoutingDataPacket>>(kafkaReader, expectedPacket);

        // Assert
        Assert.NotNull(receivedPacket);
        Assert.Equal(expectedPacket, receivedPacket);
    }

    [Fact]
    public void KafkaReader_ReadingCompleted_ShouldRaiseReadingCompletedEvent()
    {
        // Arrange
        var sut = this.CreateSut();
        var route = CreateKafkaRoute("route1", "topic1");
        var managementInfo = CreateKafkaRoutingManagementInfo(
            "url",
            new List<KafkaRoute>
            {
                route
            });
        this.SetupRouteManagementInfos([managementInfo]);

        var kafkaReader = Substitute.For<IKafkaReader>();
        this.kafkaReaderFactory.Create().Returns(kafkaReader);

        ReadingCompletedEventArgs? receivedArgs = null;
        sut.ReadingCompleted += (_, args) => receivedArgs = args;

        sut.StartReading();

        var expectedArgs = new ReadingCompletedEventArgs("route1", DateTime.UtcNow, 100);

        // Act
        kafkaReader.ReadingCompleted += Raise.EventWith(kafkaReader, expectedArgs);

        // Assert
        Assert.NotNull(receivedArgs);
        Assert.Equal(expectedArgs, receivedArgs);
    }

    [Fact]
    public void KafkaReader_ReadingCompleted_ShouldLogCompletion()
    {
        // Arrange
        var sut = this.CreateSut();
        var route = CreateKafkaRoute("route1", "topic1");
        var managementInfo = CreateKafkaRoutingManagementInfo(
            "url",
            new List<KafkaRoute>
            {
                route
            });
        this.SetupRouteManagementInfos([managementInfo]);

        var kafkaReader = Substitute.For<IKafkaReader>();
        this.kafkaReaderFactory.Create().Returns(kafkaReader);

        sut.StartReading();

        var args = new ReadingCompletedEventArgs("route1", DateTime.UtcNow, 100);

        // Act
        kafkaReader.ReadingCompleted += Raise.EventWith(kafkaReader, args);

        // Assert
        this.logger.Received(1).Info($"Kafka route reader With ID: {TestId} has completed reading.");
    }

    [Fact]
    public void KafkaReader_ReadingCompleted_ShouldRemoveFromRepository()
    {
        // Arrange
        var sut = this.CreateSut();
        var route = CreateKafkaRoute("route1", "topic1");
        var managementInfo = CreateKafkaRoutingManagementInfo(
            "url",
            new List<KafkaRoute>
            {
                route
            });
        this.SetupRouteManagementInfos([managementInfo]);

        var kafkaReader = Substitute.For<IKafkaReader>();
        this.kafkaReaderFactory.Create().Returns(kafkaReader);

        sut.StartReading();

        var args = new ReadingCompletedEventArgs("route1", DateTime.UtcNow, 100);

        // Act
        kafkaReader.ReadingCompleted += Raise.EventWith(kafkaReader, args);

        // Assert
        this.routeReaderRepository.Received(1).Remove(TestId);
    }

    private KafkaRouteReader CreateSut()
    {
        return new KafkaRouteReader(
            this.kafkaReaderFactory,
            this.routeManager,
            this.logger,
            this.routeReaderRepository,
            this.kafkaRoutingManagementInfoCreator,
            TestId);
    }

    private void SetupEmptyRoutes()
    {
        this.SetupRouteManagementInfos([]);
    }

    private void SetupRouteManagementInfos(List<KafkaRoutingManagementInfo> managementInfos)
    {
        this.kafkaRoutingManagementInfoCreator.CreateRouteManagementInfo().Returns(managementInfos);
    }

    private static KafkaRoutingManagementInfo CreateKafkaRoutingManagementInfo(string url, IReadOnlyList<KafkaRoute> routes)
    {
        return new KafkaRoutingManagementInfo(url, routes, new List<KafkaTopicMetaData>());
    }

    private static KafkaRoute CreateKafkaRoute(string name, string topic)
    {
        return new KafkaRoute(name, topic);
    }
}
