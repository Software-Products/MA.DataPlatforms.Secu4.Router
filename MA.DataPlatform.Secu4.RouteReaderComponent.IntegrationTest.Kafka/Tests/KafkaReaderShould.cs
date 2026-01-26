// <copyright file="KafkaReaderShould.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.RouteReaderComponent.IntegrationTest.Kafka.Base;
using MA.DataPlatforms.Secu4.RouteReaderComponent.IntegrationTest.Kafka.Helper;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

using NSubstitute;

namespace MA.DataPlatforms.Secu4.RouteReaderComponent.IntegrationTest.Kafka.Tests;

[Collection(nameof(RunKafkaDockerComposeCollectionFixture))]
public class KafkaReaderShould
{
    private const string Server = "localhost:9095";
    private const int DataBytesLength = 20;
    private readonly KafkaHelperProducer kafkaProducer;
    private readonly IConsumingConfigurationProvider subscriberConfigurationProvider;

    private readonly Dictionary<string, byte[]?> results = [];
    private readonly Dictionary<string, string?> resultKeys = [];
    private readonly ILogger logger;

    public KafkaReaderShould(RunKafkaDockerComposeFixture dockerComposeFixture)
    {
        this.DockerComposeFixture = dockerComposeFixture;
        this.kafkaProducer = new KafkaHelperProducer(Server);
        this.subscriberConfigurationProvider = Substitute.For<IConsumingConfigurationProvider>();
        this.logger = Substitute.For<ILogger>();
    }

    public RunKafkaDockerComposeFixture DockerComposeFixture { get; }

    [Fact]
    public void Producer_Produce_Data_With_key_and_Check_the_Receive_Data_To_See_If_Is_Same_As_Publishing_With_Earliest_Offset_Starting()
    {
        //arrange
        var kafkaReader = new KafkaReader(this.subscriberConfigurationProvider, this.logger);
        var topicName = "topic_krs_1";
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
        kafkaReader.MessageReceived += this.kafkaReader_MessageReceived;
        var autoResetEvent = new AutoResetEvent(false);
        kafkaReader.ReadingCompleted += (_, _) =>
        {
            autoResetEvent.Set();
        };

        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);
        var key = "testKey" + Guid.NewGuid();

        //act
        this.kafkaProducer.Produce(data, kafkaRoute, key);
        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(1));
        kafkaReader.StartListening(kafkaRoute);
        //assert
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(30));
        kafkaReader.Stop();
        this.results[kafkaRoute.Name].Should().NotBeNull().And.HaveCount(DataBytesLength).And.ContainInOrder(data).And.ContainItemsAssignableTo<byte>();
        this.resultKeys[kafkaRoute.Name].Should().Be(key);
    }

    [Fact]
    public void Producer_Produce_Data_With_Offset_and_Check_the_Receive_Data_To_See_If_Is_Same_As_Publishing()
    {
        //arrange
        var kafkaReader = new KafkaReader(this.subscriberConfigurationProvider, this.logger);
        var topicName = "topic_krs_2";
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
        var autoResetEvent = new AutoResetEvent(false);
        kafkaReader.MessageReceived += this.kafkaReader_MessageReceived;
        kafkaReader.ReadingCompleted += (_, _) =>
        {
            autoResetEvent.Set();
        };
        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);
        //act
        this.kafkaProducer.Produce(data, kafkaRoute);
        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(1));
        kafkaReader.StartListening(kafkaRoute);

        //assert
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(30));
        kafkaReader.Stop();
        this.results[kafkaRoute.Name].Should().NotBeNull().And.HaveCount(DataBytesLength).And.ContainInOrder(data).And.ContainItemsAssignableTo<byte>();
        this.resultKeys[kafkaRoute.Name].Should().Be(null);
    }

    [Fact]
    public void Producer_Produce_Data_With_Offset_And_Check_The_Receive_Data_To_See_If_Only_Data_After_That_Offset_Is_Read()
    {
        //arrange
        var kafkaReader = new KafkaReader(this.subscriberConfigurationProvider, this.logger);
        var topicName = "topic_krs_3";
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
        var autoResetEvent = new AutoResetEvent(false);
        var keys = new List<string?>();
        kafkaReader.MessageReceived += (_, e) =>
        {
            keys.Add(e.Key);
        };
        kafkaReader.ReadingCompleted += (_, _) =>
        {
            autoResetEvent.Set();
        };
        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);

        Task.Delay(TimeSpan.FromMilliseconds(5000)).Wait();
        //act
        for (var i = 0; i < 15; i++)
        {
            this.kafkaProducer.Produce(data, kafkaRoute, i.ToString());
        }

        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(2));
        kafkaReader.StartListening(kafkaRoute);

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
    public void Producer_Produce_Data_In_Different_Partitions_Check_Receive_All_Data_From_All_Partitions()
    {
        //arrange
        var kafkaReader = new KafkaReader(this.subscriberConfigurationProvider, this.logger);
        var groupId = Guid.NewGuid().ToString();
        var topicName = "topic_krs_4";
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topicName, 3));
        var kafkaRoute1 = new KafkaRoute("test5", topicName, 0);
        var kafkaRoute2 = new KafkaRoute("test6", topicName, 1);
        var kafkaRoute3 = new KafkaRoute("test7", topicName, 2);
        var kafkaRoute4 = new KafkaRoute("test8", topicName);
        var routes = new List<KafkaRoute>
        {
            kafkaRoute1,
            kafkaRoute2,
            kafkaRoute3
        };
        this.subscriberConfigurationProvider.Provide().Returns(
            new ConsumingConfiguration(
            [
                new KafkaConsumingConfig(
                    new KafkaListeningConfig(Server, groupId, AutoOffsetResetMode.Earliest, 0),
                    kafkaRoute4,
                    new KafkaTopicMetaData(topicName, 3))
            ]));
        var autoResetEvent = new AutoResetEvent(false);
        var routingDataPackets = new List<RoutingDataPacket?>();
        kafkaReader.MessageReceived += (_, e) =>
        {
            routingDataPackets.Add(e);
        };
        kafkaReader.ReadingCompleted += (_, _) =>
        {
            autoResetEvent.Set();
        };
        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);
        //act
        for (var i = 0; i < 3; i++)
        {
            this.kafkaProducer.Produce(data, routes[i], i.ToString());
        }

        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(1));
        kafkaReader.StartListening(kafkaRoute4);

        //assert
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(30));
        routingDataPackets.Count.Should().Be(3);
    }

    [Fact]
    public void Producer_Produce_Data_In_Different_Partitions_Check_The_Receive_All_Data_From_All_Partitions_And_Discard_Messaged_Published_After_Start_Listening()
    {
        //arrange
        var kafkaReader = new KafkaReader(this.subscriberConfigurationProvider, this.logger);
        var groupId = Guid.NewGuid().ToString();
        var topicName = "topic_krs_5";
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topicName, 3));
        var kafkaRoute1 = new KafkaRoute("test9", topicName, 0);
        var kafkaRoute2 = new KafkaRoute("test10", topicName, 1);
        var kafkaRoute3 = new KafkaRoute("test11", topicName, 2);
        var kafkaRoute4 = new KafkaRoute("test12", topicName);
        var routes = new List<KafkaRoute>
        {
            kafkaRoute1,
            kafkaRoute2,
            kafkaRoute3
        };
        this.subscriberConfigurationProvider.Provide().Returns(
            new ConsumingConfiguration(
            [
                new KafkaConsumingConfig(
                    new KafkaListeningConfig(Server, groupId, AutoOffsetResetMode.Earliest, 0),
                    kafkaRoute4,
                    new KafkaTopicMetaData(topicName, 3))
            ]));
        var autoResetEvent = new AutoResetEvent(false);
        var routingDataPackets = new List<RoutingDataPacket?>();
        kafkaReader.MessageReceived += (_, e) =>
        {
            routingDataPackets.Add(e);
        };
        kafkaReader.ReadingCompleted += (_, _) =>
        {
            autoResetEvent.Set();
        };
        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);
        //act
        for (var i = 0; i < 3; i++)
        {
            this.kafkaProducer.Produce(data, routes[i], i.ToString());
        }

        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(2));
        kafkaReader.StartListening(kafkaRoute4);

        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(5));

        for (var i = 0; i < 3; i++)
        {
            this.kafkaProducer.Produce(data, routes[i], i.ToString());
        }

        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(5));
        //assert
        autoResetEvent.WaitOne(TimeSpan.FromSeconds(30));
        routingDataPackets.Count.Should().Be(3);
    }

    [Fact]
    public void Creating_Two_Concurrent_Reader_With_Same_Configuration_ShouldNot_ThrowException()
    {
        //arrange
        var kafkaReader1 = new KafkaReader(this.subscriberConfigurationProvider, this.logger);
        var kafkaReader2 = new KafkaReader(this.subscriberConfigurationProvider, this.logger);
        var groupId = Guid.NewGuid().ToString();
        var topicName = "topic_krs_6";
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topicName, 3));
        var kafkaRoute1 = new KafkaRoute("test13", topicName, 0);
        var kafkaRoute2 = new KafkaRoute("test14", topicName, 1);
        var kafkaRoute3 = new KafkaRoute("test15", topicName, 2);
        var kafkaRoute4 = new KafkaRoute("test16", topicName);
        var routes = new List<KafkaRoute>
        {
            kafkaRoute1,
            kafkaRoute2,
            kafkaRoute3
        };
        this.subscriberConfigurationProvider.Provide().Returns(
            new ConsumingConfiguration(
            [
                new KafkaConsumingConfig(
                    new KafkaListeningConfig(Server, groupId, AutoOffsetResetMode.Earliest, 0),
                    kafkaRoute4,
                    new KafkaTopicMetaData(topicName, 3))
            ]));
        var autoResetEvent1 = new AutoResetEvent(false);
        var autoResetEvent2 = new AutoResetEvent(false);
        var routingDataPackets1 = new List<RoutingDataPacket?>();
        var routingDataPackets2 = new List<RoutingDataPacket?>();
        kafkaReader1.MessageReceived += (_, e) =>
        {
            routingDataPackets1.Add(e);
        };
        kafkaReader2.MessageReceived += (_, e) =>
        {
            routingDataPackets2.Add(e);
        };
        kafkaReader1.ReadingCompleted += (_, _) =>
        {
            autoResetEvent1.Set();
        };
        kafkaReader2.ReadingCompleted += (_, _) =>
        {
            autoResetEvent2.Set();
        };
        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);
        //act
        for (var i = 0; i < 3; i++)
        {
            this.kafkaProducer.Produce(data, routes[i], i.ToString());
        }

        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(2));

        Task.Run(() => kafkaReader1.StartListening(kafkaRoute4));
        Task.Run(() => kafkaReader2.StartListening(kafkaRoute4));

        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(5));

        for (var i = 0; i < 3; i++)
        {
            this.kafkaProducer.Produce(data, routes[i], i.ToString());
        }

        new AutoResetEvent(false).WaitOne(TimeSpan.FromSeconds(5));
        //assert
        autoResetEvent1.WaitOne(TimeSpan.FromSeconds(30));
        autoResetEvent2.WaitOne(TimeSpan.FromSeconds(30));
        routingDataPackets1.Count.Should().Be(3);
        routingDataPackets2.Count.Should().Be(3);
    }

    private void kafkaReader_MessageReceived(object? sender, RoutingDataPacket e)
    {
        if (e.Message.Length != DataBytesLength)
        {
            return;
        }

        this.results.Add(e.Route, e.Message);
        this.resultKeys.Add(e.Route, e.Key);
    }
}
