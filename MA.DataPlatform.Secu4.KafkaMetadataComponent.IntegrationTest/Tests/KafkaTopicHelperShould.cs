// <copyright file="KafkaTopicHelperShould.cs" company="McLaren Applied Ltd.">
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

using MA.DataPlatform.Secu4.KafkaMetadataComponent.IntegrationTest.Base;
using MA.DataPlatform.Secu4.KafkaMetadataComponent.IntegrationTest.Helper;
using MA.DataPlatform.Secu4.Routing.Contracts;

namespace MA.DataPlatform.Secu4.KafkaMetadataComponent.IntegrationTest.Tests;

[Collection(nameof(RunKafkaDockerComposeCollectionFixture))]
public class KafkaTopicHelperShould
{
    private const string Server = "localhost:9099";
    private const int DataBytesLength = 20;
    private readonly KafkaHelperProducer kafkaProducer;

    public KafkaTopicHelperShould(RunKafkaDockerComposeFixture dockerComposeFixture)
    {
        this.DockerComposeFixture = dockerComposeFixture;
        this.kafkaProducer = new KafkaHelperProducer(Server);
    }

    public RunKafkaDockerComposeFixture DockerComposeFixture { get; }

    [Fact]
    public void Return_The_Max_Offset_Of_Partitions_When_Have_Multiple_Partitions()
    {
        //arrange
        var prefix = Guid.NewGuid().ToString();
        var topicName = prefix + Guid.NewGuid();
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topicName, 3));
        var kafkaRoute1 = new KafkaRoute("route1", topicName, 0);
        var kafkaRoute2 = new KafkaRoute("route2", topicName, 1);
        var kafkaRoute3 = new KafkaRoute("route3", topicName, 2);
        var routes = new List<KafkaRoute>
        {
            kafkaRoute1,
            kafkaRoute2,
            kafkaRoute3
        };

        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);
        for (var i = 0; i < 15; i++)
        {
            var kafkaRoute = routes[i % 3];
            this.kafkaProducer.Produce(data, kafkaRoute, i.ToString());
        }

        Task.Delay(5000).Wait();

        var topicHelper = new KafkaTopicHelper();
        //act
        var result = topicHelper.GetInfoByTopicPrefix(Server, prefix);

        //assert
        result = [.. result.OrderBy(i => i.TopicName).ThenBy(i => i.Partition)];
        result.Count.Should().Be(3);
        result[0].TopicName.Should().Be(topicName);
        result[0].Partition.Should().Be(0);
        result[0].Offset.Should().Be(5);

        result[1].TopicName.Should().Be(topicName);
        result[1].Partition.Should().Be(1);
        result[1].Offset.Should().Be(5);

        result[2].TopicName.Should().Be(topicName);
        result[2].Partition.Should().Be(2);
        result[2].Offset.Should().Be(5);
    }

    [Fact]
    public void Return_The_Max_Offset_Of_TopicPartitions_When_Have_Multiple_Topics()
    {
        //arrange
        var prefix = Guid.NewGuid().ToString();
        var topic1 = $"{prefix}._1";
        var topic2 = $"{prefix}._2";
        var topic3 = $"{prefix}._3";

        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic1, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic2, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic3, 1));

        var kafkaRoute1 = new KafkaRoute("route1", topic1);
        var kafkaRoute2 = new KafkaRoute("route2", topic2);
        var kafkaRoute3 = new KafkaRoute("route3", topic3);

        var routes = new List<KafkaRoute>
        {
            kafkaRoute1,
            kafkaRoute2,
            kafkaRoute3
        };

        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);
        for (var i = 0; i < 15; i++)
        {
            var kafkaRoute = routes[i % 3];
            this.kafkaProducer.Produce(data, kafkaRoute, i.ToString());
        }

        Task.Delay(5000).Wait();

        var topicHelper = new KafkaTopicHelper();
        //act
        var result = topicHelper.GetInfoByTopicPrefix(Server, prefix);

        //assert
        result = [.. result.OrderBy(i => i.TopicName).ThenBy(i => i.Partition)];
        result.Count.Should().Be(3);

        result[0].TopicName.Should().Be(topic1);
        result[0].Partition.Should().Be(0);
        result[0].Offset.Should().Be(5);

        result[1].TopicName.Should().Be(topic2);
        result[1].Partition.Should().Be(0);
        result[1].Offset.Should().Be(5);

        result[2].TopicName.Should().Be(topic3);
        result[2].Partition.Should().Be(0);
        result[2].Offset.Should().Be(5);
    }

    [Fact]
    public void Return_The_Max_Offset_Of_TopicPartitions_When_Have_Multiple_Topics_And_Multiple_Partitions()
    {
        //arrange
        var prefix = Guid.NewGuid().ToString();
        var topic1 = $"{prefix}._1";
        var topic2 = $"{prefix}._2";
        var topic3 = $"{prefix}._3";
        var topic4 = $"{prefix}._4";

        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic1, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic2, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic3, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic4, 3));

        var kafkaRoute1 = new KafkaRoute("route1", topic1);
        var kafkaRoute2 = new KafkaRoute("route2", topic2);
        var kafkaRoute3 = new KafkaRoute("route3", topic3);
        var kafkaRoute4 = new KafkaRoute("route4", topic4, 0);
        var kafkaRoute5 = new KafkaRoute("route4", topic4, 1);
        var kafkaRoute6 = new KafkaRoute("route4", topic4, 2);

        var routes = new List<KafkaRoute>
        {
            kafkaRoute1,
            kafkaRoute2,
            kafkaRoute3,
            kafkaRoute4,
            kafkaRoute5,
            kafkaRoute6,
        };

        var data = new byte[DataBytesLength];
        new Random().NextBytes(data);
        for (var i = 0; i < 18; i++)
        {
            var kafkaRoute = routes[i % 6];
            this.kafkaProducer.Produce(data, kafkaRoute, i.ToString());
        }

        Task.Delay(5000).Wait();

        var topicHelper = new KafkaTopicHelper();
        //act
        var result = topicHelper.GetInfoByTopicPrefix(Server, prefix);

        //assert
        result = [.. result.OrderBy(i => i.TopicName).ThenBy(i => i.Partition)];
        result.Count.Should().Be(6);

        result[0].TopicName.Should().Be(topic1);
        result[0].Partition.Should().Be(0);
        result[0].Offset.Should().Be(3);

        result[1].TopicName.Should().Be(topic2);
        result[1].Partition.Should().Be(0);
        result[1].Offset.Should().Be(3);

        result[2].TopicName.Should().Be(topic3);
        result[2].Partition.Should().Be(0);
        result[2].Offset.Should().Be(3);

        result[3].TopicName.Should().Be(topic4);
        result[3].Partition.Should().Be(0);
        result[3].Offset.Should().Be(3);

        result[4].TopicName.Should().Be(topic4);
        result[4].Partition.Should().Be(1);
        result[4].Offset.Should().Be(3);

        result[5].TopicName.Should().Be(topic4);
        result[5].Partition.Should().Be(2);
        result[5].Offset.Should().Be(3);
    }

    [Fact]
    public void Return_The_All_The_Topics_With_Prefix()
    {
        //arrange
        var prefix = Guid.NewGuid().ToString();
        var topic1 = $"{prefix}._1";
        var topic2 = $"{prefix}._2";
        var topic3 = $"{prefix}._3";
        var topic4 = $"{prefix}._4";
        var topic5 = $"test_Unknown_{Guid.NewGuid()}";

        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic1, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic2, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic3, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic4, 3));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic5, 3));
        Task.Delay(5000).Wait();

        var topicHelper = new KafkaTopicHelper();
        //act
        var result = topicHelper.GetInfoByTopicPrefix(Server, prefix);

        //assert
        result = [.. result.OrderBy(i => i.TopicName).ThenBy(i => i.Partition)];
        result.Count.Should().Be(6);

        result[0].TopicName.Should().Be(topic1);
        result[0].Partition.Should().Be(0);
        result[0].Offset.Should().Be(0);

        result[1].TopicName.Should().Be(topic2);
        result[1].Partition.Should().Be(0);
        result[1].Offset.Should().Be(0);

        result[2].TopicName.Should().Be(topic3);
        result[2].Partition.Should().Be(0);
        result[2].Offset.Should().Be(0);

        result[3].TopicName.Should().Be(topic4);
        result[3].Partition.Should().Be(0);
        result[3].Offset.Should().Be(0);

        result[4].TopicName.Should().Be(topic4);
        result[4].Partition.Should().Be(1);
        result[4].Offset.Should().Be(0);

        result[5].TopicName.Should().Be(topic4);
        result[5].Partition.Should().Be(2);
        result[5].Offset.Should().Be(0);
    }

    [Fact]
    public void Return_The_All_The_Topics_With_PostFix()
    {
        //arrange
        var nameSuffix = Guid.NewGuid().ToString();
        var topic1 = $"1_{nameSuffix}";
        var topic2 = $"2_{nameSuffix}";
        var topic3 = $"3_{nameSuffix}";
        var topic4 = $"4_{nameSuffix}";
        var topic5 = $"5_test_Unknown_{Guid.NewGuid()}";

        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic1, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic2, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic3, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic4, 3));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic5, 1));
        Task.Delay(5000).Wait();

        var topicHelper = new KafkaTopicHelper();
        //act
        var result = topicHelper.GetInfoByTopicSuffix(Server, nameSuffix);

        //assert
        result = [.. result.OrderBy(i => i.TopicName).ThenBy(i => i.Partition)];
        result.Count.Should().Be(6);

        result[0].TopicName.Should().Be(topic1);
        result[0].Partition.Should().Be(0);
        result[0].Offset.Should().Be(0);

        result[1].TopicName.Should().Be(topic2);
        result[1].Partition.Should().Be(0);
        result[1].Offset.Should().Be(0);

        result[2].TopicName.Should().Be(topic3);
        result[2].Partition.Should().Be(0);
        result[2].Offset.Should().Be(0);

        result[3].TopicName.Should().Be(topic4);
        result[3].Partition.Should().Be(0);
        result[3].Offset.Should().Be(0);

        result[4].TopicName.Should().Be(topic4);
        result[4].Partition.Should().Be(1);
        result[4].Offset.Should().Be(0);

        result[5].TopicName.Should().Be(topic4);
        result[5].Partition.Should().Be(2);
        result[5].Offset.Should().Be(0);
    }

    [Fact]
    public void Return_The_All_The_Topics_With_SubString()
    {
        //arrange
        var subString = Guid.NewGuid().ToString();
        var topic1 = $"1_{subString}_test";
        var topic2 = $"2_{subString}_test";
        var topic3 = $"3_{subString}_test";
        var topic4 = $"4_{subString}_test";
        var topic5 = $"5_test_Unknown_{Guid.NewGuid()}";

        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic1, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic2, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic3, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic4, 3));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic5, 3));
        Task.Delay(5000).Wait();

        var topicHelper = new KafkaTopicHelper();
        //act
        var result = topicHelper.GetInfoByTopicContains(Server, subString);

        //assert
        result = [.. result.OrderBy(i => i.TopicName).ThenBy(i => i.Partition)];
        result.Count.Should().Be(6);

        result[0].TopicName.Should().Be(topic1);
        result[0].Partition.Should().Be(0);
        result[0].Offset.Should().Be(0);

        result[1].TopicName.Should().Be(topic2);
        result[1].Partition.Should().Be(0);
        result[1].Offset.Should().Be(0);

        result[2].TopicName.Should().Be(topic3);
        result[2].Partition.Should().Be(0);
        result[2].Offset.Should().Be(0);

        result[3].TopicName.Should().Be(topic4);
        result[3].Partition.Should().Be(0);
        result[3].Offset.Should().Be(0);

        result[4].TopicName.Should().Be(topic4);
        result[4].Partition.Should().Be(1);
        result[4].Offset.Should().Be(0);

        result[5].TopicName.Should().Be(topic4);
        result[5].Partition.Should().Be(2);
        result[5].Offset.Should().Be(0);
    }

    [Fact]
    public void Return_All_The_Topics_When_SubString_Is_Empty()
    {
        //arrange
        var subString = Guid.NewGuid().ToString();
        var topic1 = $"1_{subString}_test";
        var topic2 = $"2_{subString}_test";
        var topic3 = $"3_{subString}_test";
        var topic4 = $"4_{subString}_test";
        var topic5 = $"5_test_Unknown_{Guid.NewGuid()}";

        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic1, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic2, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic3, 1));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic4, 3));
        new KafkaHelperTopicCreator(Server).Create(new KafkaTopicMetaData(topic5, 1));
        Task.Delay(5000).Wait();

        var topicHelper = new KafkaTopicHelper();
        //act
        var result = topicHelper.GetInfoByTopicContains(Server, string.Empty);

        //assert
        result = [.. result.OrderBy(i => i.TopicName).ThenBy(i => i.Partition)];
        result.Count.Should().Be(7);

        result[0].TopicName.Should().Be(topic1);
        result[0].Partition.Should().Be(0);
        result[0].Offset.Should().Be(0);

        result[1].TopicName.Should().Be(topic2);
        result[1].Partition.Should().Be(0);
        result[1].Offset.Should().Be(0);

        result[2].TopicName.Should().Be(topic3);
        result[2].Partition.Should().Be(0);
        result[2].Offset.Should().Be(0);

        result[3].TopicName.Should().Be(topic4);
        result[3].Partition.Should().Be(0);
        result[3].Offset.Should().Be(0);

        result[4].TopicName.Should().Be(topic4);
        result[4].Partition.Should().Be(1);
        result[4].Offset.Should().Be(0);

        result[5].TopicName.Should().Be(topic4);
        result[5].Partition.Should().Be(2);
        result[5].Offset.Should().Be(0);

        result[6].TopicName.Should().Be(topic5);
        result[6].Partition.Should().Be(0);
        result[6].Offset.Should().Be(0);
    }
}
