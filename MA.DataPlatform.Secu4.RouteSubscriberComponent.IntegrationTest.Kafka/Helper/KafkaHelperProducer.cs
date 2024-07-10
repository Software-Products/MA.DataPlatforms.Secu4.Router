// <copyright file="KafkaHelperProducer.cs" company="McLaren Applied Ltd.">
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

using Confluent.Kafka;

using MA.DataPlatform.Secu4.Routing.Contracts;

namespace MA.DataPlatform.Secu4.RouteSubscriberComponent.IntegrationTest.Kafka.Helper;

internal class KafkaHelperProducer
{
    private readonly IProducer<string?, byte[]> producer;

    public KafkaHelperProducer(string server)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = server,
        };
        this.producer = new ProducerBuilder<string?, byte[]>(producerConfig).Build();
    }

    public void Produce(byte[] message, KafkaRoute route, string? key = null)
    {
        try
        {
            var topicPartition = new TopicPartition(route.Topic, route.Partition ?? 0);
            this.producer.Produce(
                topicPartition,
                new Message<string?, byte[]>
                {
                    Value = message,
                    Key = key
                },
                (dr) =>
                {
                    Console.WriteLine(dr.ToString());
                });
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }
    }
}
