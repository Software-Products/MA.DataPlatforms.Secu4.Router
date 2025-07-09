// <copyright file="KafkaReader.cs" company="McLaren Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouteReaderComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Shared.Consuming;

namespace MA.DataPlatforms.Secu4.RouteReaderComponent;

public class KafkaReader : KafkaConsumer, IKafkaReader
{
    private readonly ILogger logger;
    private int messageCounter;

    public KafkaReader(IConsumingConfigurationProvider consumingConfigurationProvider, ICancellationTokenSourceProvider cancellationTokenSourceProvider, ILogger logger)
        : base(consumingConfigurationProvider, cancellationTokenSourceProvider)
    {
        this.logger = logger;
    }

    public event EventHandler<ReadingCompletedEventArgs>? ReadingCompleted;

    protected override void StartReadingPartitions(KafkaConsumingConfig kafkaConfiguration, IReadOnlyList<TopicPartition> lstPartitions, CancellationToken token)
    {
        try
        {
            if (this.Consumer == null)
            {
                return;
            }

            var offsetDictionary = new Dictionary<int, long>();
            foreach (var partition in lstPartitions)
            {
                var endOffsets = this.Consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10));
                offsetDictionary.Add(partition.Partition.Value, endOffsets.High);
            }

            var lstTopicPartitionsOffset = lstPartitions.Select(i => new TopicPartitionOffset(i, GetOffset(kafkaConfiguration))).ToList();

            this.Consumer.Assign(lstTopicPartitionsOffset);

            while (lstTopicPartitionsOffset.Any())
            {
                var consumeResult = this.Consumer.Consume(token);

                Interlocked.Add(ref this.messageCounter, 1);
                this.OnMessagedReceived(
                    new RoutingDataPacket(consumeResult.Message.Value, this.Route.Name, consumeResult.Message.Timestamp.UtcDateTime, consumeResult.Message.Key));

                if (consumeResult.Offset < offsetDictionary[consumeResult.Partition.Value] - 1)
                {
                    continue;
                }

                lstTopicPartitionsOffset.RemoveAll(i => i.Partition.Value == consumeResult.Partition.Value);
                this.Consumer.Assign(lstTopicPartitionsOffset.Select(i => i.TopicPartition).ToList());
            }

            this.ReadingCompleted?.Invoke(this, new ReadingCompletedEventArgs(this.Route.Name, DateTime.Now, this.messageCounter));
            this.Consumer.Unassign();
        }
        catch (OperationCanceledException)
        {
            this.logger.Warning($"The {this.Route.Name} Listener Stopped");
        }
        catch (Exception ex)
        {
            this.logger.Error(ex.ToString());
        }
        finally
        {
            this.Consumer?.Dispose();
        }
    }
}
