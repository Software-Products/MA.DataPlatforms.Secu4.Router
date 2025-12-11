// <copyright file="KafkaListener.cs" company="Motion Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouteSubscriberComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Shared.Consuming;

namespace MA.DataPlatforms.Secu4.RouteSubscriberComponent;

public class KafkaListener : KafkaConsumer, IKafkaListener
{
    private readonly ILogger logger;

    public KafkaListener(IConsumingConfigurationProvider consumingConfigurationProvider, ICancellationTokenSourceProvider cancellationTokenSourceProvider, ILogger logger)
        : base(consumingConfigurationProvider, cancellationTokenSourceProvider)
    {
        this.logger = logger;
    }

    protected override void StartReadingPartitions(KafkaConsumingConfig kafkaConfiguration, IReadOnlyList<TopicPartition> lstPartitions, CancellationToken token)
    {
        try
        {
            if (this.Consumer == null)
            {
                return;
            }

            this.Consumer.Assign(lstPartitions.Select(i => new TopicPartitionOffset(i, GetOffset(kafkaConfiguration))).ToList());

            while (!token.IsCancellationRequested)
            {
                var consumeResult = this.Consumer.Consume(token);
                this.OnMessagedReceived(
                    new RoutingDataPacket(consumeResult.Message.Value, this.Route.Name, consumeResult.Message.Timestamp.UtcDateTime, consumeResult.Message.Key));
            }

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
