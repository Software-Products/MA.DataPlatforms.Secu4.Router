// <copyright file="KafkaConsumer.cs" company="Motion Applied Ltd.">
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

using MA.Common;
using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Contracts.Abstractions;
using MA.DataPlatforms.Secu4.Routing.PrometheusMetrics;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Shared.Core;

namespace MA.DataPlatforms.Secu4.Routing.Shared.Consuming;

public abstract class KafkaConsumer : IKafkaConsumer
{
    private static readonly TimeSpan StopWaitTimeout = TimeSpan.FromSeconds(10);

    private readonly ILogger logger;
    private readonly IConsumingConfigurationProvider consumingConfigurationProvider;
    private readonly object lifecycleLock = new();
    protected IConsumer<string?, byte[]>? Consumer;
    private CancellationTokenSource? cancellationTokenSource;
    private Task? pollTask;
    private int pollThreadId = -1;
    private bool stopped;
    private bool disposed;

    protected KafkaConsumer(ILogger logger, IConsumingConfigurationProvider consumingConfigurationProvider)
    {
        this.logger = logger;
        this.consumingConfigurationProvider = consumingConfigurationProvider;
    }

    public event EventHandler<RoutingDataPacket>? MessageReceived;

    public IKafkaRoute Route { get; private set; } = new KafkaEmptyRoute();

    public void Dispose()
    {
        this.Dispose(true);
        GC.SuppressFinalize(this);
    }

    public void StartListening(IKafkaRoute kafkaRoute)
    {
        this.Route = kafkaRoute;
        var subscriptionConfiguration = this.consumingConfigurationProvider.Provide();
        var kafkaConfiguration = subscriptionConfiguration.KafkaConsumingConfigs.FirstOrDefault(i => i.KafkaRoutes.Name == kafkaRoute.Name);

        if (kafkaConfiguration != null)
        {
            var listening = kafkaConfiguration.KafkaListeningConfig;
            var kafkaBrokerConfig = new ConsumerConfig
            {
                BootstrapServers = listening.Server,
                GroupId = listening.GroupId,
                AutoOffsetReset = GetTheOffsetMode(listening.AutoOffsetResetMode),
            };

            KafkaSecurityApplier.Apply(kafkaBrokerConfig, kafkaConfiguration.Security);

            foreach (var property in listening.Properties)
            {
                kafkaBrokerConfig.Set(property.Key, property.Value);
            }

            this.Consumer = new ConsumerBuilder<string?, byte[]>(kafkaBrokerConfig)
                .SetStatisticsHandler((_, json) => RouterStatisticsCollector.Handle(json))
                .SetLogHandler((_, log) => KafkaClientLogRouter.Route(this.logger, $"Kafka consumer {this.Route.Name}", log))
                .SetErrorHandler((_, error) => KafkaClientLogRouter.Route(this.logger, $"Kafka consumer {this.Route.Name}", error))
                .Build();

            var lstPartitions = new List<TopicPartition>();
            if (kafkaRoute.Partition.HasValue)
            {
                var topicPartition = new TopicPartition(kafkaRoute.Topic, kafkaRoute.Partition.Value);
                lstPartitions.Add(topicPartition);
            }
            else
            {
                lstPartitions = GetPartitions(kafkaConfiguration, kafkaRoute.Topic);
            }

            this.cancellationTokenSource = new CancellationTokenSource();
            var token = this.cancellationTokenSource.Token;

            // No token on Task.Run: the loop must always start so the consumer is closed/disposed
            // exactly once, on this task, even when cancellation races the start.
            this.pollTask = Task.Run(() => this.RunReadLoop(kafkaConfiguration, lstPartitions, token));
        }
        else
        {
            throw new BusinessException("the kafkaRoute configuration not found for defined kafkaRoute name");
        }
    }

    public void Stop()
    {
        this.logger.Info($"Stopping {this.Route.Name} Consumer");
        lock (this.lifecycleLock)
        {
            if (this.stopped)
            {
                this.logger.Info($"The {this.Route.Name} Consumer already stopped");
                return;
            }

            this.stopped = true;
            this.cancellationTokenSource?.Cancel();
        }

        this.WaitForPollTaskCompletion();
    }

    protected virtual void Dispose(bool disposing)
    {
        if (this.disposed)
        {
            return;
        }

        if (!disposing)
        {
            return;
        }

        this.Stop();

        if (this.pollTask == null)
        {
            // Never started listening: the poll task can't do the cleanup, so do it here.
            this.CloseAndDisposeConsumer();
        }

        if (this.pollTask == null || this.pollTask.IsCompleted)
        {
            this.cancellationTokenSource?.Dispose();
        }

        this.disposed = true;
    }

    protected abstract void StartReadingPartitions(KafkaConsumingConfig kafkaConfiguration, IReadOnlyList<TopicPartition> lstPartitions, CancellationToken token);

    protected void OnMessagedReceived(RoutingDataPacket routingDataPacket)
    {
        if (this.stopped)
        {
            return;
        }

        this.MessageReceived?.Invoke(this, routingDataPacket);
    }

    protected static Offset GetOffset(KafkaConsumingConfig kafkaConfiguration)
    {
        return (kafkaConfiguration.KafkaListeningConfig.Offset ?? 0) >= 0 ? new Offset(kafkaConfiguration.KafkaListeningConfig.Offset ?? 0) : Offset.Stored;
    }

    private void RunReadLoop(KafkaConsumingConfig kafkaConfiguration, IReadOnlyList<TopicPartition> lstPartitions, CancellationToken token)
    {
        this.pollThreadId = Environment.CurrentManagedThreadId;
        try
        {
            this.StartReadingPartitions(kafkaConfiguration, lstPartitions, token);
        }
        finally
        {
            this.CloseAndDisposeConsumer();
        }
    }

    private void CloseAndDisposeConsumer()
    {
        var consumer = this.Consumer;
        this.Consumer = null;
        if (consumer == null)
        {
            return;
        }

        try
        {
            // Graceful shutdown: completes the final auto-commit and tears down the
            // group-coordinator connection cleanly instead of aborting it mid-handshake.
            consumer.Close();
        }
        catch (Exception ex)
        {
            this.logger.Warning($"Closing the {this.Route.Name} consumer failed: {ex.Message}");
        }
        finally
        {
            consumer.Dispose();
        }
    }

    private void WaitForPollTaskCompletion()
    {
        var task = this.pollTask;
        if (task == null || task.IsCompleted)
        {
            return;
        }

        if (Environment.CurrentManagedThreadId == this.pollThreadId)
        {
            // Stop() invoked from a MessageReceived handler on the poll thread itself;
            // waiting here would deadlock. The loop exits after the handler returns and
            // cleans up in its own finally.
            return;
        }

        try
        {
            if (!task.Wait(StopWaitTimeout))
            {
                this.logger.Warning($"The {this.Route.Name} consumer poll loop did not finish within {StopWaitTimeout.TotalSeconds} seconds");
            }
        }
        catch (AggregateException)
        {
            // Loop exceptions are already logged by the derived classes.
        }
    }

    private static List<TopicPartition> GetPartitions(KafkaConsumingConfig kafkaConfiguration, string topic)
    {
        var adminClientConfig = new AdminClientConfig
        {
            BootstrapServers = kafkaConfiguration.KafkaListeningConfig.Server
        };
        KafkaSecurityApplier.Apply(adminClientConfig, kafkaConfiguration.Security);
        using var adminClient = new AdminClientBuilder(adminClientConfig).Build();
        var metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(10));
        return metadata.Topics[0].Partitions.Select(p => new TopicPartition(topic, new Partition(p.PartitionId))).ToList();
    }

    private static AutoOffsetReset GetTheOffsetMode(AutoOffsetResetMode configurationAutoOffsetResetMode)
    {
        return configurationAutoOffsetResetMode == AutoOffsetResetMode.Earliest ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest;
    }
}
