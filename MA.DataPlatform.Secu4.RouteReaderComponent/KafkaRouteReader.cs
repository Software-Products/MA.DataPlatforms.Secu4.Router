// <copyright file="KafkaRouteReader.cs" company="McLaren Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouteReaderComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Shared.Core;

namespace MA.DataPlatforms.Secu4.RouteReaderComponent;

public class KafkaRouteReader : IRouteReader
{
    private readonly IKafkaReaderFactory kafkaReaderFactory;
    private readonly IConsumingConfigurationProvider consumingConfigurationProvider;
    private readonly ILogger logger;

    public KafkaRouteReader(
        IKafkaReaderFactory kafkaReaderFactory,
        IConsumingConfigurationProvider consumingConfigurationProvider,
        ILogger logger)
    {
        this.kafkaReaderFactory = kafkaReaderFactory;
        this.consumingConfigurationProvider = consumingConfigurationProvider;
        this.logger = logger;
    }

    public event EventHandler<RoutingDataPacket>? PacketReceived;

    public event EventHandler<ReadingCompletedEventArgs>? ReadingCompleted;

    public void StartReading()
    {
        var config = this.consumingConfigurationProvider.Provide();

        var brokersConfig = config.KafkaConsumingConfigs.Select(
            i => new
            {
                i.KafkaListeningConfig.Server,
                i.KafkaRoutes,
                i.RoutesMetaData
            }).GroupBy(i => i.Server).ToList();

        foreach (var kafkaRouteManager in brokersConfig.Select(
                     groupItem => new KafkaRouteManager(
                         this.logger,
                         new KafkaBrokerUrlProvider(groupItem.Key),
                         new KafkaRouteRepository(groupItem.Select(i => i.KafkaRoutes)),
                         new KafkaTopicMetaDataRepository(groupItem.Select(i => i.RoutesMetaData)))))
        {
            kafkaRouteManager.CheckRoutes();
        }

        var routes = config.KafkaConsumingConfigs.Select(i => i.KafkaRoutes).ToList();
        foreach (var kafkaRoute in routes)
        {
            var kafkaReader = this.kafkaReaderFactory.Create();
            kafkaReader.MessageReceived += this.KafkaReader_MessageReceived;
            kafkaReader.ReadingCompleted += this.KafkaReader_ReadingCompleted;
            kafkaReader.StartListening(kafkaRoute);
        }
    }

    private void KafkaReader_ReadingCompleted(object? sender, ReadingCompletedEventArgs e)
    {
        this.ReadingCompleted?.Invoke(this, e);
    }

    private void KafkaReader_MessageReceived(object? sender, RoutingDataPacket e)
    {
        this.PacketReceived?.Invoke(this, e);
    }
}
