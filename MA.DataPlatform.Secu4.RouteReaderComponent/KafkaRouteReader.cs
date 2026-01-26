// <copyright file="KafkaRouteReader.cs" company="Motion Applied Ltd.">
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

namespace MA.DataPlatforms.Secu4.RouteReaderComponent;

public class KafkaRouteReader : IRouteReader
{
    private readonly IKafkaReaderFactory kafkaReaderFactory;
    private readonly IRouteManager routeManager;
    private readonly ILogger logger;
    private readonly IRouteReadingWritingComponentRepository<KafkaRouteReader> routeReaderRepository;
    private readonly IKafkaRoutingManagementInfoCreator kafkaRoutingManagementInfoCreator;

    public KafkaRouteReader(
        IKafkaReaderFactory kafkaReaderFactory,
        IRouteManager routeManager,
        ILogger logger,
        IRouteReadingWritingComponentRepository<KafkaRouteReader> routeReaderRepository,
        IKafkaRoutingManagementInfoCreator kafkaRoutingManagementInfoCreator,
        string id)
    {
        this.kafkaReaderFactory = kafkaReaderFactory;
        this.routeManager = routeManager;
        this.logger = logger;
        this.routeReaderRepository = routeReaderRepository;
        this.kafkaRoutingManagementInfoCreator = kafkaRoutingManagementInfoCreator;
        this.Id = id;
        logger.Info($"Created Kafka route reader With ID: {this.Id}");
        routeReaderRepository.Add(this);
    }

    public string Id { get; }

    public event EventHandler<RoutingDataPacket>? PacketReceived;

    public event EventHandler<ReadingCompletedEventArgs>? ReadingCompleted;

    public void StartReading()
    {
        this.logger.Info($"Kafka route reader With ID: {this.Id} has started reading.");
        var kaRoutingManagementInfos = this.kafkaRoutingManagementInfoCreator.CreateRouteManagementInfo();
        foreach (var kaRoutingManagementInfo in kaRoutingManagementInfos)
        {
            this.routeManager.CheckRoutes(kaRoutingManagementInfo);
        }

        var routes = kaRoutingManagementInfos.SelectMany(i => i.Routes).ToList();
        foreach (var kafkaRoute in routes)
        {
            this.logger.Info($"Starting Kafka reader for route: {kafkaRoute.Name} topic: {kafkaRoute.Topic}");
            var kafkaReader = this.kafkaReaderFactory.Create();
            kafkaReader.MessageReceived += this.KafkaReader_MessageReceived;
            kafkaReader.ReadingCompleted += this.KafkaReader_ReadingCompleted;
            kafkaReader.StartListening(kafkaRoute);
        }
    }

    private void KafkaReader_ReadingCompleted(object? sender, ReadingCompletedEventArgs e)
    {
        this.ReadingCompleted?.Invoke(this, e);
        this.logger.Info($"Kafka route reader With ID: {this.Id} has completed reading.");
        this.routeReaderRepository.Remove(this.Id);
    }

    private void KafkaReader_MessageReceived(object? sender, RoutingDataPacket e)
    {
        this.PacketReceived?.Invoke(this, e);
    }
}
