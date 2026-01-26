// <copyright file="KafkaRouteSubscriber.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.RouteSubscriberComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Shared.Core;

namespace MA.DataPlatforms.Secu4.RouteSubscriberComponent;

public class KafkaRouteSubscriber : IRouteSubscriber
{
    public string Id { get; }

    private readonly IKafkaListenerFactory kafkaListenerFactory;
    private readonly IConsumingConfigurationProvider consumingConfigurationProvider;
    private readonly IRouteManager routeManager;
    private readonly ILogger logger;
    private readonly IRouteReadingWritingComponentRepository<KafkaRouteSubscriber> routeSubscriberRepository;
    private readonly List<IKafkaListener> kafkaListeners = [];

    public KafkaRouteSubscriber(
        IKafkaListenerFactory kafkaListenerFactory,
        IConsumingConfigurationProvider consumingConfigurationProvider,
        IRouteManager routeManager,
        ILogger logger,
        IRouteReadingWritingComponentRepository<KafkaRouteSubscriber> routeSubscriberRepository,
        string id)
    {
        this.Id = id;
        this.kafkaListenerFactory = kafkaListenerFactory;
        this.consumingConfigurationProvider = consumingConfigurationProvider;
        this.routeManager = routeManager;
        this.logger = logger;
        this.routeSubscriberRepository = routeSubscriberRepository;
        logger.Info($"Created Kafka route subscriber With ID: {this.Id}");
        routeSubscriberRepository.Add(this);
    }

    public event EventHandler<RoutingDataPacket>? PacketReceived;

    public void Subscribe()
    {
        var kaRoutingManagementInfos = new KafkaRoutingManagementInfoCreator(this.consumingConfigurationProvider).CreateRouteManagementInfo();
        foreach (var kaRoutingManagementInfo in kaRoutingManagementInfos)
        {
            this.routeManager.CheckRoutes(kaRoutingManagementInfo);
        }

        var routes = kaRoutingManagementInfos.SelectMany(i => i.Routes).ToList();
        foreach (var kafkaRoute in routes)
        {
            this.logger.Info($"Start listening to Kafka route: {kafkaRoute.Name} topic: {kafkaRoute.Topic}");
            var kafkaListener = this.kafkaListenerFactory.Create();
            this.kafkaListeners.Add(kafkaListener);
            kafkaListener.MessageReceived += this.KafkaListener_MessageReceived;
            kafkaListener.StartListening(kafkaRoute);
        }
    }

    public void Unsubscribe()
    {
        foreach (var kafkaListener in this.kafkaListeners)
        {
            kafkaListener.Stop();
            this.logger.Info($"Stopped listening to Kafka route:{kafkaListener.Route.Name} topic: {kafkaListener.Route.Topic}");
        }

        this.logger.Info($"Unsubscribed from Kafka route subscriber With ID: {this.Id}");
        this.routeSubscriberRepository.Remove(this.Id);
    }

    private void KafkaListener_MessageReceived(object? sender, RoutingDataPacket e)
    {
        this.PacketReceived?.Invoke(this, e);
    }
}
