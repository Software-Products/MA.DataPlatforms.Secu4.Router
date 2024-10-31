// <copyright file="KafkaReaderFactory.cs" company="McLaren Applied Ltd.">
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
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

namespace MA.DataPlatforms.Secu4.RouteReaderComponent;

public class KafkaReaderFactory : IKafkaReaderFactory
{
    private readonly IConsumingConfigurationProvider consumingConfigurationProvider;
    private readonly ICancellationTokenSourceProvider cancellationTokenSourceProvider;
    private readonly ILogger logger;

    public KafkaReaderFactory(
        IConsumingConfigurationProvider consumingConfigurationProvider,
        ICancellationTokenSourceProvider cancellationTokenSourceProvider,
        ILogger logger)
    {
        this.consumingConfigurationProvider = consumingConfigurationProvider;
        this.cancellationTokenSourceProvider = cancellationTokenSourceProvider;
        this.logger = logger;
    }

    public IKafkaReader Create()
    {
        return new KafkaReader(this.consumingConfigurationProvider, this.cancellationTokenSourceProvider, this.logger);
    }
}
