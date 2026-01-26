// <copyright file="KafkaReaderFactoryShould.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

using NSubstitute;

namespace MA.DataPlatforms.Secu4.RouteReaderComponent.UnitTests;

public class KafkaReaderFactoryShould
{
    private readonly IConsumingConfigurationProvider consumingConfigurationProvider;
    private readonly ILogger logger;
    private readonly KafkaReaderFactory sut;

    public KafkaReaderFactoryShould()
    {
        this.consumingConfigurationProvider = Substitute.For<IConsumingConfigurationProvider>();
        this.logger = Substitute.For<ILogger>();
        this.sut = new KafkaReaderFactory(this.consumingConfigurationProvider, this.logger);
    }

    [Fact]
    public void Create_ShouldReturnKafkaReader()
    {
        // Act
        var result = this.sut.Create();

        // Assert
        Assert.NotNull(result);
        Assert.IsAssignableFrom<IKafkaReader>(result);
    }

    [Fact]
    public void Create_ShouldReturnNewInstanceEachTime()
    {
        // Act
        var result1 = this.sut.Create();
        var result2 = this.sut.Create();

        // Assert
        Assert.NotSame(result1, result2);
    }

    [Fact]
    public void Constructor_ShouldInitializeWithProvidedDependencies()
    {
        // Arrange & Act
        var factory = new KafkaReaderFactory(this.consumingConfigurationProvider, this.logger);

        // Assert
        Assert.NotNull(factory);
    }

    [Fact]
    public void Create_ShouldPassDependenciesToKafkaReader()
    {
        // Act
        var result = this.sut.Create();

        // Assert
        Assert.NotNull(result);
        // The reader should be successfully created with the injected dependencies
        Assert.IsType<KafkaReader>(result);
    }
}
