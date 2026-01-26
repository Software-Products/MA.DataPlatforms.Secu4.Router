// <copyright file="RouteReadingWritingComponentRepositoryTests.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Shared.Core;

using NSubstitute;

namespace MA.DataPlatforms.Secu4.RouteReaderComponent.UnitTests;

public class RouteReadingWritingComponentRepositoryTests
{
    private readonly ILogger logger;
    private readonly RouteReadingWritingComponentRepository<IRouteReadingWritingComponent> sut;

    public RouteReadingWritingComponentRepositoryTests()
    {
        this.logger = Substitute.For<ILogger>();
        this.sut = new RouteReadingWritingComponentRepository<IRouteReadingWritingComponent>(this.logger);
    }

    [Fact]
    public void Add_WhenComponentDoesNotExist_AddsComponentToRepository()
    {
        // Arrange
        var component = Substitute.For<IRouteReadingWritingComponent>();
        component.Id.Returns("test-id");

        // Act
        this.sut.Add(component);

        // Assert
        var result = this.sut.Get("test-id");
        Assert.NotNull(result);
        Assert.Equal("test-id", result.Id);
    }

    [Fact]
    public void Add_WhenComponentAlreadyExists_LogsErrorAndDoesNotAdd()
    {
        // Arrange
        var component1 = Substitute.For<IRouteReadingWritingComponent>();
        component1.Id.Returns("test-id");
        var component2 = Substitute.For<IRouteReadingWritingComponent>();
        component2.Id.Returns("test-id");

        this.sut.Add(component1);

        // Act
        this.sut.Add(component2);

        // Assert
        this.logger.Received(1).Error(Arg.Is<string>(s => s.Contains("test-id") && s.Contains("already exists")));
        var result = this.sut.Get("test-id");
        Assert.Same(component1, result);
    }

    [Fact]
    public void Get_WhenComponentExists_ReturnsComponent()
    {
        // Arrange
        var component = Substitute.For<IRouteReadingWritingComponent>();
        component.Id.Returns("test-id");
        this.sut.Add(component);

        // Act
        var result = this.sut.Get("test-id");

        // Assert
        Assert.NotNull(result);
        Assert.Equal("test-id", result.Id);
    }

    [Fact]
    public void Get_WhenComponentDoesNotExist_ReturnsNull()
    {
        // Act
        var result = this.sut.Get("non-existent-id");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void GetAll_WhenRepositoryIsEmpty_ReturnsEmptyList()
    {
        // Act
        var result = this.sut.GetAll();

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public void GetAll_WhenRepositoryHasComponents_ReturnsAllComponents()
    {
        // Arrange
        var component1 = Substitute.For<IRouteReadingWritingComponent>();
        component1.Id.Returns("id-1");
        var component2 = Substitute.For<IRouteReadingWritingComponent>();
        component2.Id.Returns("id-2");

        this.sut.Add(component1);
        this.sut.Add(component2);

        // Act
        var result = this.sut.GetAll();

        // Assert
        Assert.Equal(2, result.Count);
        Assert.Contains(result, c => c.Id == "id-1");
        Assert.Contains(result, c => c.Id == "id-2");
    }

    [Fact]
    public void Remove_WhenComponentExists_RemovesComponent()
    {
        // Arrange
        var component = Substitute.For<IRouteReadingWritingComponent>();
        component.Id.Returns("test-id");
        this.sut.Add(component);

        // Act
        this.sut.Remove("test-id");

        // Assert
        var result = this.sut.Get("test-id");
        Assert.Null(result);
    }

    [Fact]
    public void Remove_WhenComponentDoesNotExist_DoesNotThrow()
    {
        // Act & Assert
        var exception = Record.Exception(() => this.sut.Remove("non-existent-id"));
        Assert.Null(exception);
    }
}
