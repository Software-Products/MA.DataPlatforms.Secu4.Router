// <copyright file="EnvironmentVariableSecretResolver.cs" company="Motion Applied Ltd.">
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

using System.Text.RegularExpressions;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.Core;

/// <summary>
/// Default <see cref="ISecretResolver"/> that resolves <c>${VAR_NAME}</c>
/// placeholders from process environment variables. Non-placeholder values
/// are returned unchanged.
/// </summary>
public sealed partial class EnvironmentVariableSecretResolver : ISecretResolver
{
    [GeneratedRegex(@"^\$\{(?<name>[^}]+)\}$")]
    private static partial Regex PlaceholderPattern();

    /// <inheritdoc/>
    public string? Resolve(string? value)
    {
        if (value is null)
        {
            return null;
        }

        var match = PlaceholderPattern().Match(value);
        if (!match.Success)
        {
            return value;
        }

        var envVarName = match.Groups["name"].Value;
        return Environment.GetEnvironmentVariable(envVarName);
    }
}

