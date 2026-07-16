// <copyright file="MicrosoftConfigurationSecretResolver.cs" company="Motion Applied Ltd.">
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

using Microsoft.Extensions.Configuration;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.Core;

/// <summary>
/// <see cref="ISecretResolver"/> that looks up <c>${KEY_NAME}</c> placeholders
/// as keys in <see cref="IConfiguration"/>. This single implementation gives
/// access to any secret store that has a .NET configuration provider:
///
/// <code>
/// // Azure Key Vault
/// var config = new ConfigurationBuilder()
///     .AddAzureKeyVault(new Uri("https://my-vault.vault.azure.net/"),
///                       new DefaultAzureCredential())
///     .Build();
///
/// // AWS Secrets Manager
/// var config = new ConfigurationBuilder()
///     .AddSecretsManager()   // Kralizek.Extensions.Configuration.AWSSecretsManager
///     .Build();
///
/// // HashiCorp Vault (VaultSharp + custom IConfigurationSource)
/// var config = new ConfigurationBuilder()
///     .AddVaultConfiguration(...)
///     .Build();
///
/// // .NET User Secrets (development only)
/// var config = new ConfigurationBuilder()
///     .AddUserSecrets&lt;Program&gt;()
///     .Build();
///
/// // Then pass to the provider:
/// new JsonKafkaPublisherConfigurationProvider(
///     "kafka-publisher.json",
///     new MicrosoftConfigurationSecretResolver(config));
/// </code>
///
/// <para>
/// The placeholder name (e.g. <c>KAFKA_SASL_PASSWORD</c> from
/// <c>${KAFKA_SASL_PASSWORD}</c>) is used as-is as the configuration key.
/// Adjust the key names in the JSON file to match the naming convention of
/// your chosen secret store (e.g. Azure Key Vault uses hyphens:
/// <c>${kafka-sasl-password}</c>).
/// </para>
/// </summary>
public sealed partial class MicrosoftConfigurationSecretResolver : ISecretResolver
{
    private readonly IConfiguration configuration;

    [GeneratedRegex(@"^\$\{(?<name>[^}]+)\}$")]
    private static partial Regex PlaceholderPattern();

    public MicrosoftConfigurationSecretResolver(IConfiguration configuration)
    {
        this.configuration = configuration;
    }

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

        var key = match.Groups["name"].Value;
        return this.configuration[key];
    }
}
