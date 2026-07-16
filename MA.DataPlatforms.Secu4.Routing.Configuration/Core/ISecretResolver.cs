// <copyright file="ISecretResolver.cs" company="Motion Applied Ltd.">
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

namespace MA.DataPlatforms.Secu4.Routing.Configuration.Core;

/// <summary>
/// Resolves secret placeholders found in Kafka broker config files.
/// The default implementation reads from environment variables.
/// Swap the implementation to pull secrets from any secure store.
///
/// <para>Built-in implementations:</para>
/// <list type="bullet">
///   <item><description>
///     <see cref="EnvironmentVariableSecretResolver"/> (default) — reads
///     <c>${VAR_NAME}</c> placeholders from process environment variables.
///     Works with Docker secrets, Kubernetes secrets (mounted as env vars),
///     and CI/CD pipeline variable injection.
///   </description></item>
///   <item><description>
///     <see cref="MicrosoftConfigurationSecretResolver"/> — looks up placeholder
///     names as keys in <c>Microsoft.Extensions.Configuration.IConfiguration</c>.
///     Wire <c>IConfiguration</c> with the appropriate provider package and the
///     library will transparently pull secrets from that store:
///     <list type="bullet">
///       <item><description>Azure Key Vault — <c>Azure.Extensions.AspNetCore.Configuration.Secrets</c></description></item>
///       <item><description>AWS Secrets Manager — <c>Kralizek.Extensions.Configuration.AWSSecretsManager</c></description></item>
///       <item><description>HashiCorp Vault — <c>VaultSharp</c> with a custom <c>IConfigurationSource</c></description></item>
///       <item><description>.NET User Secrets — <c>Microsoft.Extensions.Configuration.UserSecrets</c> (dev only)</description></item>
///       <item><description>Windows DPAPI — via <c>Microsoft.AspNetCore.DataProtection</c> + a custom provider</description></item>
///     </list>
///   </description></item>
/// </list>
/// </summary>
public interface ISecretResolver
{
    /// <summary>
    /// Returns the resolved secret value for the given raw string from the
    /// config file. If the value is a <c>${PLACEHOLDER}</c> it should be
    /// looked up in the secure store; otherwise the original value is
    /// returned unchanged.
    /// </summary>
    string? Resolve(string? value);
}
