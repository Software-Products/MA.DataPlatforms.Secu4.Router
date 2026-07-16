// <copyright file="KafkaClientLogRouter.cs" company="Motion Applied Ltd.">
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

using MA.Common.Abstractions;

namespace MA.DataPlatforms.Secu4.Routing.Shared.Core;

/// <summary>
/// Routes librdkafka log and error callbacks to the application <see cref="ILogger"/>
/// so Kafka clients never write directly to stderr.
/// </summary>
public static class KafkaClientLogRouter
{
    public static void Route(ILogger logger, string clientDescription, LogMessage log)
    {
        var message = $"[{clientDescription}] {log.Facility}: {log.Message}";
        switch (log.Level)
        {
            case SyslogLevel.Emergency:
            case SyslogLevel.Alert:
            case SyslogLevel.Critical:
            case SyslogLevel.Error:
                logger.Error(message);
                break;
            case SyslogLevel.Warning:
            case SyslogLevel.Notice:
                logger.Warning(message);
                break;
            case SyslogLevel.Info:
                logger.Info(message);
                break;
            case SyslogLevel.Debug:
            default:
                logger.Debug(message);
                break;
        }
    }

    public static void Route(ILogger logger, string clientDescription, Error error)
    {
        var message = $"[{clientDescription}] {error.Code}: {error.Reason}";
        if (error.IsFatal)
        {
            logger.Error(message);
        }
        else
        {
            logger.Warning(message);
        }
    }
}
