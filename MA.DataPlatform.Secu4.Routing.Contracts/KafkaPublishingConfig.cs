// <copyright file="KafkaPublishingConfig.cs" company="McLaren Applied Ltd.">
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

namespace MA.DataPlatform.Secu4.Routing.Contracts;

public class KafkaPublishingConfig
{
    public KafkaPublishingConfig()
        : this("localhost:9092", -1, 0, 0, 0, 0)
    {
    }

    public KafkaPublishingConfig(
        string server,
        int compressionLevel,
        int compressionType,
        int linger,
        int queueMaxItem,
        int queueMaxSize)
    {
        this.Server = server;
        this.CompressionLevel = compressionLevel;
        this.CompressionType = compressionType;
        this.Linger = linger;
        this.QueueMaxItem = queueMaxItem;
        this.QueueMaxSize = queueMaxSize;
    }

    public string Server { get; }

    public int CompressionLevel { get; }

    public int CompressionType { get; }

    public int Linger { get; }

    public int QueueMaxItem { get; }

    public int QueueMaxSize { get; }
}

public class KafkaListeningConfig
{
    public KafkaListeningConfig()
        : this("localhost:9092", Guid.NewGuid().ToString(), AutoOffsetResetMode.Latest, null)
    {
    }

    public KafkaListeningConfig(string server, string groupId, AutoOffsetResetMode autoOffsetResetMode, long? offset)
    {
        this.Server = server;
        this.GroupId = groupId;
        this.AutoOffsetResetMode = autoOffsetResetMode;
        this.Offset = offset;
    }

    public string Server { get; }

    public string GroupId { get; }

    public AutoOffsetResetMode AutoOffsetResetMode { get; }

    public long? Offset { get; }
}

public enum AutoOffsetResetMode
{
    Latest = 0,
    Earliest = 1,
    FromOffset = 2,
}
