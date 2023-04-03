using System;
using MessagePack;
using MessagePack.Resolvers;

namespace Be.Vlaanderen.Basisregisters.Aws.DistributedS3Cache;

public sealed class S3CacheSerializer
{
    private static readonly Lazy<S3CacheSerializer> lazy = new(() => new S3CacheSerializer());
    private S3CacheSerializer()
    {
    }

    public CacheObject<T> DeserializeObject<T>(byte[] value, bool compression = true) where T : new()
    {
        var options = TypelessContractlessStandardResolver.Options;
        if (compression)
        {
            options = options.WithCompression(MessagePackCompression.Lz4Block);
        }
        return MessagePackSerializer.Deserialize<CacheObject<T>>(value, options);
    }

    public byte[] SerializeObject<T>(T value, bool compression = true) where T : new()
    {
        var options = TypelessContractlessStandardResolver.Options;
        if (compression)
        {
            options = options.WithCompression(MessagePackCompression.Lz4Block);
        }
        return MessagePackSerializer.Serialize(CacheObject<T>.Create(value), options);
    }

    public static S3CacheSerializer Serializer => lazy.Value;
}
