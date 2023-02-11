using System;
using EasyCompressor;
using MessagePack.Resolvers;

namespace Be.Vlaanderen.Basisregisters.Aws.DistributedS3Cache;

public sealed class S3CacheSerializer
{
    private readonly ICompressor _compressor;
    private static readonly Lazy<S3CacheSerializer> lazy = new Lazy<S3CacheSerializer>(() => new S3CacheSerializer());

    private S3CacheSerializer()
    {
        _compressor = new LZ4Compressor();
    }

    public CacheObject<T> DeserializeObject<T>(byte[] value, bool compression = true) where T : new()
    {
        byte[] input = compression ? _compressor.Decompress(value) : value;
        var cacheObject =
            MessagePack.MessagePackSerializer.Deserialize<CacheObject<T>>(input, ContractlessStandardResolver.Options);
        return cacheObject;
    }

    public byte[] SerializeObject<T>(T value, bool compression = true) where T : new()
    {
        var cacheObject = CacheObject<T>.Create(value);
        var serializedCacheObject =
            MessagePack.MessagePackSerializer.Serialize(cacheObject, ContractlessStandardResolver.Options);
        return compression ? _compressor.Compress(serializedCacheObject) : serializedCacheObject;
    }

    public static S3CacheSerializer Serializer => lazy.Value;
}
