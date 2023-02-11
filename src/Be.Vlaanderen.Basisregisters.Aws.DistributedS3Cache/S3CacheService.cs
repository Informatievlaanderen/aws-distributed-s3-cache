using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;

namespace Be.Vlaanderen.Basisregisters.Aws.DistributedS3Cache;

public sealed class S3CacheService
{
    private readonly DistributedS3Cache _cache;

    public S3CacheService(DistributedS3Cache cache)
    {
        _cache = cache;
    }

    public async Task<string> GetHeadKey()
    {
        return await _cache.GetHeadAsync();
    }
    public async Task<string> GetPrevKey()
    {
        return await _cache.GetPrevAsync();
    }

    public async Task SetValue<T>(string key, T value, CancellationToken token = default) where T : new()
    {
        var obj = S3CacheSerializer.Serializer.SerializeObject(value, true);
        await _cache.SetAsync(key, obj, token);
    }
    public async Task<T> GetValue<T>(string key, CancellationToken token = default) where T : new()
    {
        var serializedObj = await _cache.GetAsync(key, token);
        var obj = S3CacheSerializer.Serializer.DeserializeObject<T>(serializedObj, true);
        return obj.Value;
    }

    public async Task<T> GetHeadValue<T>(CancellationToken token = default) where T : new()
    {
        string key = await GetHeadKey();
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new FileNotFoundException("Key not found");
        }

        return await GetValue<T>(key, token);
    }

    public async Task<T> GetPrevValue<T>(CancellationToken token = default) where T : new()
    {
        string key = await GetPrevKey();
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new FileNotFoundException("Key not found");
        }

        return await GetValue<T>(key, token);
    }

    private Task Prune()
    {
        throw new NotImplementedException();
    }
}
