using Amazon.S3;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;

namespace Be.Vlaanderen.Basisregisters.Aws.DistributedS3Cache;

public class DistributedS3Cache : IDistributedCache
{
    private readonly S3ClientHelper _s3ClientHelper;

    public DistributedS3Cache(IAmazonS3 s3Client, DistributedS3CacheOptions options)
    {
        _s3ClientHelper = new S3ClientHelper(s3Client, options);
    }

    public async Task<string> GetPrevAsync(CancellationToken token = default)
    {
        return await _s3ClientHelper.S3ReadTextAsync(S3ClientHelper.PREV_FILE, token);
    }

    public async Task<string> GetHeadAsync(CancellationToken token = default)
    {
        return await _s3ClientHelper.S3ReadTextAsync(S3ClientHelper.HEAD_FILE, token);
    }

    public byte[] Get(string key)
    {
        return _s3ClientHelper.DownloadBlobAsync(key).Result;
    }

    public async Task<byte[]> GetAsync(string key, CancellationToken token = default)
    {
        return await _s3ClientHelper.DownloadBlobAsync(key, token);
    }

    public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
    {
        _s3ClientHelper.MultipartUploadBlobAsync(key, value, CancellationToken.None).GetAwaiter().GetResult();
    }

    public async Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options,
        CancellationToken token = default)
    {
        await _s3ClientHelper.MultipartUploadBlobAsync(key, value, token);
    }

    public void Refresh(string key)
    {
        //Do nothing
    }

    public Task RefreshAsync(string key, CancellationToken token = default)
    {
        //Do nothing
        return Task.CompletedTask;
    }

    public void Remove(string key)
    {
        _s3ClientHelper.DeleteBlobAsync(key, CancellationToken.None).GetAwaiter().GetResult();
    }

    public async Task RemoveAsync(string key, CancellationToken token = default)
    {
        await _s3ClientHelper.DeleteBlobAsync(key, token);
    }

    public async Task PruneAsync(CancellationToken token = default)
    {
        await _s3ClientHelper.PruneBlobsAsync(token);
    }
}
