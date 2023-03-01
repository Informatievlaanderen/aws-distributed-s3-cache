using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Amazon.S3;
using Amazon.S3.Transfer;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3.Model;

namespace Be.Vlaanderen.Basisregisters.Aws.DistributedS3Cache;

public class S3ClientHelper
{
    public const string HEAD_FILE = "HEAD";
    public const string PREV_FILE = "PREV";

    private readonly DistributedS3CacheOptions _options;
    private readonly IAmazonS3 _s3Client;

    public S3ClientHelper(IAmazonS3 s3Client, DistributedS3CacheOptions options)
    {
        _s3Client = s3Client;
        _options = options;

        if (string.IsNullOrWhiteSpace(options.Bucket) || string.IsNullOrWhiteSpace(options.RootDir))
        {
            throw new ArgumentException("invalid Bucket or RootDir name in DistributedS3CacheOptions");
        }
    }

    public async Task<byte[]> DownloadBlobAsync(string key, CancellationToken cancellationToken = default)
    {
        try
        {
            using var transferUtility = new TransferUtility(_s3Client);
            await using var stream = await transferUtility.OpenStreamAsync(_options.Bucket, $"{_options.RootDir}/{key}/{key}",
                cancellationToken);
            using var memoryStream = new MemoryStream();
            memoryStream.Seek(0, SeekOrigin.Begin);
            await stream.CopyToAsync(memoryStream, cancellationToken);
            return memoryStream.ToArray();
        }
        catch (AmazonS3Exception exception) when (
            exception.ErrorType == ErrorType.Sender
            && string.Equals(exception.ErrorCode, "NoSuchKey", StringComparison.OrdinalIgnoreCase))
        {
            return null!;
        }
    }

    public async Task MultipartUploadBlobAsync(string key, byte[] serializedObject, CancellationToken token = default)
    {
        var fullFileName = $"{_options.RootDir}/{key}/{key}";
        using var transferUtility = new TransferUtility(_s3Client);
        try
        {
            using (var stream = new MemoryStream(serializedObject))
            {
                stream.Seek(0, SeekOrigin.Begin);
                var request = new TransferUtilityUploadRequest()
                {
                    BucketName = _options.Bucket,
                    Key = fullFileName,
                    InputStream = stream,
                    ContentType = "application/octet-stream"
                };
                await transferUtility.UploadAsync(request, token);
                await UpdateCurrentHead(key, token);
            }
        }
        catch (AmazonS3Exception e)
        {
            Console.WriteLine("Error encountered on server. Message:'{0}' when writing an object", e.Message);
        }
        catch (Exception e)
        {
            Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
        }
    }

    private async Task UpdateCurrentHead(string key, CancellationToken token = default)
    {
        var prev = await S3ReadTextAsync(PREV_FILE, token);
        var currentHead = await S3ReadTextAsync(HEAD_FILE, token);
        if (!string.IsNullOrWhiteSpace(currentHead) && currentHead != prev)
        {
            await S3WriteTextAsync(PREV_FILE, currentHead, token);
        }
        await S3WriteTextAsync(HEAD_FILE, key, token);
    }

    private async Task S3WriteTextAsync(string name, string text, CancellationToken token = default)
    {
        string key = $"{_options.RootDir}/{name}";
        try
        {
            var putObjectRequest = new PutObjectRequest
            {
                BucketName = _options.Bucket,
                Key = key,
                ContentBody = text,
                ContentType = "text/plain"
            };
            await _s3Client.PutObjectAsync(putObjectRequest, token);
        }
        catch (AmazonS3Exception e)
        {
            Console.WriteLine(
                "Error encountered ***. Message:'{0}' when writing an object"
                , e.Message);
            throw;
        }
        catch (Exception e)
        {
            Console.WriteLine(
                "Unknown encountered on server. Message:'{0}' when writing an object"
                , e.Message);
            throw;
        }
    }

    public async Task<string> S3ReadTextAsync(string name, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _s3Client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = _options.Bucket,
                Key = $"{_options.RootDir}/{name}"
            }, cancellationToken);

            using StreamReader reader = new StreamReader(response.ResponseStream);
            return await reader.ReadToEndAsync();
        }
        catch (AmazonS3Exception exception) when (
            exception.ErrorType == ErrorType.Sender
            && string.Equals(exception.ErrorCode, "NoSuchKey", StringComparison.OrdinalIgnoreCase))
        {
            return null!;
        }
    }

    public async Task DeleteBlobAsync(string name, CancellationToken cancellationToken = default)
    {
        await _s3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = _options.Bucket,
            Key = $"{_options.RootDir}/{name}",
        }, cancellationToken);
    }

    public async Task PruneBlobsAsync(CancellationToken cancellationToken = default)
    {
        var keys = await ListUnreachableDirectoriesAsync(cancellationToken);
        foreach (var key in keys)
        {
            await _s3Client.DeleteObjectAsync(new DeleteObjectRequest
            {
                BucketName = _options.Bucket,
                Key = key,
            }, cancellationToken);
        }
    }

    private async Task<List<string>> ListUnreachableDirectoriesAsync(CancellationToken cancellationToken = default)
    {
        var excludedKeys = new List<string>
        {
            $"{_options.RootDir}/{HEAD_FILE}",
            $"{_options.RootDir}/{PREV_FILE}",
        };

        string head = await S3ReadTextAsync(HEAD_FILE, cancellationToken);
        if (!string.IsNullOrWhiteSpace(head))
        {
            excludedKeys.Add($"{_options.RootDir}/{head}/{head}");
        }

        string prev = await S3ReadTextAsync(PREV_FILE, cancellationToken);
        if (!string.IsNullOrWhiteSpace(prev))
        {
            excludedKeys.Add($"{_options.RootDir}/{prev}/{prev}");
        }

        excludedKeys = excludedKeys.Distinct().ToList();

        var response = await _s3Client.ListObjectsAsync(new ListObjectsRequest
        {
            BucketName = _options.Bucket,
            Prefix = $"{_options.RootDir}/",

        }, cancellationToken);


        var results = new List<string>();
        response.S3Objects.ForEach(i =>
        {
            var key = i.Key;
            if (excludedKeys.Contains(key))
            {
                return;
            }
            results.Add(key);
        });
        return results;
    }
}
