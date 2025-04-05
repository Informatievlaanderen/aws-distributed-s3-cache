namespace Be.Vlaanderen.Basisregisters.Aws.DistributedS3Cache.Tests;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Xunit;
using Xunit.Abstractions;

public class DistributedCacheTests
{
    private readonly ITestOutputHelper _output;

    public DistributedCacheTests(ITestOutputHelper output)
    {
        this._output = output;
    }

    [Fact]
    public void SmallObjectSerializerWithCompressionTest()
    {
        var sample = GetObject(small: true);
        var ret = BenchMarkSerializer(() => S3CacheSerializer.Serializer.SerializeObject(sample));

        //Serialize
        var obj = ret.Response;

        //Deserialize
        var results = S3CacheSerializer.Serializer.DeserializeObject<ExpandoObject>(obj).Value;
        Assert.Equal(sample, results);
    }

    [Fact]
    public void LargeObjectSerializerWithCompressionTest()
    {
        var sample = GetObject(small: false);
        //Serialize
        var benchMarkSerializer = BenchMarkSerializer(() => S3CacheSerializer.Serializer.SerializeObject(sample, compression: true));
        _output.WriteLine($"BenchMark Serializer Large Object With Compression \n Size: {benchMarkSerializer.Mb}MB Seconds: {benchMarkSerializer.Seconds}");

        //Deserialize
        var benchMarkDeserializer = BenchMark(() =>
            S3CacheSerializer.Serializer.DeserializeObject<List<ExpandoObject>>(benchMarkSerializer.Response,
                compression: true));
        _output.WriteLine($"BenchMark Deserializer Large Object With Compression \n Seconds: {benchMarkDeserializer.Seconds}");

        Assert.Equal(sample, benchMarkDeserializer.Response.Value);
    }

    [Fact]
    public void SmallObjectSerializerWithoutCompressionTest()
    {
        var sample = GetObject(small: true);
        var ret = BenchMarkSerializer(() => S3CacheSerializer.Serializer.SerializeObject(sample, compression: false));

        //Serialize
        var obj = ret.Response;

        //Deserialize
        var results = S3CacheSerializer.Serializer.DeserializeObject<ExpandoObject>(obj, compression: false).Value;
        Assert.Equal(sample, results);
    }

    [Fact]
    public void LargeObjectSerializerWithoutCompressionTest()
    {
        var sample = GetObject(small: false);

        //Serialize
        var benchMarkSerializer = BenchMarkSerializer(() => S3CacheSerializer.Serializer.SerializeObject(sample, compression: false));
        _output.WriteLine($"BenchMark Serializer Large Object Without Compression \n Size: {benchMarkSerializer.Mb}MB Seconds: {benchMarkSerializer.Seconds}");

        //Deserialize
        var benchMarkDeserializer = BenchMark(() =>
            S3CacheSerializer.Serializer.DeserializeObject<List<ExpandoObject>>(benchMarkSerializer.Response,
                compression: false));
        _output.WriteLine($"BenchMark Deserializer Large Object Without Compression \n Seconds: {benchMarkDeserializer.Seconds}");

        Assert.Equal(sample, benchMarkDeserializer.Response.Value);
    }

    [Theory (Skip = "Requires AWS Credentials")]
    [InlineData("AWS_ACCESS_KEY","AWS_ACCESS_KEY_SECRET","BUCKET_NAME","ROOT_DIRECTORYNAME")]
    public async Task PruneOldSnapshots(string accessKey, string secret, string bucket, string rootDir)
    {
        var options = new DistributedS3CacheOptions
        {
            Bucket = bucket,
            RootDir = rootDir
        };
        var s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secret), RegionEndpoint.EUWest1);
        var service = new S3CacheService(new DistributedS3Cache(s3Client, options));
        await service.Prune();
    }

    [Theory (Skip = "Requires AWS Credentials")]
    [InlineData("AWS_ACCESS_KEY","AWS_ACCESS_KEY_SECRET","BUCKET_NAME","ROOT_DIRECTORYNAME")]
    public void Generate5Snapshots(string accessKey, string secret, string bucket, string rootDir)
    {
        var options = new DistributedS3CacheOptions
        {
            Bucket = bucket,
            RootDir = rootDir
        };
        var s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secret), RegionEndpoint.EUWest1);
        var service = new S3CacheService(new DistributedS3Cache(s3Client, options));

        var largeObject = GetObject(small: false);

        for (int i = 1; i < 6; i++)
        {
            //Push snapshot to S3
            var counter = i;
            var seconds = BenchMark(() => service.SetValue($"{counter}", largeObject).GetAwaiter().GetResult());
            _output.WriteLine($"Snapshot with key {i} with compression to S3 done in {seconds} seconds");
        }
    }

    private (byte[] Response, float Mb, double Seconds) BenchMarkSerializer(Func<byte[]> func)
    {
        var stopwatch = new Stopwatch();
        stopwatch.Start();
        var results = func();
        stopwatch.Stop();
        (byte[] Response, float Mb, double Seconds) ret = new(results, (results.Length / 1024f / 1024f), stopwatch.Elapsed.TotalSeconds);
        return ret;
    }

    private (T Response, double Seconds) BenchMark<T>(Func<T> func)
    {
        var stopwatch = new Stopwatch();
        stopwatch.Start();
        var results = func();
        stopwatch.Stop();
        (T Response, double Seconds) ret = new(results, stopwatch.Elapsed.TotalSeconds);
        return ret;

    }

    private double BenchMark(Action action)
    {
        var stopwatch = new Stopwatch();
        stopwatch.Start();
        action();
        stopwatch.Stop();
        return stopwatch.Elapsed.TotalSeconds;

    }

    private object GetObject(bool small = true)
    {
        dynamic sample = new ExpandoObject();
        sample.Name = "Yusuf";
        sample.Age = 26;
        sample.SomeDate = DateTime.UtcNow;
        sample.IsMale = true;


        if (small)
        {
            return sample;
        }

        var largeObject = new List<dynamic>();
        for (int i = 0; i < 50000; i++)
        {
            largeObject.Add(sample);
        }

        return largeObject;
    }
}
