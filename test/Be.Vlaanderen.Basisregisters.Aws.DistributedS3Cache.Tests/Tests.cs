using System;
using System.Collections.Generic;
using System.Dynamic;
using Xunit;

namespace Be.Vlaanderen.Basisregisters.Aws.DistributedS3Cache.Tests;

public class DistributedCacheTests
{
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
        for (int i = 0; i < 5000000; i++)
        {
            largeObject.Add(sample);
        }

        return largeObject;
    }

    [Fact]
    public void SmallObjectSerializerWithCompressionTest()
    {
        var sample = GetObject(small: true);
        //Serialize
        var obj = S3CacheSerializer.Serializer.SerializeObject(sample);

        //Deserialize
        var results = S3CacheSerializer.Serializer.DeserializeObject<ExpandoObject>(obj).Value;
        Assert.Equal(sample, results);
    }

    [Fact]
    public void LargeObjectSerializerWithCompressionTest()
    {
        var sample = GetObject(small: false);
        //Serialize
        var obj = S3CacheSerializer.Serializer.SerializeObject(sample);

        //Deserialize
        var results = S3CacheSerializer.Serializer.DeserializeObject<List<ExpandoObject>>(obj).Value;
        Assert.Equal(sample, results);
    }

    [Fact]
    public void SmallObjectSerializerWithoutCompressionTest()
    {
        var sample = GetObject(small: true);
        //Serialize
        var obj = S3CacheSerializer.Serializer.SerializeObject(sample, compression: false);

        //Deserialize
        var results = S3CacheSerializer.Serializer.DeserializeObject<ExpandoObject>(obj, compression: false).Value;
        Assert.Equal(sample, results);
    }

    [Fact]
    public void LargeObjectSerializerWithoutCompressionTest()
    {
        var sample = GetObject(small: false);
        //Serialize
        var obj = S3CacheSerializer.Serializer.SerializeObject(sample, compression: false);

        //Deserialize
        var results = S3CacheSerializer.Serializer.DeserializeObject<List<ExpandoObject>>(obj, compression: false)
            .Value;
        Assert.Equal(sample, results);
    }
}
