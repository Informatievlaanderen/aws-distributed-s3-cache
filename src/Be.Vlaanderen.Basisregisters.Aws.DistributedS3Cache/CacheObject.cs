namespace Be.Vlaanderen.Basisregisters.Aws.DistributedS3Cache;

using System;
using MessagePack;

public sealed class CacheObject<T> where T: new()
{
    public long UnixTimeStamp { get; set; }

    public T Value { get; set; } = default!;

    [IgnoreMember]
    public DateTime Timestamp => new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).AddSeconds(UnixTimeStamp).ToLocalTime();

    public static CacheObject<T> Create(T value) => new CacheObject<T>()
    {
        UnixTimeStamp = Convert.ToInt64(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1)).TotalSeconds),
        Value = value
    };
}
