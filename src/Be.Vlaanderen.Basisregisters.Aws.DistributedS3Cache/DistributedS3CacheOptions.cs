namespace Be.Vlaanderen.Basisregisters.Aws.DistributedS3Cache;

using System;
using System.Diagnostics.CodeAnalysis;

public sealed class DistributedS3CacheOptions
{
    public string Bucket { get; set; } = String.Empty;
    public string RootDir { get; set; } = String.Empty;
}
