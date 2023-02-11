using System;
using System.Diagnostics.CodeAnalysis;

namespace Be.Vlaanderen.Basisregisters.Aws.DistributedS3Cache;

public sealed class DistributedS3CacheOptions
{
    public string Bucket { get; set; } = String.Empty;
    public string RootDir { get; set; } = String.Empty;
}
