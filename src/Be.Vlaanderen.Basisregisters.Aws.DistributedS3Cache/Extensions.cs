namespace Be.Vlaanderen.Basisregisters.Aws.DistributedS3Cache;

using Amazon.S3;
using Microsoft.Extensions.DependencyInjection;

public static class Extensions
{
    public static void RegisterDistributedS3Cache(this IServiceCollection services, IAmazonS3 s3Client ,DistributedS3CacheOptions options)
    {
        services.AddSingleton(_ => new DistributedS3Cache(s3Client, options));
        services.AddTransient<S3CacheService>();
    }

}
