using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;

namespace Orleans.Runtime;

internal class HistogramAggregator
{
    private readonly KeyValuePair<string, object>[] _tags;
    private readonly HistogramBucketAggregator[] _buckets;
    private long _count;
    private long _sum;

    public static IMetricLabelProvider LabelProvider { get; set; } = null;

    public HistogramAggregator(long[] buckets, KeyValuePair<string, object>[] tags, Func<long, KeyValuePair<string, object>> getLabel)
    {
        if (buckets[^1] != long.MaxValue)
        {
            buckets = buckets.Concat(new[] { long.MaxValue }).ToArray();
        }

        _tags = tags;
        _buckets = buckets.Select(b => new HistogramBucketAggregator(tags, b, getLabel(b))).ToArray();
    }

    public void Record(long number, object context = null)
    {
        int i;
        for (i = 0; i < _buckets.Length; i++)
        {
            if (number <= _buckets[i].Bound)
            {
                break;
            }
        }
        // Merge _tags (static, includes duration) with dynamic labels, but do not overwrite duration
        var tags = _tags;
        if (LabelProvider != null)
        {
            var extra = LabelProvider.GetLabels(context)?.ToArray();
            if (extra != null && extra.Length > 0)
            {
                // Remove any 'duration' from extra to avoid overwriting
                extra = extra.Where(kv => kv.Key.ToString() != "duration").ToArray();
                tags = _tags.Concat(extra).ToArray();
            }
        }
        _buckets[i].Add(1, tags);
        Interlocked.Increment(ref _count);
        Interlocked.Add(ref _sum, number);
    }

    public IEnumerable<Measurement<long>> CollectBuckets()
    {
        foreach (var bucket in _buckets)
        {
            yield return bucket.Collect();
        }
    }

    public Measurement<long> CollectCount() => new(_count, _tags);

    public Measurement<long> CollectSum() => new(_sum, _tags);
}

public static class HistogramAggregatorExtension
{
    public static void SetLabelProvider(IMetricLabelProvider provider)
    {
        HistogramAggregator.LabelProvider = provider;
    }
}
