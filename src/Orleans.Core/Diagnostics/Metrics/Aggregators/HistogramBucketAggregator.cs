using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;

namespace Orleans.Runtime;

internal class HistogramBucketAggregator
{
    private long _value = 0;
    private KeyValuePair<string, object>[] _tags;
    public long Bound { get; }

    public HistogramBucketAggregator(KeyValuePair<string, object>[] tags, long bound, KeyValuePair<string, object> label)
    {
        _tags = tags.Concat(new[] { label }).ToArray();
        Bound = bound;
    }

    public ReadOnlySpan<KeyValuePair<string, object>> Tags => _tags;

    public long Value => _value;

    public void Add(long measurement, KeyValuePair<string, object>[] tags)
    {
        Interlocked.Add(ref _value, measurement);
        if (tags != null)
        {
            // Ensure 'duration' is always present
            var hasDuration = tags.Any(kv => kv.Key.ToString() == "duration");
            if (!hasDuration && _tags.Any(kv => kv.Key.ToString() == "duration"))
            {
                tags = tags.Concat(_tags.Where(kv => kv.Key.ToString() == "duration")).ToArray();
            }
            _tags = tags;
        }
    }

    public Measurement<long> Collect()
    {
        return new Measurement<long>(_value, _tags);
    }
}
