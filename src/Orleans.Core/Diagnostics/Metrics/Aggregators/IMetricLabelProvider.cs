using System.Collections.Generic;

namespace Orleans.Runtime;

/// <summary>
/// Provides additional labels for metrics at record time.
/// </summary>
public interface IMetricLabelProvider
{
    /// <summary>
    /// Returns additional labels to attach to a metric measurement.
    /// </summary>
    /// <param name="context">Optional context object (e.g., message, request, etc.)</param>
    /// <returns>Enumerable of key-value pairs for labels.</returns>
    IEnumerable<KeyValuePair<string, object>> GetLabels(object context = null);
} 