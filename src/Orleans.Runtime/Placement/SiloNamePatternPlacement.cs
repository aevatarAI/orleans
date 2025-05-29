using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Orleans.Metadata;
using Orleans.Placement;
using Orleans.Runtime;
using Orleans.Runtime.Placement;
using Orleans.Statistics;
using Microsoft.Extensions.Logging;

namespace Orleans.Runtime.Placement
{
    /// <summary>
    /// A placement strategy which places grains on silos based on a name pattern.
    /// This allows for targeting silos whose names contain a specific substring.
    /// </summary>
    [Serializable, GenerateSerializer, Immutable, SuppressReferenceTracking]
    public sealed class SiloNamePatternPlacement : PlacementStrategy
    {
        /// <summary>
        /// Gets the singleton instance of this class.
        /// </summary>
        internal static SiloNamePatternPlacement Singleton { get; } = new SiloNamePatternPlacement();

        /// <summary>
        /// The property key used to store the silo name pattern in grain properties.
        /// </summary>
        public const string SiloNamePatternPropertyKey = "Orleans.Placement.SiloNamePattern";

        /// <summary>
        /// The property key used to store the maximum CPU usage threshold.
        /// </summary>
        public const string MaxCpuUsagePropertyKey = "Orleans.Placement.MaxCpuUsage";

        /// <summary>
        /// The property key used to store the maximum memory usage threshold.
        /// </summary>
        public const string MaxMemoryUsagePropertyKey = "Orleans.Placement.MaxMemoryUsage";

        /// <summary>
        /// Gets or sets the silo name pattern used to match against silo names.
        /// Grain will be activated on a silo whose name begins with this pattern.
        /// </summary>
        [Id(0)]
        public string SiloNamePattern { get; private set; }

        /// <summary>
        /// Gets or sets the maximum CPU usage threshold in percentage (0-100).
        /// Silos with CPU usage above this threshold will be excluded.
        /// </summary>
        [Id(1)]
        public float MaxCpuUsage { get; private set; } = 60f; // Default to 60%

        /// <summary>
        /// Gets or sets the maximum memory usage threshold in percentage (0-100).
        /// Silos with memory usage above this threshold will be excluded.
        /// </summary>
        [Id(2)]
        public float MaxMemoryUsage { get; private set; } = 60f; // Default to 60%

        /// <summary>
        /// Initializes a new instance of the <see cref="SiloNamePatternPlacement"/> class.
        /// </summary>
        public SiloNamePatternPlacement()
        {
        }

        /// <summary>
        /// Creates a new instance of the <see cref="SiloNamePatternPlacement"/> class with the specified silo name pattern.
        /// </summary>
        /// <param name="siloNamePattern">The pattern to match against silo names. Grain will be activated on a silo whose name begins with this pattern.</param>
        /// <param name="maxCpuUsage">The maximum CPU usage threshold (0-100%). Default is 60%.</param>
        /// <param name="maxMemoryUsage">The maximum memory usage threshold (0-100%). Default is 60%.</param>
        /// <returns>A new instance of <see cref="SiloNamePatternPlacement"/> with the specified parameters.</returns>
        public static SiloNamePatternPlacement Create(
            string siloNamePattern, 
            float maxCpuUsage = 60f, 
            float maxMemoryUsage = 60f)
        {
            var instance = new SiloNamePatternPlacement
            {
                SiloNamePattern = siloNamePattern,
                MaxCpuUsage = maxCpuUsage,
                MaxMemoryUsage = maxMemoryUsage
            };
            return instance;
        }

        /// <summary>
        /// Initializes an instance of this type using the provided grain properties.
        /// </summary>
        /// <param name="properties">The grain properties.</param>
        public override void Initialize(GrainProperties properties)
        {
            base.Initialize(properties);
            
            // Extract silo name pattern from grain properties if available
            if (properties.Properties.TryGetValue(SiloNamePatternPropertyKey, out var siloNamePattern))
            {
                SiloNamePattern = siloNamePattern;
            }
            
            // Extract resource thresholds if available
            if (properties.Properties.TryGetValue(MaxCpuUsagePropertyKey, out var maxCpuUsageStr) &&
                float.TryParse(maxCpuUsageStr, out var maxCpuUsage))
            {
                MaxCpuUsage = maxCpuUsage;
            }
            
            if (properties.Properties.TryGetValue(MaxMemoryUsagePropertyKey, out var maxMemoryUsageStr) &&
                float.TryParse(maxMemoryUsageStr, out var maxMemoryUsage))
            {
                MaxMemoryUsage = maxMemoryUsage;
            }
        }

        /// <summary>
        /// Populates grain properties to specify the preferred placement strategy.
        /// </summary>
        /// <param name="services">The service provider.</param>
        /// <param name="grainClass">The grain class.</param>
        /// <param name="grainType">The grain type.</param>
        /// <param name="properties">The grain properties which will be populated by this method call.</param>
        public override void PopulateGrainProperties(IServiceProvider services, Type grainClass, GrainType grainType, Dictionary<string, string> properties)
        {
            base.PopulateGrainProperties(services, grainClass, grainType, properties);
            
            // Store the silo name pattern in grain properties
            if (!string.IsNullOrWhiteSpace(SiloNamePattern))
            {
                properties[SiloNamePatternPropertyKey] = SiloNamePattern;
            }
            
            // Store resource thresholds
            properties[MaxCpuUsagePropertyKey] = MaxCpuUsage.ToString();
            properties[MaxMemoryUsagePropertyKey] = MaxMemoryUsage.ToString();
        }
    }

    /// <summary>
    /// A placement director which places grain activations on silos whose names match a specified pattern
    /// and filters out silos with CPU or memory usage exceeding specified thresholds.
    /// </summary>
    internal class SiloNamePatternPlacementDirector : IPlacementDirector, ISiloStatisticsChangeListener
    {
        private readonly ISiloStatusOracle _siloStatusOracle;
        private readonly GrainPropertiesResolver _grainPropertiesResolver;
        private readonly Dictionary<SiloAddress, SiloRuntimeStatistics> _siloStatistics = 
            new Dictionary<SiloAddress, SiloRuntimeStatistics>();
        private readonly ILogger<SiloNamePatternPlacementDirector> _logger;
        
        // Resource-related constants for score computation
        private const float CpuWeight = 0.6f;
        private const float MemoryWeight = 0.1f;
        private const float ActivationCountWeight = 0.3f;
        
        /// <summary>
        /// Initializes a new instance of the <see cref="SiloNamePatternPlacementDirector"/> class.
        /// </summary>
        /// <param name="siloStatusOracle">The silo status oracle.</param>
        /// <param name="grainPropertiesResolver">The grain properties resolver.</param>
        /// <param name="deploymentLoadPublisher">The deployment load publisher.</param>
        /// <param name="logger">The logger.</param>
        public SiloNamePatternPlacementDirector(
            ISiloStatusOracle siloStatusOracle,
            GrainPropertiesResolver grainPropertiesResolver,
            DeploymentLoadPublisher deploymentLoadPublisher,
            ILogger<SiloNamePatternPlacementDirector> logger) 
        {
            _siloStatusOracle = siloStatusOracle ?? throw new ArgumentNullException(nameof(siloStatusOracle));
            _grainPropertiesResolver = grainPropertiesResolver ?? throw new ArgumentNullException(nameof(grainPropertiesResolver));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            
            // Subscribe to silo statistics events
            deploymentLoadPublisher?.SubscribeToStatisticsChangeEvents(this);
        }

        /// <summary>
        /// Called when silo statistics change.
        /// </summary>
        public void SiloStatisticsChangeNotification(SiloAddress address, SiloRuntimeStatistics statistics)
        {
            _siloStatistics[address] = statistics;
        }

        /// <summary>
        /// Called when a silo is removed.
        /// </summary>
        public void RemoveSilo(SiloAddress address)
        {
            _siloStatistics.Remove(address);
        }
        
        /// <summary>
        /// Helper struct to track resource statistics for silos with information needed for scoring.
        /// </summary>
        private readonly record struct ResourceStatistics(
            bool IsOverloaded, 
            float CpuUsage, 
            float MemoryUsage, 
            float AvailableMemory, 
            float MaxAvailableMemory, 
            int ActivationCount)
        {
            /// <summary>
            /// Creates a ResourceStatistics instance from a SiloRuntimeStatistics instance.
            /// </summary>
            public static ResourceStatistics FromRuntime(SiloRuntimeStatistics statistics)
            {
                return new ResourceStatistics(
                    IsOverloaded: statistics.IsOverloaded,
                    CpuUsage: statistics.EnvironmentStatistics.CpuUsagePercentage,
                    MemoryUsage: statistics.EnvironmentStatistics.MemoryUsageBytes,
                    AvailableMemory: statistics.EnvironmentStatistics.AvailableMemoryBytes,
                    MaxAvailableMemory: statistics.EnvironmentStatistics.MaximumAvailableMemoryBytes,
                    ActivationCount: statistics.ActivationCount);
            }
        }

        /// <summary>
        /// Selects the optimal silo from the matching silos based on resource usage metrics.
        /// </summary>
        /// <param name="matchingSilos">The list of matching silos to select from.</param>
        /// <param name="maxCpuUsage">The maximum CPU usage threshold.</param>
        /// <param name="maxMemoryUsage">The maximum memory usage threshold.</param>
        /// <returns>The selected silo address.</returns>
        private SiloAddress SelectOptimalSiloByResources(List<SiloAddress> matchingSilos, float maxCpuUsage, float maxMemoryUsage)
        {
            var stopwatch = Stopwatch.StartNew();
            int candidateCount = matchingSilos.Count;
            // If no statistics available or only one silo, use random selection
            if (_siloStatistics.Count == 0 || candidateCount == 1)
            {
                var result = matchingSilos[Random.Shared.Next(candidateCount)];
                stopwatch.Stop();
                _logger.LogInformation("[SelectOptimalSiloByResources] Insufficient statistics or only one candidate, returning directly. Total elapsed: {Elapsed}ms, Candidates: {Candidates}, Selected: {Selected}", stopwatch.Elapsed.TotalMilliseconds, candidateCount, result);
                return result;
            }
            // 1st for-loop: 过滤有效silo
            var validSilos = new List<(SiloAddress Address, ResourceStatistics Stats)>();
            int maxActivationCount = 0;
            float maxMaxAvailableMemory = 0;
            var filterLoopWatch = Stopwatch.StartNew();
            foreach (var siloAddress in matchingSilos)
            {
                if (_siloStatistics.TryGetValue(siloAddress, out var stats))
                {
                    var resourceStats = ResourceStatistics.FromRuntime(stats);
                    // Skip overloaded silos or those exceeding thresholds
                    if (resourceStats.IsOverloaded || 
                        resourceStats.CpuUsage > maxCpuUsage || 
                        (resourceStats.MaxAvailableMemory > 0 && 
                         resourceStats.MemoryUsage / resourceStats.MaxAvailableMemory * 100 > maxMemoryUsage))
                    {
                        continue;
                    }
                    validSilos.Add((siloAddress, resourceStats));
                    // Track max values for normalization
                    if (resourceStats.MaxAvailableMemory > maxMaxAvailableMemory)
                    {
                        maxMaxAvailableMemory = resourceStats.MaxAvailableMemory;
                    }
                    if (resourceStats.ActivationCount > maxActivationCount)
                    {
                        maxActivationCount = resourceStats.ActivationCount;
                    }
                }
            }
            filterLoopWatch.Stop();
            int validCount = validSilos.Count;
            // If no valid silos after filtering, fall back to all matching silos
            if (validCount == 0)
            {
                stopwatch.Stop();
                _logger.LogWarning("[SelectOptimalSiloByResources] No valid silos after filtering, returning random. Total elapsed: {Elapsed}ms, Candidates: {Candidates}", stopwatch.Elapsed.TotalMilliseconds, candidateCount);
                return matchingSilos[Random.Shared.Next(candidateCount)];
            }
            // If only one valid silo, return it
            if (validCount == 1)
            {
                stopwatch.Stop();
                _logger.LogInformation("[SelectOptimalSiloByResources] Only one valid silo, returning directly. Total elapsed: {Elapsed}ms, Candidates: {Candidates}, Valids: {Valids}, Selected: {Selected}", stopwatch.Elapsed.TotalMilliseconds, candidateCount, validCount, validSilos[0].Address);
                return validSilos[0].Address;
            }
            var scoreLoopWatch = Stopwatch.StartNew();
            (SiloAddress Address, float Score) bestSilo = (validSilos[0].Address, float.MaxValue);
            foreach (var (address, stats) in validSilos)
            {
                float score = CalculateScore(in stats, maxMaxAvailableMemory, maxActivationCount);
                // Add small random jitter to avoid always picking the same silo when scores are equal
                float jitter = Random.Shared.NextSingle() / 100_000f;
                if (score + jitter < bestSilo.Score)
                {
                    bestSilo = (address, score + jitter);
                }
            }
            scoreLoopWatch.Stop();
            stopwatch.Stop();
            _logger.LogInformation(
                "[SelectOptimalSiloByResources] Selection complete. Total elapsed: {TotalElapsed}ms, Filter loop: {FilterElapsed}ms, Score loop: {ScoreElapsed}ms, Candidates: {Candidates}, Valids: {Valids}, Best: {Best}",
                stopwatch.Elapsed.TotalMilliseconds,
                filterLoopWatch.Elapsed.TotalMilliseconds,
                scoreLoopWatch.Elapsed.TotalMilliseconds,
                candidateCount,
                validCount,
                bestSilo.Address);
            return bestSilo.Address;
        }
        
        /// <summary>
        /// Calculates a score for a silo based on its resource statistics.
        /// Lower score means better placement target (less resource usage).
        /// </summary>
        private float CalculateScore(in ResourceStatistics stats, float maxMaxAvailableMemory, int maxActivationCount)
        {
            // Normalize CPU usage (0-100%)
            float normalizedCpuUsage = stats.CpuUsage / 100f;
            float score = CpuWeight * normalizedCpuUsage;
            
            // Add memory-related components if memory information is available
            if (stats.MaxAvailableMemory > 0)
            {
                float maxAvailableMemory = stats.MaxAvailableMemory; // cache locally
                
                // Normalized memory usage (0-1)
                float normalizedMemoryUsage = stats.MemoryUsage / maxAvailableMemory;
                
                // Normalized memory availability (0-1), reversed so higher is worse
                float normalizedAvailableMemory = 1 - stats.AvailableMemory / maxAvailableMemory;
                
                // Add memory component to score
                score += MemoryWeight * normalizedMemoryUsage;
            }
            
            // Add activation count component if we have valid max
            if (maxActivationCount > 0)
            {
                score += ActivationCountWeight * ((float)stats.ActivationCount / maxActivationCount);
            }
            
            Debug.Assert(score >= 0f && score <= 1.01f, "Score should be between 0 and 1");
            return score;
        }

        /// <summary>
        /// Picks an appropriate silo to place the specified target on based on the silo name pattern
        /// and resource utilization thresholds.
        /// </summary>
        /// <param name="strategy">The target's placement strategy.</param>
        /// <param name="target">The grain being placed as well as information about the request which triggered the placement.</param>
        /// <param name="context">The placement context.</param>
        /// <returns>An appropriate silo to place the specified target on.</returns>
        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            // Get pattern from grain properties
            string siloNamePattern = null;
            float maxCpuUsage = 60f;
            float maxMemoryUsage = 60f;
            
            if (_grainPropertiesResolver.TryGetGrainProperties(target.GrainIdentity.Type, out var properties))
            {
                // Try to get pattern from properties
                if (properties.Properties.TryGetValue(SiloNamePatternPlacement.SiloNamePatternPropertyKey, out var pattern))
                {
                    siloNamePattern = pattern;
                }
                
                // Try to get resource thresholds from properties
                if (properties.Properties.TryGetValue(SiloNamePatternPlacement.MaxCpuUsagePropertyKey, out var cpuStr) &&
                    float.TryParse(cpuStr, out var cpu))
                {
                    maxCpuUsage = cpu;
                }
                
                if (properties.Properties.TryGetValue(SiloNamePatternPlacement.MaxMemoryUsagePropertyKey, out var memStr) &&
                    float.TryParse(memStr, out var mem))
                {
                    maxMemoryUsage = mem;
                }
            }

            if (string.IsNullOrWhiteSpace(siloNamePattern))
            {
                throw new OrleansException($"SiloNamePatternPlacement strategy requires a valid silo name pattern. " +
                                            $"Current pattern: '{siloNamePattern}'");
            }

            var compatibleSilos = context.GetCompatibleSilos(target);

            // If a valid placement hint was specified, use it.
            if (IPlacementDirector.GetPlacementHint(target.RequestContextData, compatibleSilos) is { } placementHint)
            {
                return Task.FromResult(placementHint);
            }

            // Find all active silos whose names contain the specified pattern
            var matchingSilos = new List<SiloAddress>();
            // Iterate over compatible silos directly (likely fewer items)
            foreach (var siloAddress in compatibleSilos)
            {
                // Only check status/name for compatible silos
                if (_siloStatusOracle.TryGetSiloName(siloAddress, out var siloName) &&
                    siloName.StartsWith(siloNamePattern, StringComparison.OrdinalIgnoreCase))
                {
                    matchingSilos.Add(siloAddress);
                }
            }

            if (matchingSilos.Count == 0)
            {
                // If no matching silos found, throw an exception
                throw new OrleansException($"No silos matching pattern '{siloNamePattern}' found. Available silos: {string.Join(", ", compatibleSilos.Select(s => s.ToString()))}");
            }

            // Select an optimal silo using resource-based metrics instead of random selection
            var selectedSilo = SelectOptimalSiloByResources(matchingSilos, maxCpuUsage, maxMemoryUsage);
            return Task.FromResult(selectedSilo);
        }
    }

    /// <summary>
    /// Attribute used to specify that a grain should be placed on silos whose names match a specified pattern.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class SiloNamePatternPlacementAttribute : PlacementAttribute
    {
        /// <summary>
        /// Gets the name pattern to match silos. Grain will be activated on a silo whose name begins with this pattern.
        /// </summary>
        public string SiloNamePattern { get; }

        /// <summary>
        /// Gets the maximum CPU usage threshold (0-100%).
        /// </summary>
        public float MaxCpuUsage { get; }
        
        /// <summary>
        /// Gets the maximum memory usage threshold (0-100%).
        /// </summary>
        public float MaxMemoryUsage { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SiloNamePatternPlacementAttribute"/> class.
        /// </summary>
        /// <param name="siloNamePattern">The pattern to match silo names. Grain will be activated on a silo whose name begins with this pattern.</param>
        public SiloNamePatternPlacementAttribute(string siloNamePattern)
            : base(SiloNamePatternPlacement.Create(siloNamePattern))
        {
            SiloNamePattern = siloNamePattern ?? throw new ArgumentNullException(nameof(siloNamePattern));
            MaxCpuUsage = 60f; // Default
            MaxMemoryUsage = 60f; // Default
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SiloNamePatternPlacementAttribute"/> class with resource thresholds.
        /// </summary>
        /// <param name="siloNamePattern">The pattern to match silo names. Grain will be activated on a silo whose name begins with this pattern.</param>
        /// <param name="maxCpuUsage">The maximum CPU usage threshold (0-100%).</param>
        /// <param name="maxMemoryUsage">The maximum memory usage threshold (0-100%).</param>
        public SiloNamePatternPlacementAttribute(
            string siloNamePattern, 
            float maxCpuUsage, 
            float maxMemoryUsage)
            : base(SiloNamePatternPlacement.Create(siloNamePattern, maxCpuUsage, maxMemoryUsage))
        {
            SiloNamePattern = siloNamePattern ?? throw new ArgumentNullException(nameof(siloNamePattern));
            MaxCpuUsage = maxCpuUsage;
            MaxMemoryUsage = maxMemoryUsage;
        }
    }
} 