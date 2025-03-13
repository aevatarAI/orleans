using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Orleans.Configuration;
using Orleans.Messaging;

namespace Orleans.Runtime.Membership
{
    public class ZooKeeperGatewayListProvider : IGatewayListProvider
    {
        private readonly ZooKeeperWatcher _watcher;

        /// <summary>
        /// The deployment connection string. for eg. "192.168.1.1,192.168.1.2/ClusterId"
        /// </summary>
        private readonly ZooKeeperBasedMembershipTable _zooKeeperBasedMembershipTable;
        private readonly IOptions<GatewayOptions> _gatewayOptions;

        public ZooKeeperGatewayListProvider(
            ZooKeeperBasedMembershipTable zooKeeperBasedMembershipTable,
            ILogger<ZooKeeperGatewayListProvider> logger,
            IOptions<GatewayOptions> gatewayOptions)
        {
            _watcher = new ZooKeeperWatcher(logger);
            
            _zooKeeperBasedMembershipTable = zooKeeperBasedMembershipTable;
            _gatewayOptions = gatewayOptions;
        }

        /// <summary>
        /// Initializes the ZooKeeper based gateway provider
        /// </summary>
        public Task InitializeGatewayListProvider() => Task.CompletedTask;

        /// <summary>
        /// Returns the list of gateways (silos) that can be used by a client to connect to Orleans cluster.
        /// The Uri is in the form of: "gwy.tcp://IP:port/Generation". See Utils.ToGatewayUri and Utils.ToSiloAddress for more details about Uri format.
        /// </summary>
        public async Task<IList<Uri>> GetGateways()
        {
            var membershipTableData = await _zooKeeperBasedMembershipTable.ReadAll();

            return membershipTableData
                .Members
                .Select(e => e.Item1)
                .Where(m => m.Status == SiloStatus.Active && m.ProxyPort != 0)
                .Select(m =>
                {
                    var endpoint = new IPEndPoint(m.SiloAddress.Endpoint.Address, m.ProxyPort);
                    var gatewayAddress = SiloAddress.New(endpoint, m.SiloAddress.Generation);

                    return gatewayAddress.ToGatewayUri();
                }).ToList();
        }

        /// <summary>
        /// Specifies how often this IGatewayListProvider is refreshed, to have a bound on max staleness of its returned information.
        /// </summary>
        public TimeSpan MaxStaleness => _gatewayOptions.Value.GatewayListRefreshPeriod;

        /// <summary>
        /// Specifies whether this IGatewayListProvider ever refreshes its returned information, or always returns the same gw list.
        /// (currently only the static config based StaticGatewayListProvider is not updatable. All others are.)
        /// </summary>
        public bool IsUpdatable => true;
    }
}
