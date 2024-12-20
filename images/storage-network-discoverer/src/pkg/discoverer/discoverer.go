/*
Copyright 2024 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package discoverer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"slices"
	"strings"
	"time"

	"k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"storage-network-controller/internal/config"
	"storage-network-controller/internal/logger"
	"storage-network-controller/internal/utils"
	"storage-network-controller/pkg/cache"
)

type discoveredIPs []string

const (
	// this type will be added to node's status.addresses field
	RVStorageIPType = "SDSRVStorageIP"
)

var DiscoveryCache *cache.TTLCache[string, string]

func DiscoveryLoop(ctx context.Context, cfg *config.Options, mgr manager.Manager) error {
	// just syntactic sugar for logger
	log := logger.FromContext(ctx)

	storageNetworks, err := parseCIDRs(cfg.StorageNetworkCIDR)
	if err != nil {
		log.Error(err, "Cannot parse storage-network-cidr")
		return err
	}

	myNodeName := os.Getenv("NODE_NAME")
	if myNodeName == "" {
		return errors.New("cannot get node name because no NODE_NAME env variable")
	}

	// we cannot do `mgr.GetClient()` because its require a cache, but we do not want cache feature
	// so, create our own kubeclient WITHOUT cache
	cl, err := client.New(mgr.GetConfig(), client.Options{})
	if err != nil {
		log.Error(err, "Cannot create k8s client")
		return err
	}

	log.Info("Discoverer settings:")
	log.Info(fmt.Sprintf("* Log level: %s", cfg.Loglevel))
	log.Info(fmt.Sprintf("* Discovery every %d seconds", cfg.DiscoverySec))
	log.Info(fmt.Sprintf("* Cache founded IP(s) for %d seconds", cfg.CacheTTLSec))
	log.Info(fmt.Sprintf("* Storage network CIDRs: %s", storageNetworks))
	log.Info(fmt.Sprintf("* I discovery IPs on node %s", myNodeName))

	// create a new DiscoveryCache with TTL (item expiring) capabilities
	DiscoveryCache = cache.NewTTL[string, string](ctx)

	// discoverer loop
	for {
		select {
		case <-ctx.Done():
			// do nothing in case of context canceling and just return from the loop
			return nil

		default:
			if err := discovery(ctx, myNodeName, storageNetworks, &cl, *cfg); err != nil {
				log.Error(err, "Discovery error occurred")
				return err
			}
		}

		time.Sleep(time.Duration(cfg.DiscoverySec) * time.Second)
	}
}

func parseCIDRs(cidrs config.StorageNetworkCIDR) ([]netip.Prefix, error) {
	networks := make([]netip.Prefix, len(cidrs))

	var err error

	for i, cidr := range cidrs {
		if networks[i], err = netip.ParsePrefix(cidr); err != nil {
			return nil, err
		}
	}

	return networks, nil
}

func discovery(ctx context.Context, nodeName string, storageNetworks []netip.Prefix, cl *client.Client, cfg config.Options) error {
	log := logger.FromContext(ctx)

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Error(err, "Cannot get network interfaces")
		return err
	}

	var foundedIP discoveredIPs

	for _, address := range addrs {
		// check the address type: it should be not a loopback and in a private range
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.IsPrivate() {
			if ipnet.IP.To4() != nil {
				ipStr := ipnet.IP.String()

				if utils.IPInCIDR(storageNetworks, ipStr, log) {
					log.Debug(fmt.Sprintf("IP '%s' found in CIDR blocks", ipStr))
					foundedIP = append(foundedIP, ipStr)
				} else {
					log.Trace(fmt.Sprintf("IP '%s' not founded in any CIDR blocks", ipStr))
				}
			}
		}
	}

	if len(foundedIP) > 0 {
		log.Info(fmt.Sprintf("Founded %d storage network IPs: %s", len(foundedIP), strings.Join(foundedIP, ", ")))

		// If there is 2 or more IPs founded we get only FIRST IP and warning about others
		if len(foundedIP) > 1 {
			log.Warning(fmt.Sprintf("Founded more than one storage IP: %s. Use first.", strings.Join(foundedIP, ", ")))
		}
		ip := foundedIP[0]

		// check node status only if no IP in cache
		if _, found := DiscoveryCache.Get(ip); !found {
			err := updateNodeStatusIfNeeded(ctx, nodeName, ip, *cl)
			if err != nil {
				log.Error(err, "cannot update node status field for now. Waiting for next reconciliation")
				return nil
			}
			DiscoveryCache.Set(ip, "", time.Duration(cfg.CacheTTLSec)*time.Second)
		}
	}

	return nil
}

func updateNodeStatusIfNeeded(ctx context.Context, nodeName string, ip string, cl client.Client) error {
	log := logger.FromContext(ctx)

	node := &v1.Node{}
	err := cl.Get(ctx, client.ObjectKey{
		Name: nodeName,
	}, node)

	if err != nil {
		log.Error(err, "cannot get my node info")
		return err
	}

	addresses := node.Status.Addresses

	// index of address with type SDSRVStorageIP (if will founded in node addresses)
	storageAddrIdx := slices.IndexFunc(addresses, func(addr v1.NodeAddress) bool { return addr.Type == RVStorageIPType })

	if storageAddrIdx == -1 {
		// no address on node status yet
		log.Trace(fmt.Sprintf("Append %s with IP %s to status.addresses", RVStorageIPType, ip))
		addresses = append(addresses, v1.NodeAddress{Type: RVStorageIPType, Address: ip})
	} else {
		// if is same address, then just return and do nothing
		if addresses[storageAddrIdx].Address == ip {
			return nil
		}

		// address already exists in node.status and it differrent from address in 'ip'
		log.Trace(fmt.Sprintf("Change %s from %s to %s in status.addresses", RVStorageIPType, addresses[storageAddrIdx].Address, ip))
		addresses[storageAddrIdx].Address = ip
	}

	log.Info(fmt.Sprintf("[updateNodeStatusIfNeeded] update node '%s' and set %s=%s", node.Name, RVStorageIPType, ip))

	node.Status.Addresses = addresses
	err = cl.Status().Update(ctx, node)

	if err != nil {
		log.Error(err, "cannot update node status addresses")
		return err
	}

	return nil
}
