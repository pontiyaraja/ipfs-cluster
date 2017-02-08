package ipfscluster

import (
	"errors"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-peer"
)

// RPCAPI is a go-libp2p-gorpc service which provides the internal ipfs-cluster
// API, which enables components and cluster peers to communicate and
// request actions from each other.
//
// The RPC API methods are usually redirects to the actual methods in
// the different components of ipfs-cluster, with very little added logic.
// Refer to documentation on those methods for details on their behaviour.
type RPCAPI struct {
	cluster *Cluster
}

// CidArg is an arguments that carry a Cid. It may carry more things in the
// future.
type CidArg struct {
	Cid string
}

// NewCidArg returns a CidArg which carries the given Cid. It panics if it is
// nil.
func NewCidArg(c *cid.Cid) *CidArg {
	if c == nil {
		panic("Cid cannot be nil")
	}
	return &CidArg{c.String()}
}

// CID decodes and returns a Cid from a CidArg.
func (arg *CidArg) CID() (*cid.Cid, error) {
	c, err := cid.Decode(arg.Cid)
	if err != nil {
		return nil, err
	}
	return c, nil
}

/*
   Cluster components methods
*/

// ID runs Cluster.ID()
func (api *RPCAPI) ID(in struct{}, out *IDSerial) error {
	id := api.cluster.ID().ToSerial()
	*out = id
	return nil
}

// Pin runs Cluster.Pin().
func (api *RPCAPI) Pin(in *CidArg, out *struct{}) error {
	c, err := in.CID()
	if err != nil {
		return err
	}
	return api.cluster.Pin(c)
}

// Unpin runs Cluster.Unpin().
func (api *RPCAPI) Unpin(in *CidArg, out *struct{}) error {
	c, err := in.CID()
	if err != nil {
		return err
	}
	return api.cluster.Unpin(c)
}

// PinList runs Cluster.Pins().
func (api *RPCAPI) PinList(in struct{}, out *[]string) error {
	cidList := api.cluster.Pins()
	cidStrList := make([]string, 0, len(cidList))
	for _, c := range cidList {
		cidStrList = append(cidStrList, c.String())
	}
	*out = cidStrList
	return nil
}

// Version runs Cluster.Version().
func (api *RPCAPI) Version(in struct{}, out *string) error {
	*out = api.cluster.Version()
	return nil
}

// Peers runs Cluster.Peers().
func (api *RPCAPI) Peers(in struct{}, out *[]IDSerial) error {
	peers := api.cluster.Peers()
	var sPeers []IDSerial
	for _, p := range peers {
		sPeers = append(sPeers, p.ToSerial())
	}
	*out = sPeers
	return nil
}

// PeerAdd runs Cluster.PeerAdd().
func (api *RPCAPI) PeerAdd(in MultiaddrSerial, out *IDSerial) error {
	addr := in.ToMultiaddr()
	id, err := api.cluster.PeerAdd(addr)
	*out = id.ToSerial()
	return err
}

// PeerRemove runs Cluster.PeerRm().
func (api *RPCAPI) PeerRemove(in peer.ID, out *struct{}) error {
	return api.cluster.PeerRemove(in)
}

// Join runs Cluster.Join().
func (api *RPCAPI) Join(in MultiaddrSerial, out *struct{}) error {
	addr := in.ToMultiaddr()
	err := api.cluster.Join(addr)
	return err
}

// StatusAll runs Cluster.StatusAll().
func (api *RPCAPI) StatusAll(in struct{}, out *[]GlobalPinInfo) error {
	pinfo, err := api.cluster.StatusAll()
	*out = pinfo
	return err
}

// Status runs Cluster.Status().
func (api *RPCAPI) Status(in *CidArg, out *GlobalPinInfo) error {
	c, err := in.CID()
	if err != nil {
		return err
	}
	pinfo, err := api.cluster.Status(c)
	*out = pinfo
	return err
}

// SyncAllLocal runs Cluster.SyncAllLocal().
func (api *RPCAPI) SyncAllLocal(in struct{}, out *[]PinInfo) error {
	pinfo, err := api.cluster.SyncAllLocal()
	*out = pinfo
	return err
}

// SyncLocal runs Cluster.SyncLocal().
func (api *RPCAPI) SyncLocal(in *CidArg, out *PinInfo) error {
	c, err := in.CID()
	if err != nil {
		return err
	}
	pinfo, err := api.cluster.SyncLocal(c)
	*out = pinfo
	return err
}

// SyncAll runs Cluster.SyncAll().
func (api *RPCAPI) SyncAll(in struct{}, out *[]GlobalPinInfo) error {
	pinfo, err := api.cluster.SyncAll()
	*out = pinfo
	return err
}

// Sync runs Cluster.Sync().
func (api *RPCAPI) Sync(in *CidArg, out *GlobalPinInfo) error {
	c, err := in.CID()
	if err != nil {
		return err
	}
	pinfo, err := api.cluster.Sync(c)
	*out = pinfo
	return err
}

// StateSync runs Cluster.StateSync().
func (api *RPCAPI) StateSync(in struct{}, out *[]PinInfo) error {
	pinfo, err := api.cluster.StateSync()
	*out = pinfo
	return err
}

// Recover runs Cluster.Recover().
func (api *RPCAPI) Recover(in *CidArg, out *GlobalPinInfo) error {
	c, err := in.CID()
	if err != nil {
		return err
	}
	pinfo, err := api.cluster.Recover(c)
	*out = pinfo
	return err
}

/*
   Tracker component methods
*/

// Track runs PinTracker.Track().
func (api *RPCAPI) Track(in *CidArg, out *struct{}) error {
	c, err := in.CID()
	if err != nil {
		return err
	}
	return api.cluster.tracker.Track(c)
}

// Untrack runs PinTracker.Untrack().
func (api *RPCAPI) Untrack(in *CidArg, out *struct{}) error {
	c, err := in.CID()
	if err != nil {
		return err
	}
	return api.cluster.tracker.Untrack(c)
}

// TrackerStatusAll runs PinTracker.StatusAll().
func (api *RPCAPI) TrackerStatusAll(in struct{}, out *[]PinInfo) error {
	*out = api.cluster.tracker.StatusAll()
	return nil
}

// TrackerStatus runs PinTracker.Status().
func (api *RPCAPI) TrackerStatus(in *CidArg, out *PinInfo) error {
	c, err := in.CID()
	if err != nil {
		return err
	}
	pinfo := api.cluster.tracker.Status(c)
	*out = pinfo
	return nil
}

// TrackerRecover runs PinTracker.Recover().
func (api *RPCAPI) TrackerRecover(in *CidArg, out *PinInfo) error {
	c, err := in.CID()
	if err != nil {
		return err
	}
	pinfo, err := api.cluster.tracker.Recover(c)
	*out = pinfo
	return err
}

/*
   IPFS Connector component methods
*/

// IPFSPin runs IPFSConnector.Pin().
func (api *RPCAPI) IPFSPin(in *CidArg, out *struct{}) error {
	c, err := in.CID()
	if err != nil {
		return err
	}
	return api.cluster.ipfs.Pin(c)
}

// IPFSUnpin runs IPFSConnector.Unpin().
func (api *RPCAPI) IPFSUnpin(in *CidArg, out *struct{}) error {
	c, err := in.CID()
	if err != nil {
		return err
	}
	return api.cluster.ipfs.Unpin(c)
}

// IPFSPinLsCid runs IPFSConnector.PinLsCid().
func (api *RPCAPI) IPFSPinLsCid(in *CidArg, out *IPFSPinStatus) error {
	c, err := in.CID()
	if err != nil {
		return err
	}
	b, err := api.cluster.ipfs.PinLsCid(c)
	*out = b
	return err
}

// IPFSPinLs runs IPFSConnector.PinLs().
func (api *RPCAPI) IPFSPinLs(in struct{}, out *map[string]IPFSPinStatus) error {
	m, err := api.cluster.ipfs.PinLs()
	*out = m
	return err
}

/*
   Consensus component methods
*/

// ConsensusLogPin runs Consensus.LogPin().
func (api *RPCAPI) ConsensusLogPin(in *CidArg, out *struct{}) error {
	c, err := in.CID()
	if err != nil {
		return err
	}
	return api.cluster.consensus.LogPin(c)
}

// ConsensusLogUnpin runs Consensus.LogUnpin().
func (api *RPCAPI) ConsensusLogUnpin(in *CidArg, out *struct{}) error {
	c, err := in.CID()
	if err != nil {
		return err
	}
	return api.cluster.consensus.LogUnpin(c)
}

// ConsensusLogAddPeer runs Consensus.LogAddPeer().
func (api *RPCAPI) ConsensusLogAddPeer(in MultiaddrSerial, out *struct{}) error {
	addr := in.ToMultiaddr()
	return api.cluster.consensus.LogAddPeer(addr)
}

// ConsensusLogRmPeer runs Consensus.LogRmPeer().
func (api *RPCAPI) ConsensusLogRmPeer(in peer.ID, out *struct{}) error {
	return api.cluster.consensus.LogRmPeer(in)
}

/*
   Peer Manager methods
*/

// PeerManagerAddPeer runs peerManager.addPeer().
func (api *RPCAPI) PeerManagerAddPeer(in MultiaddrSerial, out *struct{}) error {
	addr := in.ToMultiaddr()
	err := api.cluster.peerManager.addPeer(addr)
	return err
}

// PeerManagerAddFromMultiaddrs runs peerManager.addFromMultiaddrs().
func (api *RPCAPI) PeerManagerAddFromMultiaddrs(in MultiaddrsSerial, out *struct{}) error {
	addrs := in.ToMultiaddrs()
	err := api.cluster.peerManager.addFromMultiaddrs(addrs)
	return err
}

// PeerManagerRmPeerShutdown runs peerManager.rmPeer().
func (api *RPCAPI) PeerManagerRmPeerShutdown(in peer.ID, out *struct{}) error {
	return api.cluster.peerManager.rmPeer(in, true)
}

// PeerManagerRmPeer runs peerManager.rmPeer().
func (api *RPCAPI) PeerManagerRmPeer(in peer.ID, out *struct{}) error {
	return api.cluster.peerManager.rmPeer(in, false)
}

/*
   Other
*/

// RemoteMultiaddrForPeer returns the multiaddr of a peer as seen by this peer.
// This is necessary for a peer to figure out which of its multiaddresses the
// peers are seeing (also when crossing NATs). It should be called from
// the peer the IN parameter indicates.
func (api *RPCAPI) RemoteMultiaddrForPeer(in peer.ID, out *MultiaddrSerial) error {
	conns := api.cluster.host.Network().ConnsToPeer(in)
	if len(conns) == 0 {
		return errors.New("no connections to: " + in.Pretty())
	}
	*out = MultiaddrToSerial(multiaddrJoin(conns[0].RemoteMultiaddr(), in))
	return nil
}