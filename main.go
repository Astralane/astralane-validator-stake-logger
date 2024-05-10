package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"
	"time"
	"github.com/davecgh/go-spew/spew"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/nadoo/ipset"
	"gopkg.in/yaml.v3"
	"encoding/json"
)

type PeerNode struct {
	Pubkey     solana.PublicKey
	GossipIp   string
	GossipPort string
	TPUIp      string
	TPUPort    string
	Stake      uint64
}

type jsn struct {
	Cn string `json:"class_name"`
	Address string `json:"address"`
	Sp float64 `json:"stake_percent"`
	Sv uint64 `json:"stake_amount"`
	Pk string `json:"pubkey"`

}

var (
	flagConfigFile              = flag.String("config-file", "config.yml", "configuration file")
	flagPubkey                  = flag.String("pubkey", "", "validator-pubkey")
	flagRpcUri                  = flag.String("rpc-uri", "https://api.mainnet-beta.solana.com", "the rpc uri to use")
	flagRpcIdentity             = flag.Bool("fetch-identity", false, "fetch identity from rpc")
	flagOurLocalhost            = flag.Bool("our-localhost", false, "use localhost:8899 for rpc and fetch identity from that rpc")
	flagDefaultTPUPolicy        = flag.String("tpu-policy", "", "the default iptables policy for tpu, default is passthrough")
	flagDefaultTPUQuicPolicy    = flag.String("tpu-quic-policy", "", "the default iptables policy for quic tpu, default is passthrough")
	flagDefaultTPUQuicfwdPolicy = flag.String("tpu-quic-fwd-policy", "", "the default iptables policy for quic tpu fwd, default is passthrough")
	flagDefaultFWDPolicy        = flag.String("fwd-policy", "", "the default iptables policy for tpu forward, default is passthrough")
	flagDefaultVotePolicy       = flag.String("vote-policy", "", "the default iptables policy for votes, default is passthrough")
	flagUpdateIpSets            = flag.Bool("update", true, "whether or not to keep ipsets updated")
	flagSleep                   = flag.Duration("sleep", 10*time.Hour, "how long to sleep between updates")

	mangleChain       = "solana-nodes"
	filterChain       = "solana-tpu"
	filterChainCustom = "solana-tpu-custom"
	gossipSet         = "solana-gossip"

	quit = make(chan os.Signal)
)

type TrafficClass struct {
	Name   string  `yaml:"name"`
	Stake  float64 `yaml:"stake_percentage"` // If it has more than this stake
	FwMark uint64  `yaml:"fwmark,omitempty"`
}

type Config struct {
	Classes       []TrafficClass `yaml:"staked_classes"`
	UnstakedClass TrafficClass   `yaml:"unstaked_class"`
}


func cleanUp(c <-chan os.Signal, cfg *Config, validatorPorts *ValidatorPorts) {
	<-c

	log.Println("Cleaning up and deleting all sets and firewall rules")

	// Clean up
	ipset.Flush(gossipSet)
	ipset.Destroy(gossipSet)

	for _, set := range cfg.Classes {
		ipset.Flush(set.Name)
		ipset.Destroy(set.Name)
		//ipt.Delete("mangle", mangleChain, "-m", "set", "--match-set", set.Name, "src", "-j", "MARK", "--set-mark", "4")
	}

	// We didn't find the TPU port so we never added those rules



	log.Println("Finished cleaning up")

	os.Exit(1)
}

func reloadConfig(c <-chan os.Signal, cfg *Config) {
	<-c

	log.Println("Reloading configuration files")

	// @TODO reload configuration file
}

func setUpdate(c <-chan os.Signal) {
	<-c

	log.Println("Updating ipsets")
	// @TODO change the ipset
}

func main() {
	flag.Parse()

	// Set validator ports to nil to start with
	var validatorPorts *ValidatorPorts = nil

	// Load traffic classes
	f, err := os.Open(*flagConfigFile)
	if err != nil {
		log.Println("couldn't open config file", *flagConfigFile, err)
		os.Exit(1)
	}

	if *flagOurLocalhost {
		*flagRpcUri = "http://localhost:8899/"
		*flagRpcIdentity = true
	}

	// Load config file
	var cfg Config
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&cfg)
	if err != nil {
		log.Println("couldn't decode config file", *flagConfigFile, err)
		os.Exit(1)
	}

	// Special traffic class for unstaked nodes visible in gossip (e.g. RPC)
	cfg.UnstakedClass.Stake = -1
	cfg.Classes = append(cfg.Classes, cfg.UnstakedClass)

	// Sort the classes by stake weight
	sort.SliceStable(cfg.Classes, func(i, j int) bool {
		return cfg.Classes[i].Stake > cfg.Classes[j].Stake
	})
	f1,_:= os.Create("gossipset.json")
	defer f1.Close();
	f2,_:= os.Create("staked.json")
	defer f2.Close();
	f3,_:= os.Create("unstaked.json")
	defer f3.Close();
	var buff1 []jsn
	var buff2 []jsn
	var buff3 []jsn	
	// Connect to rpc
	client := rpc.New(*flagRpcUri)

	// Fetch identity
	if *flagRpcIdentity {
		out, err := client.GetIdentity(context.TODO())
		if err == nil {
			*flagPubkey = out.Identity.String()
			log.Println("loaded identity=", *flagPubkey)
		} else {
			log.Println("couldn't fetch validator identity, firewall will not by default handle tpu/tpufwd/vote ports", err)
		}
	}

	// Create iptables and ipset

	if err := ipset.Init(); err != nil {
		log.Println("error in ipset init", err)
		os.Exit(1)
	}

	// Clear the ipsets
	ipset.Create(gossipSet)
	ipset.Flush(gossipSet)
	for _, set := range cfg.Classes {
		ipset.Create(set.Name)
		ipset.Flush(set.Name)
	}

	// Clean up on signals
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	go cleanUp(c, &cfg, validatorPorts)

	cUSR1 := make(chan os.Signal, 1)
	signal.Notify(cUSR1, syscall.SIGUSR1)
	go setUpdate(cUSR1)

	cHUP := make(chan os.Signal, 1)
	signal.Notify(cHUP, syscall.SIGHUP)
	go reloadConfig(cHUP, &cfg)


	// Create mangle rules for all the classes


	// If there's no pubkey, then send all matching traffic to the filter chain and the mangle chain

	// Add the forwarding rules from the main filter chain to the custom rules one


	for { // loop
		var totalStake uint64 = 0
		var stakedPeers map[string]PeerNode

		// No need to update ipsets
		if *flagUpdateIpSets {
			log.Println("Updating stake weights")

			stakedNodes, err := client.GetVoteAccounts( // staked nodes = vote accounts
				context.TODO(),
				&rpc.GetVoteAccountsOpts{},
			)// assuming getting all vote accounts

			if err != nil {
				log.Println("couldn't load vote accounts nodes", err)
				time.Sleep(time.Second * 5)
				continue
			}

			// Current nodes
			stakedPeers = make(map[string]PeerNode)

			for _, node := range stakedNodes.Current {// finding the total stake from the above staked nodes
				totalStake += node.ActivatedStake

				// Don't add my self and don't add unstaked nodes
				if *flagPubkey != "" || flagPubkey != nil {// checks for invalid values 
					if node.NodePubkey.String() == *flagPubkey { // ignore ours 
						continue
					}
				}
				if node.ActivatedStake <= 0 {// skipping unstaked nodes aka 0 sol or less
					continue
				}

				stakedPeers[node.NodePubkey.String()] = PeerNode{
					Stake:  node.ActivatedStake,
					Pubkey: node.NodePubkey,
				} //adding friendly peers to stuct stakedPeers
			}

			// Delinquent nodes
			for _, node := range stakedNodes.Delinquent {// list of delinquent nodes
				totalStake += node.ActivatedStake

				// Don't add my self and don't add unstaked nodes
				if *flagPubkey != "" || flagPubkey != nil {
					if node.NodePubkey.String() == *flagPubkey {
						continue
					}
				}
				if node.ActivatedStake <= 0 {
					continue
				}

				stakedPeers[node.NodePubkey.String()] = PeerNode{ // adding delinquent nodes too ?
					Stake:  node.ActivatedStake,
					Pubkey: node.NodePubkey,
				}
			}
		}

		// Fetch the IPs for all the cluster nodes
		nodes, err := client.GetClusterNodes( // difference between peer nodes , cluster nodes
			context.TODO(),
		)

		if err != nil {
			log.Println("couldn't load cluster nodes", err)
			time.Sleep(time.Second * 5)
			continue
		}

		// @TODO if a node disappears from gossip, it would be good to remove it from the ipset
		// otherwise the ipsets will just continue to grow, samething for our own tpu host address
		// if we change IP.
		for _, node := range nodes { // going thorough all cluster nodes
			if *flagPubkey != "" {
				if *flagPubkey == node.Pubkey.String() {//if ours
					spew.Dump(node)
					// If this is our node, configure the TPU forwarding rules
					if node.TPU != nil {
						tpuAddr := *node.TPU // does forwarding rules below i think its safe to ignore this 
						quicTPUAddr := *node.TPUQUIC
						_, _, err := net.SplitHostPort(tpuAddr)
						_, _, errq := net.SplitHostPort(quicTPUAddr)
						if err == nil && errq == nil {
							if err == nil && errq == nil {
								if !(*flagUpdateIpSets) {
									// we've found our validator, let's not look at any other nodes
									break
								}
							} else {
								log.Println("couldn't load validator ports for your pubkey", err)
							}
						} else {
							log.Println("error parsing your validator ports", err)
						}
					}
				}
			}

			// If the node has a gossip address
			if node.Gossip != nil {
				// Currently add both TPU and Gossip addresses if both are listed
				// not sure if TPU would ever be different from gossip (assumption: not)
				var addresses []string
				gossip_host, _, err := net.SplitHostPort(*(node.Gossip))// adding gossip address
				if err != nil {
					spew.Dump(node.Gossip)
					log.Println("couldn't parse gossip host", *(node.Gossip), err)
					continue
				}
				addresses = append(addresses, gossip_host)

				if node.TPU != nil { // adding tpu address to address array
					tpu := *(node.TPU)
					if tpu != "" {
						tpu_host, _, err := net.SplitHostPort(tpu)
						if err == nil {
							if tpu_host != gossip_host {
								addresses = append(addresses, tpu_host)
							}
						} else {
							log.Println("couldn't parse tpu host", err)
						}
					}
				}

				// If this is a staked node i.e. listed in staked peers
				if val, ok := stakedPeers[node.Pubkey.String()]; ok {
					percent := float64(val.Stake) / float64(totalStake)

					added := false
					for _, class := range cfg.Classes {
						// Add to the highest class it matches
						for _, addr := range addresses { // going through all the addresses 
							ipset.Add(gossipSet, addr) // add all addresses to the gossipset
							tmp:=jsn{
								Cn:class.Name,
								Address:addr,
								Sp: percent,
								Sv: val.Stake,
								Pk: node.Pubkey.String(),
							}
							buff1=append(buff1,tmp)
							if percent > class.Stake && !added {
								// Add to the first class found, then set flag
								// so we don't add it to any lower staked classes
								ipset.Add(class.Name, addr) // final list
								added = true
								tmp:=jsn{
									Cn:class.Name,
									Address:addr,
									Sp: percent,
									Sv: val.Stake,
									Pk: node.Pubkey.String(),
								}
								buff2=append(buff2,tmp)
							} else {
								// Delete from all other clasess
								ipset.Del(class.Name, addr)
							}
						}
					}
				} else {
					// unstaked node add to the special unstaked class
					// and delete from all other classes
					for _, addr := range addresses {
						ipset.Add(gossipSet, addr) // add all addresses to the gossipset
						ipset.Add(cfg.UnstakedClass.Name, addr)
						tmp:=jsn{
							Cn:cfg.UnstakedClass.Name,
							Address:addr,
							Sp: 0,
							Sv: 0,
							Pk: node.Pubkey.String(),
						}
						buff3=append(buff3,tmp)
						for _, class := range cfg.Classes {
							if class.Name != cfg.UnstakedClass.Name {
								ipset.Del(class.Name, addr)
							}
						}
					}
				}
			} else {
				fmt.Println("not visible in gossip", node.Pubkey.String())
			}
		}
		if *flagUpdateIpSets {
			log.Println("updated ipsets: ", len(nodes), " visible in gossip and added to ipset")
		} else {
			log.Println("not updating ipsets")
		}

		// update every 10 secs
		out1,err:=json.Marshal(buff1)
		if err!=nil{
			log.Fatal(err);
		}
		out2,err:=json.Marshal(buff2)
		if err!=nil{
			log.Fatal(err);
		}
		out3,err:=json.Marshal(buff3)
		if err!=nil{
			log.Fatal(err);
		}
		
		t:=time.Now().UTC().Unix()
		f1.Write([]byte(strconv.FormatInt(t, 10)))
		f1.Write([]byte("\n"))
		f1.Write(out1)
		buff1=[]jsn{} 
		f2.Write([]byte(strconv.FormatInt(t, 10)))
		f2.Write([]byte("\n"))
		f2.Write(out2)
		buff2=[]jsn{}
		f3.Write([]byte(strconv.FormatInt(t, 10)))
		f3.Write([]byte("\n")) 
		f3.Write(out3)
		buff3=[]jsn{}
		time.Sleep(*flagSleep)
	}
}