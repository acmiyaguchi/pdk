package network

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/pilosa/pdk"
	"github.com/pilosa/pilosa"
)

type Main struct {
	Iface         string
	Filename      string
	Snaplen       int32
	Promisc       bool
	Timeout       time.Duration
	NumExtractors int
	PilosaHost    string
	Filter        string
	Database      string
	BindAddr      string
	BufSize       int

	netEndpointIDs   *StringIDs
	transEndpointIDs *StringIDs
	netProtoIDs      *StringIDs
	transProtoIDs    *StringIDs
	appProtoIDs      *StringIDs
	methodIDs        *StringIDs
	userAgentIDs     *StringIDs
	hostnameIDs      *StringIDs

	client pdk.PilosaImporter

	nexter Nexter

	totalLen int64
	lenLock  sync.Mutex

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

func (m *Main) Run() error {
	setupClient, err := pilosa.NewClient(m.PilosaHost)
	if err != nil {
		return err
	}
	err = setupClient.CreateDB(context.Background(), m.Database, pilosa.DBOptions{})
	if err != nil {
		return fmt.Errorf("creating database '%v': %v", m.Database, err)
	}
	for _, frame := range Frames {
		err = setupClient.CreateFrame(context.Background(), m.Database, frame, pilosa.FrameOptions{})
		if err != nil {
			return fmt.Errorf("creating frame '%v': %v", frame, err)
		}
	}
	m.client = pdk.NewImportClient(m.PilosaHost, m.Database, Frames, m.BufSize)
	defer m.client.Close()

	go func() {
		log.Fatal(pdk.StartMappingProxy(m.BindAddr, "http://"+m.PilosaHost, m))
	}()

	// print total captured traffic when killed via Ctrl-c
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			m.lenLock.Lock()
			log.Printf("Total captured traffic: %v, num packets: %v", pdk.Bytes(m.totalLen), m.nexter.Last())
			m.lenLock.Unlock()
			os.Exit(0)
		}
	}()

	// print total captured traffic every 10 seconds
	nt := time.NewTicker(time.Second * 10)
	go func() {
		for range nt.C {
			m.lenLock.Lock()
			log.Printf("Total captured traffic: %v, num packets: %v", pdk.Bytes(m.totalLen), m.nexter.Last())
			m.lenLock.Unlock()
		}
	}()
	defer nt.Stop()

	var h *pcap.Handle
	if m.Filename != "" {
		h, err = pcap.OpenOffline(m.Filename)
	} else {
		h, err = pcap.OpenLive(m.Iface, m.Snaplen, m.Promisc, m.Timeout)
	}
	if err != nil {
		return fmt.Errorf("open error: %v", err)
	}

	err = h.SetBPFFilter(m.Filter)
	if err != nil {
		return fmt.Errorf("error setting bpf filter: %v", err)
	}
	packetSource := gopacket.NewPacketSource(h, h.LinkType())
	packets := packetSource.Packets()

	extractorWG := sync.WaitGroup{}
	for i := 0; i < m.NumExtractors; i++ {
		extractorWG.Add(1)
		go func() {
			m.extractAndPost(packets)
			extractorWG.Done()
		}()
	}
	extractorWG.Wait()
	return nil
}

func (m *Main) extractAndPost(packets chan gopacket.Packet) {
	var profileID uint64
	for packet := range packets {
		errL := packet.ErrorLayer()
		if errL != nil {
			log.Printf("Decoding Error: %v", errL)
			fmt.Println()
			continue
		}
		profileID = m.nexter.Next()

		length := packet.Metadata().Length
		m.AddLength(length)
		m.client.SetBit(uint64(length), profileID, packetSizeFrame)
		// ts := packet.Metadata().Timestamp

		netLayer := packet.NetworkLayer()
		if netLayer == nil {
			continue
		}
		netProto := netLayer.LayerType()
		m.client.SetBit(m.netProtoIDs.GetID(netProto.String()), profileID, netProtoFrame)
		netFlow := netLayer.NetworkFlow()
		netSrc, netDst := netFlow.Endpoints()
		m.client.SetBit(m.netEndpointIDs.GetID(netSrc.String()), profileID, netSrcFrame)
		m.client.SetBit(m.netEndpointIDs.GetID(netDst.String()), profileID, netDstFrame)

		transLayer := packet.TransportLayer()
		if transLayer == nil {
			continue
		}
		transProto := transLayer.LayerType()
		m.client.SetBit(m.transProtoIDs.GetID(transProto.String()), profileID, transProtoFrame)
		transFlow := transLayer.TransportFlow()
		transSrc, transDst := transFlow.Endpoints()
		m.client.SetBit(m.transEndpointIDs.GetID(transSrc.String()), profileID, transSrcFrame)
		m.client.SetBit(m.transEndpointIDs.GetID(transDst.String()), profileID, transDstFrame)
		if tcpLayer, ok := transLayer.(*layers.TCP); ok {
			if tcpLayer.FIN {
				m.client.SetBit(uint64(FIN), profileID, TCPFlagsFrame)
			}
			if tcpLayer.SYN {
				m.client.SetBit(uint64(SYN), profileID, TCPFlagsFrame)
			}
			if tcpLayer.RST {
				m.client.SetBit(uint64(RST), profileID, TCPFlagsFrame)
			}
			if tcpLayer.PSH {
				m.client.SetBit(uint64(PSH), profileID, TCPFlagsFrame)
			}
			if tcpLayer.ACK {
				m.client.SetBit(uint64(ACK), profileID, TCPFlagsFrame)
			}
			if tcpLayer.URG {
				m.client.SetBit(uint64(URG), profileID, TCPFlagsFrame)
			}
			if tcpLayer.ECE {
				m.client.SetBit(uint64(ECE), profileID, TCPFlagsFrame)
			}
			if tcpLayer.CWR {
				m.client.SetBit(uint64(CWR), profileID, TCPFlagsFrame)
			}
			if tcpLayer.NS {
				m.client.SetBit(uint64(NS), profileID, TCPFlagsFrame)
			}
		}
		appLayer := packet.ApplicationLayer()
		if appLayer != nil {
			appProto := appLayer.LayerType()
			m.client.SetBit(m.appProtoIDs.GetID(appProto.String()), profileID, appProtoFrame)
			appBytes := appLayer.Payload()
			buf := bytes.NewBuffer(appBytes)
			req, err := http.ReadRequest(bufio.NewReader(buf))
			if err == nil {
				userAgent := req.UserAgent()
				m.client.SetBit(m.userAgentIDs.GetID(userAgent), profileID, userAgentFrame)
				method := req.Method
				m.client.SetBit(m.methodIDs.GetID(method), profileID, methodFrame)
				hostname := req.Host
				m.client.SetBit(m.hostnameIDs.GetID(hostname), profileID, hostnameFrame)
			} else {
				// try HTTP response?
				// resp, err := http.ReadResponse(bufio.NewReader(buf))
				// 	if err == nil {
				// 	}
			}
		}
	}
}

func (m *Main) Get(frame string, id uint64) interface{} {
	switch frame {
	case netSrcFrame, netDstFrame:
		return m.netEndpointIDs.Get(id)
	case transSrcFrame, transDstFrame:
		return m.transEndpointIDs.Get(id)
	case netProtoFrame:
		return m.netProtoIDs.Get(id)
	case transProtoFrame:
		return m.transProtoIDs.Get(id)
	case appProtoFrame:
		return m.appProtoIDs.Get(id)
	case hostnameFrame:
		return m.hostnameIDs.Get(id)
	case methodFrame:
		return m.methodIDs.Get(id)
	case userAgentFrame:
		return m.userAgentIDs.Get(id)
	case packetSizeFrame:
		return id
	case TCPFlagsFrame:
		return TCPFlag(id).String()
	default:
		log.Fatalf("Unknown frame name: %v, can't translate id: %v", frame, id)
		return nil
	}
}

func (m *Main) GetID(frame string, ival interface{}) (uint64, error) {
	val, isString := ival.(string)
	checkStr := func(mapper func(string) uint64) (uint64, error) {
		if isString {
			return mapper(val), nil
		}
		return 0, fmt.Errorf("%v is not a string, but should be for frame %s", ival, frame)
	}
	switch frame {
	case netSrcFrame, netDstFrame:
		return checkStr(m.netEndpointIDs.GetID)
	case transSrcFrame, transDstFrame:
		return checkStr(m.transEndpointIDs.GetID)
	case netProtoFrame:
		return checkStr(m.netProtoIDs.GetID)
	case transProtoFrame:
		return checkStr(m.transProtoIDs.GetID)
	case appProtoFrame:
		return checkStr(m.appProtoIDs.GetID)
	case hostnameFrame:
		return checkStr(m.hostnameIDs.GetID)
	case methodFrame:
		return checkStr(m.methodIDs.GetID)
	case userAgentFrame:
		return checkStr(m.userAgentIDs.GetID)
	case packetSizeFrame:
		fval, ok := ival.(float64)
		if !ok {
			return 0, fmt.Errorf("%v should be numeric for frame %s", ival, frame)
		}
		return uint64(fval), nil
	case TCPFlagsFrame:
		if ret, ok := TCPFlagMap[val]; isString && ok {
			return ret, nil
		}
		return 0, fmt.Errorf("%v not a valid value for %s", ival, TCPFlagsFrame)
	default:
		return 0, fmt.Errorf("%s is not a known frame. try one of %v", frame, strings.Join(Frames, ", "))
	}

}

func (m *Main) AddLength(num int) {
	m.lenLock.Lock()
	m.totalLen += int64(num)
	m.lenLock.Unlock()
}

func NewMain(stdin io.Reader, stdout, stderr io.Writer) *Main {
	return &Main{
		netEndpointIDs:   NewStringIDs(),
		transEndpointIDs: NewStringIDs(),
		netProtoIDs:      NewStringIDs(),
		transProtoIDs:    NewStringIDs(),
		appProtoIDs:      NewStringIDs(),
		methodIDs:        NewStringIDs(),
		userAgentIDs:     NewStringIDs(),
		hostnameIDs:      NewStringIDs(),

		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}
