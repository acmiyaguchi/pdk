package retention

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	_ "net/http/pprof"

	"github.com/acmiyaguchi/pdk"
	pcli "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pilosa"
)

var Frames = []string{
	"channel",
	"country",
	"os",
	"days_since_profile_creation",
	/*
		"subsession_length",
		"distribution_id",
		"submission_date",
		"sync_configured",
		"app_version",
		"locale",
		"active_addons_count",
		"is_default_browser",
		"default_search_engine",
		"unique_domains_count",
		"total_uri_count"
	*/
}

type Main struct {
	PilosaHost  string
	URLFile     string
	Concurrency int
	Index       string
	BufferSize  int

	importer pdk.PilosaImporter
	urls     []string

	clientIDs    *StringIDs
	frameIDs     map[string]int
	frameMapper  map[string]*StringIDs
	recordMapper []pdk.BitMapper

	totalBytes int64
	bytesLock  sync.Mutex

	totalRecs   *Counter
	skippedRecs *Counter
}

func NewMain() *Main {
	m := &Main{
		Concurrency: 1,
		urls:        make([]string, 0),

		totalRecs:   &Counter{},
		skippedRecs: &Counter{},

		clientIDs:    NewStringIDs(),
		frameIDs:     make(map[string]int),
		frameMapper:  make(map[string]*StringIDs),
		recordMapper: make([]pdk.BitMapper, 0),
	}

	// The first two fields are reserved for (client_id, timestamp)
	var field_offset = 2

	// Initialize the frame->id and frame->(label->id) mappings
	for i, frame := range Frames {
		m.frameIDs[frame] = i + field_offset
		m.frameMapper[frame] = NewStringIDs()
	}

	m.recordMapper = getBitMappers(m)

	return m
}

func (m *Main) Run() error {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	err := m.readURLs()
	if err != nil {
		return err
	}

	m.importer = pdk.NewImportClient(m.PilosaHost, m.Index, Frames, m.BufferSize)

	pilosaURI, err := pcli.NewURIFromAddress(m.PilosaHost)
	if err != nil {
		return fmt.Errorf("interpreting pilosaHost '%v': %v", m.PilosaHost, err)
	}
	setupClient := pcli.NewClientWithURI(pilosaURI)
	index, err := pcli.NewIndex(m.Index, &pcli.IndexOptions{})
	if err != nil {
		return fmt.Errorf("making index: %v", err)
	}
	err = setupClient.EnsureIndex(index)
	if err != nil {
		return fmt.Errorf("ensuring index existence: %v", err)
	}
	for _, frame := range Frames {
		fram, err := index.Frame(frame, &pcli.FrameOptions{CacheType: pilosa.CacheTypeRanked})
		if err != nil {
			return fmt.Errorf("making frame: %v", err)
		}
		err = setupClient.EnsureFrame(fram)
		if err != nil {
			return fmt.Errorf("creating frame '%v': %v", frame, err)
		}
	}

	ticker := m.printStats()

	urls := make(chan string)
	records := make(chan Record)

	go func() {
		for _, url := range m.urls {
			urls <- url
		}
		close(urls)
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Printf("Profiles: %d, Bytes: %s",
				m.clientIDs.Last(), pdk.Bytes(m.BytesProcessed()))
			os.Exit(0)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < m.Concurrency; i++ {
		wg.Add(1)
		go func() {
			m.fetch(urls, records)
			wg.Done()
		}()
	}
	var wg2 sync.WaitGroup
	for i := 0; i < m.Concurrency; i++ {
		wg2.Add(1)
		go func() {
			m.parseMapAndPost(records)
			wg2.Done()
		}()
	}
	wg.Wait()
	close(records)
	wg2.Wait()
	m.importer.Close()
	ticker.Stop()
	return err
}

func (m *Main) readURLs() error {
	if m.URLFile == "" {
		return fmt.Errorf("Need to specify a URL File")
	}
	f, err := os.Open(m.URLFile)
	if err != nil {
		return err
	}
	s := bufio.NewScanner(f)
	for s.Scan() {
		m.urls = append(m.urls, s.Text())
	}
	if err := s.Err(); err != nil {
		return err
	}
	return nil
}

func (m *Main) printStats() *time.Ticker {
	t := time.NewTicker(time.Second * 10)
	start := time.Now()
	go func() {
		for range t.C {
			duration := time.Since(start)
			bytes := m.BytesProcessed()
			log.Printf("Profiles: %d, Bytes: %s, Records: %v, Duration: %v, Rate: %v/s",
				m.clientIDs.Last(), pdk.Bytes(bytes), m.totalRecs.Get(),
				duration, pdk.Bytes(float64(bytes)/duration.Seconds()))
			log.Printf("Skipped: %v", m.skippedRecs.Get())
		}
	}()
	return t
}

func (m *Main) fetch(urls <-chan string, records chan<- Record) {
	for url := range urls {
		var content io.ReadCloser
		if strings.HasPrefix(url, "http") {
			resp, err := http.Get(url)
			if err != nil {
				log.Printf("fetching %s, err: %v", url, err)
				continue
			}
			content = resp.Body
		} else {
			f, err := os.Open(url)
			if err != nil {
				log.Printf("opening %s, err: %v", url, err)
				continue
			}
			content = f
		}

		scan := bufio.NewScanner(content)
		// discard header line
		correctLine := false
		if scan.Scan() {
			header := scan.Text()
			if strings.HasPrefix(header, "client_id") {
				correctLine = true
			}
		}
		for scan.Scan() {
			m.totalRecs.Add(1)
			record := scan.Text()
			m.AddBytes(len(record))
			if correctLine {
				// last field needs to be shifted over by 1
				lastcomma := strings.LastIndex(record, ",")
				if lastcomma == -1 {
					m.skippedRecs.Add(1)
					continue
				}
				record = record[:lastcomma] + "," + record[lastcomma:]
			}
			records <- Record{Val: record}
		}
		err := content.Close()
		if err != nil {
			log.Printf("closing %s, err: %v", url, err)
		}
	}
}

type Record struct {
	Val string
}

func (r Record) Clean() ([]string, bool) {
	if len(r.Val) == 0 {
		return nil, false
	}
	fields := strings.Split(r.Val, ",")
	return fields, true
}

type BitFrame struct {
	Bit   uint64
	Frame string
}

func (m *Main) parseMapAndPost(records <-chan Record) {
Records:
	for record := range records {
		fields, ok := record.Clean()
		if !ok {
			m.skippedRecs.Add(1)
			continue
		}

		client_id := m.clientIDs.GetID(fields[0])

		layout := "2006-01-02"
		timestamp, err := time.Parse(layout, fields[1])
		if err != nil {
			// TODO: Set a default date
		}

		bitsToSet := make([]BitFrame, 0)
		var bms = m.recordMapper
		for _, bm := range bms {
			if len(bm.Fields) != len(bm.Parsers) {
				// TODO if len(pm.Parsers) == 1, use that for all fields
				log.Fatalf("parse: BitMapper has different number of fields: %v and parsers: %v", bm.Fields, bm.Parsers)
			}

			// parse fields into a slice `parsed`
			parsed := make([]interface{}, 0, len(bm.Fields))
			for n, fieldnum := range bm.Fields {
				parser := bm.Parsers[n]
				if fieldnum >= len(fields) {
					log.Printf("parse: field index: %v out of range for: %v", fieldnum, fields)
					m.skippedRecs.Add(1)
					continue Records
				}
				parsedField, err := parser.Parse(fields[fieldnum])
				if err != nil && fields[fieldnum] == "" {
					m.skippedRecs.Add(1)
					continue Records
				} else if err != nil {
					log.Printf("parsing: field: %v err: %v bm: %v rec: %v", fields[fieldnum], err, bm, record)
					m.skippedRecs.Add(1)
					continue Records
				}
				parsed = append(parsed, parsedField)
			}

			// Handle errors during the parsing process
			ids, err := bm.Mapper.ID(parsed...)
			if err != nil {
				log.Printf("mapping: bm: %v, err: %v rec: %v", bm, err, record)
				m.skippedRecs.Add(1)
				continue Records
			}

			// map those fields to a slice of IDs
			for _, id := range ids {
				bitsToSet = append(bitsToSet, BitFrame{Bit: uint64(id), Frame: bm.Frame})
			}
		}

		for _, bit := range bitsToSet {
			m.importer.SetBitTimestamp(bit.Bit, client_id, bit.Frame, timestamp)
		}
	}
}

func getBitMappers(m *Main) []pdk.BitMapper {
	// The custom function maps categorical labels for a field to an
	// uint64 id that is stored in memory. This id is then mapped to
	// the bitmap by the predifined IntMapper.
	// (string -> int64) -> int64
	idMapper := func(field string) pdk.CustomMapper {
		return pdk.CustomMapper{
			// Get the id for the field
			Func: func(fields ...interface{}) interface{} {
				return m.frameMapper[field].GetID(fields[0].(string))
			},
			Mapper: pdk.IntMapper{
				Min: math.MinInt64,
				Max: math.MaxInt64,
				// NOTE: BitDepth is not defined
			},
		}
	}

	// Generate a categorical bitmapper by applying the idMapper to
	// each categorical field in the incoming dataset
	categoricalBitMapper := func(field string) pdk.BitMapper {
		return pdk.BitMapper{
			Frame:   field,
			Mapper:  idMapper(field),
			Parsers: []pdk.Parser{pdk.StringParser{}},
			Fields:  []int{m.frameIDs[field]},
		}
	}

	recordMapper := []pdk.BitMapper{
		categoricalBitMapper("channel"),
		categoricalBitMapper("country"),
		categoricalBitMapper("os"),
		pdk.BitMapper{
			Frame:   "days_since_profile_creation",
			Mapper:  pdk.IntMapper{Min: -1, Max: math.MaxInt64},
			Parsers: []pdk.Parser{pdk.IntParser{}},
			Fields:  []int{m.frameIDs["days_since_profile_creation"]},
		},
	}

	return recordMapper
}

func (m *Main) AddBytes(n int) {
	m.bytesLock.Lock()
	m.totalBytes += int64(n)
	m.bytesLock.Unlock()
}

func (m *Main) BytesProcessed() (num int64) {
	m.bytesLock.Lock()
	num = m.totalBytes
	m.bytesLock.Unlock()
	return
}

type Counter struct {
	num  int64
	lock sync.Mutex
}

func (c *Counter) Add(n int) {
	c.lock.Lock()
	c.num += int64(n)
	c.lock.Unlock()
}

func (c *Counter) Get() (ret int64) {
	c.lock.Lock()
	ret = c.num
	c.lock.Unlock()
	return
}
