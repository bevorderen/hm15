package main

import (
	"./appsinstalled"
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	normalErrorRate = 0.01
)

type deviceApps struct {
	devType string
	devId   string
	lat     float64
	lon     float64
	apps    []uint32
}

type Config struct {
	clients  map[string]*memcache.Client
	workers  int
	retry    int
	duration int
	dryRun   bool
}

func initConfig(
	addresses map[string]string,
	timeout int,
	workers int,
	retry int,
	duration int,
	dryRun bool) *Config {
	clients := make(map[string]*memcache.Client)
	for name, address := range addresses {
		clients[name] = memcache.New(address)
		clients[name].Timeout = time.Duration(timeout) * time.Millisecond
	}
	return &Config{
		clients:  clients,
		workers:  workers,
		retry:    retry,
		duration: duration,
		dryRun:   dryRun,
	}
}

func parseAppsInstalled(line string) (deviceApps, error) {
	lineParts := strings.Split(strings.TrimSpace(line), "\t")
	if len(lineParts) != 5 {
		return deviceApps{}, fmt.Errorf("line have 5 line")
	}
	devType, devId := lineParts[0], lineParts[1]

	if len(devType) == 0 {
		return deviceApps{}, errors.New("unknown device type")
	}

	if len(devId) == 0 {
		return deviceApps{}, errors.New("device id not found")
	}

	lat, err := strconv.ParseFloat(lineParts[2], 64)
	if err != nil {
		return deviceApps{}, err
	}

	lon, err := strconv.ParseFloat(lineParts[3], 64)
	if err != nil {
		return deviceApps{}, err
	}

	var apps []uint32
	for _, rawApp := range strings.Split(lineParts[4], ",") {
		appId, err := strconv.Atoi(rawApp)
		if err != nil {
			continue
		}
		apps = append(apps, uint32(appId))
	}
	return deviceApps{
		devType: devType,
		devId:   devId,
		lat:     lat,
		lon:     lon,
		apps:    apps,
	}, nil
}

func readFileToChan(filePath string, output chan string) {
	log.Printf("Read file %s", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Can't open file %s. %s", filePath, err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Can't close file %s. %s", filePath, err)
		}
	}()

	gz, err := gzip.NewReader(file)
	if err != nil {
		log.Fatalf("Can't open gzip file %s. %s", filePath, err)
	}

	count := 0
	reader := bufio.NewReader(gz)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				fmt.Println(err)
				return
			}
		}
		if len(line) > 0 {
			output <- line
			count++
		}
	}

	log.Printf("%d lines read from %s", count, filePath)
}

func dotRenameFile(filePath string, dryRun bool) {
	path, name := filepath.Split(filePath)
	if dryRun == false {
		err := os.Rename(filePath, filepath.Join(path, "."+name))
		if err != nil {
			log.Printf("Cant rename file %s, err = %s", filePath, err)
		}
	}
}
func fillChannelAppDevice(channel chan string, channelAppDevice chan deviceApps) {
	var failedLine, successLine float32
	for line := range channel {
		apps, err := parseAppsInstalled(line)
		if err == nil {
			channelAppDevice <- apps
			successLine += 1
		} else {
			log.Printf("Error ecur when parse app. %s", err)
			failedLine += 1
		}
	}

	total := failedLine + successLine
	errRate := failedLine / total
	if errRate < normalErrorRate {
		log.Printf("Acceptable error rate %f. Successful load", errRate)
	} else {
		log.Printf("High error rate (%f > %f). Failed load", errRate, normalErrorRate)
	}
}

func memcSetWithRetry(config *Config, message *memcache.Item, memcClient *memcache.Client) error {
	var err error

	for attempt := 0; attempt < config.retry; attempt++ {
		err = memcClient.Set(&memcache.Item{Key: message.Key, Value: message.Value})
		if err != nil {
			time.Sleep(time.Duration(config.duration) * time.Second)
			continue
		}
		break
	}
	return err
}

func setToMemc(wg *sync.WaitGroup, channelAppDevice chan deviceApps, config Config) {
	defer wg.Done()
	for app := range channelAppDevice {
		memcClient, ok := config.clients[app.devType]

		if ok != true {
			log.Printf("Unknown device type")
			continue
		}
		ua := &appsinstalled.UserApps{
			Lat:  proto.Float64(app.lat),
			Lon:  proto.Float64(app.lon),
			Apps: app.apps,
		}
		key := fmt.Sprintf("%s:%s", app.devType, app.devId)
		packed, err := proto.Marshal(ua)
		if err != nil {
			log.Printf("Error ecure when marshal %s", err)
		}
		item := memcache.Item{
			Key:   key,
			Value: packed,
		}

		if config.dryRun {
			log.Printf("%v -> %v\n", item.Key, app)
			continue
		}
		if err := memcSetWithRetry(&config, &item, memcClient); err != nil {
			log.Printf("error connect to Memcached: %s\n", app.devType)
			log.Printf("%s", err)
		}
	}
}

func processFile(channel chan string, config Config) {
	channelAppDevice := make(chan deviceApps)
	var wg sync.WaitGroup
	go fillChannelAppDevice(channel, channelAppDevice)
	for i := 0; i < config.workers; i++ {
		wg.Add(1)
		go setToMemc(&wg, channelAppDevice, config)
	}
	log.Printf("File finished")
	wg.Wait()
}

func main() {
	start := time.Now()
	var (
		idfa, gaid, adid, dvid, pattern, logFileArg string
		workers, socketTimeout, duration, retry     int
		dryRun, test                                bool
	)
	flag.BoolVar(&test, "test", false, "run protobuf test")
	flag.BoolVar(&dryRun, "dryRun", false, "dry run")
	flag.StringVar(&logFileArg, "log", "", "log file")
	flag.StringVar(&pattern, "pattern", "./[^.]*.tsv.gz", "files path pattern")

	flag.StringVar(&idfa, "idfa", "127.0.0.1:33013", "host and port for idfa")
	flag.StringVar(&gaid, "gaid", "127.0.0.1:33014", "host and port for idfa")
	flag.StringVar(&adid, "adid", "127.0.0.1:33015", "host and port for idfa")
	flag.StringVar(&dvid, "dvid", "127.0.0.1:33016", " host and port for idfa")

	flag.IntVar(&workers, "workers", 4, "workers count")
	flag.IntVar(&socketTimeout, "timeout", 500, "socket timeout")
	flag.IntVar(&duration, "duration", 3, "duration between retry")
	flag.IntVar(&retry, "retry", 5, "count of retry")

	flag.Parse()

	addresses := map[string]string{
		"idfa": idfa,
		"gaid": gaid,
		"adid": adid,
		"dvid": dvid,
	}

	if test {
		prototest()
		return
	}

	// setup log file
	logFile, err := os.OpenFile(logFileArg, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Can't open log file %s. %s Log will be written to the console", logFileArg, err)
	}
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	defer func(logFile *os.File) {
		if logFile != nil {
			err := logFile.Close()
			if err != nil {
				log.Printf("Can't close file %s. %s", logFileArg, err)
			}
		}
	}(logFile)

	// read file by pattern
	filePaths, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("Error ocur when reading file. %s", err)
	}

	if len(filePaths) == 0 {
		log.Fatalf("Files by pattern %s not found", pattern)
	}

	lineChan := make(chan string)

	var wp sync.WaitGroup

	config := initConfig(addresses, socketTimeout, workers, retry, duration, dryRun)

	for _, filePath := range filePaths {
		wp.Add(1)
		go func(f string, c chan string) {
			readFileToChan(f, c)
			dotRenameFile(f, dryRun)
			wp.Done()
		}(filePath, lineChan)
	}
	go processFile(lineChan, *config)

	wp.Wait()
	close(lineChan)

	execTime := time.Since(start)
	log.Printf("Execution time = %s", execTime)
}

func prototest() {
	sample := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"

	for _, line := range strings.Split(sample, "\n") {
		appsInstalled, _ := parseAppsInstalled(line)
		ua := &appsinstalled.UserApps{
			Lat:  proto.Float64(appsInstalled.lat),
			Lon:  proto.Float64(appsInstalled.lon),
			Apps: appsInstalled.apps,
		}

		packed, err := proto.Marshal(ua)
		if err != nil {
			log.Fatalf("failed proto marshal ua, err %s", err)
		}

		unpacked := &appsinstalled.UserApps{}
		err = proto.Unmarshal(packed, unpacked)
		if err != nil {
			log.Fatalf("failed proto Unmarshal packed ua, err %s", err)
		}

		badLat := unpacked.GetLat() != ua.GetLat()
		badLon := unpacked.GetLon() != ua.GetLon()
		badApps := !reflect.DeepEqual(ua.GetApps(), unpacked.GetApps())
		if badLat || badLon || badApps {
			log.Fatalf("ua and unpacked value are different")
		}
	}
	log.Print("success")
	os.Exit(0)
}
