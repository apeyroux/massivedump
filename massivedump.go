package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

type Server struct {
	Host     string
	Port     int
	Db       []string
	Login    string
	Password string
}

type Dump struct {
	Data    []byte
	Success bool
}

type Configuration struct {
	Dumpdata string
	Every    int
	Server   []Server
}

var flconfigfilepath string
var fldump bool
var flpurge bool

func init() {
	flag.StringVar(&flconfigfilepath, "c", "", "Fichier de configuration")
	flag.BoolVar(&fldump, "d", false, "dump les bases")
	flag.BoolVar(&flpurge, "p", false, "purge les bases")
}

func loadconfig(configfilepath string) (*Configuration, error) {
	config := new(Configuration)
	cf, err := ioutil.ReadFile(configfilepath)

	if err != nil {
		return nil, err
	}

	errjs := json.Unmarshal(cf, &config)
	if errjs != nil {
		return nil, errjs
	}

	return config, nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func runDump(config Configuration, server Server, wg *sync.WaitGroup) {
	for _, db := range server.Db {
		began := time.Now()
		cdump := make(chan *Dump)
		dumpcmd := fmt.Sprintf("mysqldump -h %s -P %d -u %s -p%s %s", server.Host, server.Port, server.Login, server.Password, db)
		tdumpcmd := strings.Fields(dumpcmd)

		go func() {
			dump := new(Dump)
			dumpc := exec.Command(tdumpcmd[0], tdumpcmd[1:len(tdumpcmd)]...)
			data, err := dumpc.Output()
			if err != nil {
				log.Printf("dump %s on %s (err:%s cmd:%s) [fail]", db, server.Host, err, dumpcmd)
				dump.Data = nil
				dump.Success = false
			} else {
				dump.Data = data
				dump.Success = true
			}
			cdump <- dump
		}()

		dump := <-cdump

		if !dump.Success {
			continue
		}

		dumpdata := fmt.Sprintf("%s/%s-%d/%s", config.Dumpdata, server.Host, server.Port, db)
		dumpdataexist, _ := exists(dumpdata)
		if !dumpdataexist {
			err := os.MkdirAll(dumpdata, 0755)
			if err != nil {
				log.Printf("dump %s on %s (err:%s cmd:%s) [fail]", db, server.Host, err, dumpcmd)
				continue
			}
		}

		dumpf := fmt.Sprintf("%s/%s-%s.sql", dumpdata, db, time.Now().Format("02012006-1504"))
		errio := ioutil.WriteFile(dumpf, dump.Data, 0644)
		if errio != nil {
			log.Printf("dump %s on %s (err:%s cmd:%s) [fail]", db, server.Host, errio, dumpcmd)
			continue
		}

		log.Printf("dump %s on %s in %s", db, server.Host, time.Since(began))

	}
	wg.Done()
}

func runPurge(config Configuration, server Server, wg *sync.WaitGroup) {
	for _, db := range server.Db {
		dumpdata := fmt.Sprintf("%s/%s-%d/%s/", config.Dumpdata, server.Host, server.Port, db)
		files, _ := ioutil.ReadDir(dumpdata)
		for _, f := range files {
			pf := dumpdata + f.Name()
			sf, _ := os.Stat(pf)
			fttl := time.Since(sf.ModTime()).Hours()
			if fttl > float64(config.Every) {
				log.Printf("sup. de %s vieux de %.2f heures", pf, fttl)
			}
		}
	}
	wg.Done()
}

func main() {

	flag.Parse()

	if flconfigfilepath == "" {
		flag.Usage()
		os.Exit(0)
	}

	config, err := loadconfig(flconfigfilepath)

	if err != nil {
		log.Fatalf("err load config %s", err)
		os.Exit(2)
	}

	if !fldump && !flpurge {
		flag.Usage()
		os.Exit(0)
	}

	if flpurge {
		wg := new(sync.WaitGroup)
		for _, server := range config.Server {
			wg.Add(1)
			go runPurge(*config, server, wg)
		}
		wg.Wait()
	}

	if fldump {
		wg := new(sync.WaitGroup)
		for _, server := range config.Server {
			wg.Add(1)
			go runDump(*config, server, wg)
		}
		wg.Wait()
	}

}
