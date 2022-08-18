package main

import (
	"context"
	"fmt"
	"github.com/aktivgo/ksqldb-go"
	"github.com/semichkin-gopkg/uuid"
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	GoroutinesCount = 1000000
)

const (
	KsqlURL = "http://localhost:8088"
)

var (
	resources = []uuid.UUID{
		"1a34b742-1ec4-11ed-861d-0242ac120002",
		"2a4aad70-1ec4-11ed-861d-0242ac120002",
	}
	leads = []uuid.UUID{
		"1f486320-1ec4-11ed-861d-0242ac120002",
		"24d36d76-1ec4-11ed-861d-0242ac120002",
	}
	types = []string{
		"'tg_send_text'",
		"'tg_send_text', 'tg_start'",
		"'tg_start'",
	}
)

func main() {
	client := ksqldb.NewClient(KsqlURL, "", "")

	triggered := 0

	wg := sync.WaitGroup{}
	for i := 0; i < GoroutinesCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			pullQuery, pushQuery := generateQueries()

			if err := pull(client, pullQuery); err == nil {
				triggered++
				log.Println("triggered")
				return
			}

			log.Println("pull return empty response")

			if err := push(client, pushQuery); err != nil {
				log.Println(err)
				return
			}

			triggered++
			log.Println("triggered")
		}()

		time.Sleep(time.Millisecond * 50)
	}

	wg.Wait()

	log.Println("triggered count:", triggered)
}

func generateQueries() (string, string) {
	resourceId := resources[rand.Intn(len(resources))]
	leadId := leads[rand.Intn(len(leads))]
	_type := types[rand.Intn(len(types))]

	return fmt.Sprintf(
			"SELECT * FROM events WHERE resource_id = '%s' AND lead_id = '%s' AND direction = 'in' AND type IN (%s) AND timestamp >= %d;",
			resourceId,
			leadId,
			_type,
			time.Now().Unix()-60,
		), fmt.Sprintf(
			"SELECT * FROM events WHERE resource_id = '%s' AND lead_id = '%s' AND direction = 'in' AND type IN (%s) AND timestamp >= %d EMIT CHANGES;",
			resourceId,
			leadId,
			_type,
			time.Now().Unix()-60,
		)
}

func pull(client *ksqldb.Client, query string) error {
	ctx, ctxCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ctxCancel()

	_, _, err := client.Pull(ctx, query, false)
	if err != nil {
		return err
	}

	return nil
}

func push(client *ksqldb.Client, query string) error {
	ec := make(chan error)
	rc := make(chan ksqldb.Row)
	hc := make(chan ksqldb.Header, 1)

	go func() {
		for range rc {
			ec <- nil
		}
	}()

	ctx, ctxCancel := context.WithTimeout(context.Background(), 10*time.Hour)
	defer ctxCancel()

	go func() {
		if err := client.Push(ctx, query, rc, hc); err != nil {
			ec <- fmt.Errorf("Error running Push request against ksqlDB:\n%v", err)
		}
	}()

	log.Println("start push query")

	return <-ec
}
