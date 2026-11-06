// Command idemq-worker-demo is a runnable example of the client SDK: it
// pretends to be a "send welcome email" worker whose handler fails ~30% of
// the time (simulating a flaky downstream email provider), to demonstrate
// retry-with-backoff and dead-lettering end to end against a real server.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os/signal"
	"syscall"

	"github.com/Kuwar-Vishnudatt/idemq/pkg/client"
)

type emailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func main() {
	addr := flag.String("addr", "http://localhost:8080", "idemq-server base URL")
	workerID := flag.String("worker-id", "demo-worker-1", "identifies this worker in logs")
	failRate := flag.Float64("fail-rate", 0.3, "probability a job handler simulates failure")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c := client.New(*addr)

	handler := func(ctx context.Context, job *client.Job) error {
		var payload emailJob
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return fmt.Errorf("bad payload: %w", err)
		}
		if rand.Float64() < *failRate {
			return fmt.Errorf("simulated SMTP timeout sending to %s", payload.To)
		}
		log.Printf("[%s] sent %q to %s (attempt %d)", *workerID, payload.Subject, payload.To, job.Attempts)
		return nil
	}

	log.Printf("[%s] polling %s for queue=emails", *workerID, *addr)
	client.RunWorker(ctx, c, client.WorkerConfig{Queue: "emails"}, handler)
}
