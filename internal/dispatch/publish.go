package dispatch

import (
	"context"
	"sync"
	"time"

	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/events"
)

// publish sends the batch and reports the outcome of each message, along with
// how many messages were attempted at all.
//
// The batch is split into chunks published concurrently. Chunking rather than
// one goroutine per message is what lets both drivers work the way they want to:
// Kafka writes a chunk in a single round trip, and RabbitMQ — which has no batch
// publish — gets as many concurrent publishes as there are chunks, spread across
// its channel pool.
//
// Once a shutdown begins, chunks not yet started are abandoned rather than
// published on a dying process; the caller hands their claims straight back.
func (p *Pipeline) publish(ctx context.Context, msgs []core.Message) (results []events.Publish, attempted int) {
	results = make([]events.Publish, len(msgs))

	chunks := chunk(len(msgs), p.cfg.Workers)

	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)

	for _, c := range chunks {
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		attempted = c.end

		go func(c span) {
			defer wg.Done()

			started := time.Now()

			publishCtx, cancel := context.WithTimeout(ctx, p.cfg.PublishTimeout)
			defer cancel()

			errs := p.router.Publish(publishCtx, p.stream, msgs[c.start:c.end])

			// One duration for the chunk. A per-message figure would be a
			// fiction for a driver that writes the chunk in one round trip.
			elapsed := time.Since(started)

			mu.Lock()
			defer mu.Unlock()

			for i := range msgs[c.start:c.end] {
				err := errs[i]
				results[c.start+i] = events.Publish{
					ID:        msgs[c.start+i].ID,
					Duration:  elapsed,
					Err:       err,
					Permanent: err != nil && core.IsPermanent(err),
				}
			}
		}(c)
	}

	wg.Wait()

	return results, attempted
}

// span is a half-open range of the batch.
type span struct{ start, end int }

// chunk divides n items into at most workers contiguous spans, keeping the
// sizes within one of each other.
func chunk(n, workers int) []span {
	if n <= 0 {
		return nil
	}
	if workers < 1 {
		workers = 1
	}
	if workers > n {
		workers = n
	}

	spans := make([]span, 0, workers)
	size, remainder := n/workers, n%workers

	start := 0
	for i := range workers {
		end := start + size
		if i < remainder {
			end++
		}
		spans = append(spans, span{start: start, end: end})
		start = end
	}

	return spans
}
