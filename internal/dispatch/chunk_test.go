package dispatch

import (
	"slices"
	"testing"
)

func TestChunkCoversEveryMessageExactlyOnce(t *testing.T) {
	for _, n := range []int{1, 2, 7, 100, 199, 200} {
		for _, workers := range []int{1, 3, 8, 64} {
			spans := chunk(n, workers)

			covered := make([]bool, n)
			for _, s := range spans {
				if s.start < 0 || s.end > n || s.start >= s.end {
					t.Fatalf("chunk(%d, %d) produced an invalid span %+v", n, workers, s)
				}
				for i := s.start; i < s.end; i++ {
					if covered[i] {
						t.Fatalf("chunk(%d, %d) covers index %d twice", n, workers, i)
					}
					covered[i] = true
				}
			}

			if slices.Contains(covered, false) {
				t.Errorf("chunk(%d, %d) leaves messages unpublished: %v", n, workers, spans)
			}
			if len(spans) > workers {
				t.Errorf("chunk(%d, %d) produced %d spans, more than the worker count", n, workers, len(spans))
			}
		}
	}
}

func TestChunkBalancesSpans(t *testing.T) {
	spans := chunk(10, 4)

	sizes := make([]int, len(spans))
	for i, s := range spans {
		sizes[i] = s.end - s.start
	}

	lo, hi := slices.Min(sizes), slices.Max(sizes)
	if hi-lo > 1 {
		t.Errorf("chunk sizes %v differ by more than one; one worker would carry the batch", sizes)
	}
}

func TestChunkHandlesDegenerateInput(t *testing.T) {
	if got := chunk(0, 4); got != nil {
		t.Errorf("chunk(0, 4) = %v, want nil", got)
	}
	if got := chunk(3, 0); len(got) != 1 || got[0] != (span{0, 3}) {
		t.Errorf("chunk(3, 0) = %v, want a single span covering everything", got)
	}
}
