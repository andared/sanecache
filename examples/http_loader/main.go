package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/andared/sanecache"
)

type article struct {
	Title string `json:"title"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// A real HTTP server on a free loopback port keeps the example self-contained.
	var upstreamCalls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		if r.URL.Path != "/articles/42" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"title":"Caching HTTP responses"}`))
	}))
	defer upstream.Close()
	client := upstream.Client()
	client.Timeout = 5 * time.Second

	c := sanecache.New(sanecache.Options[string, article]{
		TTL:         time.Minute,
		NegativeTTL: 10 * time.Second,
		MaxEntries:  100,
		Loader: func(ctx context.Context, id string) (article, error) {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet,
				upstream.URL+"/articles/"+url.PathEscape(id), nil)
			if err != nil {
				return article{}, err
			}
			response, err := client.Do(request)
			if err != nil {
				return article{}, err
			}
			defer func() { _ = response.Body.Close() }()
			switch response.StatusCode {
			case http.StatusNotFound:
				// Only absence is cached; transport failures and other statuses stay errors.
				return article{}, sanecache.ErrNotFound
			case http.StatusOK:
				var value article
				err := json.NewDecoder(response.Body).Decode(&value)
				return value, err
			default:
				return article{}, fmt.Errorf("upstream returned %s", response.Status)
			}
		},
	})
	defer c.Close()

	for range 2 {
		value, err := c.GetOrLoad(context.Background(), "42")
		if err != nil {
			return err
		}
		fmt.Println(value.Title)
	}
	for range 2 {
		_, err := c.GetOrLoad(context.Background(), "missing")
		if !errors.Is(err, sanecache.ErrNotFound) {
			return fmt.Errorf("expected ErrNotFound, got %v", err)
		}
		fmt.Println("article not found")
	}
	fmt.Println("upstream requests:", upstreamCalls.Load())
	return nil
}
