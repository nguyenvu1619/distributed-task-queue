package main

import (
	"fmt"
	"sync"
)

type SafeCounter struct {
	v  map[string]bool
	mu sync.Mutex
}

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

func (safeCounter *SafeCounter) CheckExist(url string) bool {
	defer safeCounter.mu.Unlock()
	safeCounter.mu.Lock()
	if !safeCounter.v[url] {
		safeCounter.v[url] = true
		return false
	}
	return true
}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl(url string, depth int, fetcher Fetcher, done chan struct{}, safeCounter *SafeCounter) {
	defer func() { done <- struct{}{} }()
	if depth <= 0 || safeCounter.CheckExist(url) {
		return
	}
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("found: %s %q\n", url, body)
	childDone := make(chan struct{})
	numWorkers := 0
	for _, u := range urls {
		numWorkers++
		go Crawl(u, depth-1, fetcher, childDone, safeCounter)
	}
	for i := 0; i < numWorkers; i++ {
		<-childDone
	}
}

func main() {
	done := make(chan struct{})
	safeCounter := SafeCounter{v: make(map[string]bool)}
	go Crawl("https://golang.org/", 4, fetcher, done, &safeCounter)
	<-done
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
