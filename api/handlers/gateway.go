package handlers

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// NewProxy creates a reverse proxy handler for a given target URL.
func NewProxy(target string) http.Handler {
	targetURL, err := url.Parse(target)
	if err != nil {
		log.Fatalf("Error parsing target URL: %v", err)
	}
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = targetURL.Host
	}
	return proxy
}

func Healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
