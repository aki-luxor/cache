package main

import (
	_ "embed"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// Default per-user quota (bytes)
	defaultQuota     = 10 * 1024 * 1024 * 1024 // 10GB
	defaultChunkSize = 8 * 1024 * 1024         // 8MB
)

// reserveRequest defines the JSON body for reserve cache API.
type reserveRequest struct {
	Key       string `json:"key"`
	Version   string `json:"version"`
	CacheSize int64  `json:"cacheSize"`
	RunId     string `json:"runId"`
}

// reserveResult is returned under "result" for a successful reserve.
type reserveResult struct {
	UploadId      string   `json:"uploadId"`
	PresignedUrls []string `json:"presignedUrls"`
	ChunkSize     int64    `json:"chunkSize"`
}

// reserveResponse wraps reserveResult.
type reserveResponse struct {
	Result reserveResult `json:"result"`
}

// errorResponse standard error JSON.
type errorResponse struct {
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
}

// commitRequest defines JSON body for commit cache API.
type commitRequest struct {
	Size     int64    `json:"size"`
	UploadId string   `json:"uploadId"`
	Etags    []string `json:"etags"`
}

// tokenInfo holds per-user quota and current usage (bytes).
type tokenInfo struct {
	Quota int64
	Used  int64
}

// cacheEntry holds committed cache metadata.
type cacheEntry struct {
	Key             string
	Version         string
	RunId           string
	CacheId         int64
	ArchivePath     string
	ArchiveLocation string
	CreationTime    time.Time
	Compression     string
	Size            int64
}

// listResponse for GET /caches?key= listing.
type listResponse struct {
	TotalCount     int             `json:"totalCount"`
	ArtifactCaches []artifactCache `json:"artifactCaches"`
}

// artifactCache element in listing.
type artifactCache struct {
	CacheKey     string `json:"cacheKey"`
	CacheVersion string `json:"cacheVersion"`
	Scope        string `json:"scope"`
	CreationTime string `json:"creationTime"`
	Size         int64  `json:"sizeOnDisk,omitempty"`
	Compression  string `json:"compression,omitempty"`
}

func main() {
	// initialize server; tokens can be created via /token endpoint
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("INFO: Using port %s", port)

	storageDir := filepath.Join(os.TempDir(), "tenki-cloud-cache")
	log.Printf("INFO: Using storage directory: %s", storageDir)
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("FATAL: failed to create storage dir '%s': %v", storageDir, err)
	} else {
		log.Printf("INFO: Ensured storage directory exists: %s", storageDir)
	}

	srv := &server{
		storageDir:  storageDir,
		nextCacheID: 1,
		reserves:    make(map[string]*reserveRecord),
		caches:      make(map[string]*cacheEntry),
		byUpload:    make(map[string]*cacheEntry),
		tokens:      make(map[string]*tokenInfo),
	}

	log.Printf("INFO: Registering HTTP handlers...")
	// token creation endpoint (no auth)
	http.HandleFunc("/token", srv.handleTokenCreation)
	http.HandleFunc("/cache", srv.handleGetCache)
	http.HandleFunc("/caches/commit", srv.handleCommit)
	http.HandleFunc("/caches", srv.handleCaches)
	http.HandleFunc("/upload/", srv.handleUpload)
	http.HandleFunc("/download/", srv.handleDownload)

	// Twirp RPC: GetCacheEntryDownloadURL
	http.HandleFunc(
		"/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL",
		func(w http.ResponseWriter, r *http.Request) {
			log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
			log.Printf("DEBUG: Handling Twirp request for GetCacheEntryDownloadURL")
			if r.Method != "POST" {
				log.Printf("ERROR: Twirp: Method %s not allowed, expected POST", r.Method)
				writeTwirpError(w, "malformed", "Method not allowed, expected POST", http.StatusMethodNotAllowed)
				return
			}
			// Auth guard same as /cache
			token, ok := srv.requireAuth(w, r) // Pass token through requireAuth now
			if !ok {
				// requireAuth handles logging and response (already JSON)
				log.Printf("DEBUG: Twirp: Authentication failed.")
				return
			}
			log.Printf("DEBUG: Twirp: Authentication successful for token prefix %s...", truncateToken(token))

			// Decode Twirp JSON body
			var req struct {
				Key         string   `json:"key"`
				RestoreKeys []string `json:"restoreKeys"`
				Version     string   `json:"version"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				log.Printf("ERROR: Twirp: Malformed request body: %v", err)
				writeTwirpError(w, "malformed", "Malformed request: "+err.Error(), http.StatusBadRequest)
				log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusBadRequest)
				return
			}
			log.Printf("DEBUG: Twirp: Decoded request: Key=%s, RestoreKeys=%v, Version=%s", req.Key, req.RestoreKeys, req.Version)

			// Lookup cache entry
			srv.mu.Lock()
			log.Printf("DEBUG: Twirp: Acquired lock for cache lookup")
			var matchedKey, downloadURL string
			allKeys := append([]string{req.Key}, req.RestoreKeys...)
			log.Printf("DEBUG: Twirp: Searching for keys: %v with version: %s", allKeys, req.Version)
			for _, k := range allKeys {
				comp := k + "|" + req.Version
				log.Printf("DEBUG: Twirp: Checking composite key: %s", comp)
				if e, found := srv.caches[comp]; found {
					log.Printf("DEBUG: Twirp: Cache HIT for composite key: %s", comp)
					matchedKey = e.Key
					downloadURL = e.ArchiveLocation
					log.Printf("DEBUG: Twirp: Found entry: MatchedKey=%s, DownloadURL=%s", matchedKey, downloadURL)
					break // Stop on first match
				} else {
					log.Printf("DEBUG: Twirp: Cache MISS for composite key: %s", comp)
				}
			}
			srv.mu.Unlock()
			log.Printf("DEBUG: Twirp: Released lock after cache lookup")

			// Build Twirp response
			resp := struct {
				Ok                bool   `json:"ok"`
				MatchedKey        string `json:"matchedKey,omitempty"`
				SignedDownloadUrl string `json:"signedDownloadUrl,omitempty"`
			}{}
			if matchedKey == "" {
				log.Printf("DEBUG: Twirp: No matching cache entry found. Responding with ok=false")
				resp.Ok = false
			} else {
				log.Printf("DEBUG: Twirp: Matching cache entry found. Responding with ok=true, MatchedKey=%s, URL=%s", matchedKey, downloadURL)
				resp.Ok = true
				resp.MatchedKey = matchedKey
				resp.SignedDownloadUrl = downloadURL
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusOK)
		},
	)

	// Twirp RPC: CreateCacheEntry (Reservation)
	http.HandleFunc(
		"/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
		srv.handleTwirpCreateCacheEntry,
	)

	// serve Swagger spec and UI
	http.HandleFunc("/swagger.json", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
		w.Header().Set("Content-Type", "application/json")
		w.Write(swaggerSpec)
		log.Printf("DEBUG: Served swagger.json")
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusOK)
	})
	http.HandleFunc("/docs", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Tenki Cache API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist/swagger-ui.css" />
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist/swagger-ui-bundle.js"></script>
  <script>
    window.onload = function() {
      SwaggerUIBundle({ url: "/swagger.json", dom_id: "#swagger-ui" });
    };
  </script>
</body>
</html>`))
		log.Printf("DEBUG: Served Swagger UI HTML for /docs")
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusOK)
	})
	http.HandleFunc("/docs/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
		log.Printf("DEBUG: Redirecting /docs/ to /docs")
		http.Redirect(w, r, "/docs", http.StatusMovedPermanently)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusMovedPermanently)
	})

	log.Printf("INFO: TenkiCloud cache server starting to listen on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil)) // This blocks, logs fatal error on exit
}

// reserveRecord holds metadata from reserve phase, including which user token reserved it.
type reserveRecord struct {
	Key         string
	Version     string
	RunId       string
	CacheId     int64
	Token       string
	Size        int64
	Compression string
}

// server holds in-memory storage and state.
type server struct {
	storageDir  string
	mu          sync.Mutex // Protects all maps and nextCacheID
	nextCacheID int64
	reserves    map[string]*reserveRecord
	caches      map[string]*cacheEntry
	byUpload    map[string]*cacheEntry
	tokens      map[string]*tokenInfo
}

// requireAuth validates the Bearer token.
// Returns the token string and whether auth succeeded. Logs errors and writes response on failure.
func (s *server) requireAuth(w http.ResponseWriter, r *http.Request) (string, bool) {
	log.Printf("DEBUG: requireAuth: Checking auth for %s", r.URL.Path)
	// Download path might have different auth needs or be public
	if strings.HasPrefix(r.URL.Path, "/download/") {
		log.Printf("DEBUG: requireAuth: Skipping auth for download path %s", r.URL.Path)
		// Depending on requirements, you might still want some auth here,
		// but based on original code, it was skipped. Return empty token but true.
		return "", true
	}

	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		log.Printf("WARN: requireAuth: Unauthorized - Missing or invalid 'Bearer ' prefix in Authorization header for %s", r.URL.Path)
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(errorResponse{Message: "Unauthorized: Invalid or missing Bearer token"})
		return "", false
	}

	token := strings.TrimPrefix(auth, "Bearer ")
	if token == "" {
		log.Printf("WARN: requireAuth: Unauthorized - Empty token provided for %s", r.URL.Path)
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(errorResponse{Message: "Unauthorized: Empty token"})
		return "", false
	}

	// Log only prefix/suffix for security
	truncatedToken := truncateToken(token)
	log.Printf("DEBUG: requireAuth: Extracted token prefix %s... for %s", truncatedToken, r.URL.Path)

	s.mu.Lock()
	log.Printf("DEBUG: requireAuth: Acquired lock to check token %s...", truncatedToken)
	_, ok := s.tokens[token]
	s.mu.Unlock()
	log.Printf("DEBUG: requireAuth: Released lock after checking token %s...", truncatedToken)

	if !ok {
		log.Printf("WARN: requireAuth: Unauthorized - Token %s... not found for %s", truncatedToken, r.URL.Path)
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(errorResponse{Message: "Unauthorized: Invalid token"})
		return "", false
	}

	log.Printf("DEBUG: requireAuth: Authentication successful for token %s... on path %s", truncatedToken, r.URL.Path)
	return token, true
}

// baseURL constructs the request URL origin (scheme://host).
func baseURL(r *http.Request) string {
	scheme := "http"
	// Check Forwarded headers or X-Forwarded-Proto if behind a proxy
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	} else if r.TLS != nil {
		scheme = "https"
	}
	host := r.Host
	if forwardedHost := r.Header.Get("X-Forwarded-Host"); forwardedHost != "" {
		host = forwardedHost
	}
	url := fmt.Sprintf("%s://%s", scheme, host)
	log.Printf("DEBUG: baseURL: Determined base URL: %s (Scheme: %s, Host: %s, TLS: %v, X-Forwarded-Proto: %s, X-Forwarded-Host: %s)",
		url, scheme, r.Host, r.TLS != nil, r.Header.Get("X-Forwarded-Proto"), r.Header.Get("X-Forwarded-Host"))
	return url
}

// handleCaches supports POST (reserve) and GET (listing).
func (s *server) handleCaches(w http.ResponseWriter, r *http.Request) {
	log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
	// Authenticate request first
	token, ok := s.requireAuth(w, r)
	if !ok {
		// requireAuth handles logging and response
		log.Printf("DEBUG: handleCaches: Authentication failed.")
		log.Printf("DEBUG: << Response END: %s %s Status: %d (Unauthorized)", r.Method, r.URL.Path, http.StatusUnauthorized)
		return
	}
	log.Printf("DEBUG: handleCaches: Authentication successful for token prefix %s...", truncateToken(token))

	switch r.Method {
	case "GET":
		s.handleListCaches(w, r, token)
	case "POST":
		s.handleReserveCache(w, r, token)
	default:
		log.Printf("WARN: handleCaches: Method %s not allowed for %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusMethodNotAllowed)
	}
}

func (s *server) handleListCaches(w http.ResponseWriter, r *http.Request, token string) {
	log.Printf("DEBUG: handleListCaches: Processing GET request")
	key := r.URL.Query().Get("key")
	version := r.URL.Query().Get("version") // Although original code didn't use version for listing, it's often paired with key
	log.Printf("DEBUG: handleListCaches: Listing caches for Key='%s', Version='%s'", key, version)
	if key == "" {
		log.Printf("WARN: handleListCaches: 'key' query parameter is required for listing.")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorResponse{Message: "'key' query parameter is required"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusBadRequest)
		return
	}

	var list []artifactCache
	s.mu.Lock()
	log.Printf("DEBUG: handleListCaches: Acquired lock for cache listing")
	log.Printf("DEBUG: handleListCaches: Iterating %d cached entries", len(s.caches))
	for compKey, e := range s.caches {
		// Match logic based on original code (exact key match)
		// Key format is key|version
		if e.Key == key {
			log.Printf("DEBUG: handleListCaches: Match found for key '%s' (composite: %s)", key, compKey)
			list = append(list, artifactCache{
				CacheKey:     e.Key,
				CacheVersion: e.Version, // Include version in response
				Scope:        "",        // Scope seems unused/fixed in original code
				CreationTime: e.CreationTime.Format(time.RFC3339),
				Size:         e.Size,
				Compression:  e.Compression,
			})
		} else {
			//log.Printf("DEBUG: handleListCaches: No match for key '%s' (composite: %s, entry key: %s)", key, compKey, e.Key) // Too verbose maybe
		}
	}
	s.mu.Unlock()
	log.Printf("DEBUG: handleListCaches: Released lock after cache listing")

	response := listResponse{TotalCount: len(list), ArtifactCaches: list}
	log.Printf("DEBUG: handleListCaches: Found %d matching entries for key '%s'. Responding.", len(list), key)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
	log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusOK)
}

func (s *server) handleReserveCache(w http.ResponseWriter, r *http.Request, token string) {
	log.Printf("DEBUG: handleReserveCache: Processing POST request (reserve)")
	var req reserveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("WARN: handleReserveCache: Failed to decode request body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorResponse{Message: "invalid request: " + err.Error()})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusBadRequest)
		return
	}
	log.Printf("DEBUG: handleReserveCache: Decoded request: Key='%s', Version='%s', CacheSize=%d, RunId='%s'", req.Key, req.Version, req.CacheSize, req.RunId)

	// Enforce per-user quota
	s.mu.Lock()
	log.Printf("DEBUG: handleReserveCache: Acquired lock for quota check")
	user, userFound := s.tokens[token]
	s.mu.Unlock() // Release lock after reading user info
	log.Printf("DEBUG: handleReserveCache: Released lock after quota check")

	if !userFound {
		// Should not happen if requireAuth passed, but defensive check
		log.Printf("ERROR: handleReserveCache: Token %s... passed auth but not found during quota check!", truncateToken(token))
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(errorResponse{Message: "Internal server error: user token inconsistency"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
		return
	}
	log.Printf("DEBUG: handleReserveCache: Checking quota for token %s... (Quota: %d B, Used: %d B, Requested: %d B)", truncateToken(token), user.Quota, user.Used, req.CacheSize)

	// Per-file size limit check
	if req.CacheSize > user.Quota {
		msg := fmt.Sprintf(
			"Cache size of %d B (~%d MB) exceeds user quota of %d B (~%d MB)",
			req.CacheSize, req.CacheSize/(1024*1024), user.Quota, user.Quota/(1024*1024))
		log.Printf("WARN: handleReserveCache: Quota exceeded (file size): %s", msg)
		w.WriteHeader(http.StatusBadRequest) // Actions cache uses 400 for quota exceeded
		json.NewEncoder(w).Encode(errorResponse{Message: msg})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusBadRequest)
		return
	}

	// Cumulative usage limit check
	if user.Used+req.CacheSize > user.Quota {
		msg := fmt.Sprintf(
			"Quota exceeded: current usage %d B (~%d MB) + requested %d B (~%d MB) > quota %d B (~%d MB)",
			user.Used, user.Used/(1024*1024), req.CacheSize, req.CacheSize/(1024*1024), user.Quota, user.Quota/(1024*1024))
		log.Printf("WARN: handleReserveCache: Quota exceeded (cumulative): %s", msg)
		w.WriteHeader(http.StatusBadRequest) // Actions cache uses 400 for quota exceeded
		json.NewEncoder(w).Encode(errorResponse{Message: msg})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusBadRequest)
		return
	}
	log.Printf("DEBUG: handleReserveCache: Quota check passed for token %s...", truncateToken(token))

	// Generate upload ID
	uploadId, err := newUploadId()
	if err != nil {
		log.Printf("ERROR: handleReserveCache: Failed to generate upload ID: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(errorResponse{Message: "Internal server error: failed to generate upload ID"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
		return
	}
	log.Printf("DEBUG: handleReserveCache: Generated UploadId: %s", uploadId)

	s.mu.Lock()
	log.Printf("DEBUG: handleReserveCache: Acquired lock to record reservation")
	cacheId := s.nextCacheID
	s.nextCacheID++
	// Record reserve with user token for ownership check during commit/upload
	record := &reserveRecord{
		Key:         req.Key,
		Version:     req.Version,
		CacheId:     cacheId,
		Token:       token,
		Size:        req.CacheSize,
		Compression: "gzip",
	}
	s.reserves[uploadId] = record
	s.mu.Unlock()
	log.Printf("DEBUG: handleReserveCache: Released lock after recording reservation")
	log.Printf("INFO: handleReserveCache: Reserved cache: UploadId=%s, CacheId=%d, Key='%s', Version='%s', Size=%d, Token=%s...",
		uploadId, cacheId, req.Key, req.Version, req.CacheSize, truncateToken(token))

	// Calculate number of parts and presign URLs
	parts := int((req.CacheSize + defaultChunkSize - 1) / defaultChunkSize)
	urls := make([]string, parts)
	origin := baseURL(r)
	log.Printf("DEBUG: handleReserveCache: Calculated %d upload parts for size %d B with chunk size %d B", parts, req.CacheSize, defaultChunkSize)
	for i := 0; i < parts; i++ {
		urls[i] = fmt.Sprintf("%s/upload/%s/%d", origin, uploadId, i)
	}
	log.Printf("DEBUG: handleReserveCache: Generated %d presigned URLs (first: %s)", len(urls), urls[0])

	response := reserveResponse{Result: reserveResult{
		UploadId:      uploadId,
		PresignedUrls: urls,
		ChunkSize:     defaultChunkSize,
	}}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated) // 201 Created is appropriate for a successful reservation
	json.NewEncoder(w).Encode(response)
	log.Printf("DEBUG: handleReserveCache: Responded with reservation details for UploadId %s", uploadId)
	log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusCreated)
}

// handleUpload accepts PUTs to /upload/{uploadId}/{partIndex}.
func (s *server) handleUpload(w http.ResponseWriter, r *http.Request) {
	log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
	if r.Method != "PUT" {
		log.Printf("WARN: handleUpload: Method %s not allowed, expected PUT for %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusMethodNotAllowed)
		return
	}

	// Authenticate request - check token validity AND ownership of the uploadId
	token, ok := s.requireAuth(w, r)
	if !ok {
		log.Printf("DEBUG: handleUpload: Authentication failed.")
		log.Printf("DEBUG: << Response END: %s %s Status: %d (Unauthorized)", r.Method, r.URL.Path, http.StatusUnauthorized)
		return
	}
	// Note: requireAuth only checks if token *exists*. We need to check if *this token* owns the uploadId.

	// Parse path: /upload/{uploadId}/{partIndex}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 || parts[0] != "upload" {
		log.Printf("WARN: handleUpload: Invalid URL path format: %s", r.URL.Path)
		w.WriteHeader(http.StatusNotFound) // Or BadRequest? NotFound seems reasonable.
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusNotFound)
		return
	}
	uploadId := parts[1]
	idxStr := parts[2]
	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		log.Printf("WARN: handleUpload: Invalid part index '%s' in path %s: %v", idxStr, r.URL.Path, err)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorResponse{Message: "Invalid part index"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusBadRequest)
		return
	}
	log.Printf("DEBUG: handleUpload: Parsed UploadId=%s, PartIndex=%d", uploadId, idx)

	// Verify reserve exists and belongs to this token
	s.mu.Lock()
	log.Printf("DEBUG: handleUpload: Acquired lock to check reservation %s", uploadId)
	reserve, reserveFound := s.reserves[uploadId]
	s.mu.Unlock() // Release lock after reading reserve info
	log.Printf("DEBUG: handleUpload: Released lock after checking reservation %s", uploadId)

	if !reserveFound {
		log.Printf("WARN: handleUpload: Reservation not found for UploadId %s", uploadId)
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(errorResponse{Message: "Upload ID not found or expired"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusNotFound)
		return
	}
	log.Printf("DEBUG: handleUpload: Found reservation for UploadId %s (Key: %s, Version: %s)", uploadId, reserve.Key, reserve.Version)

	// Check ownership
	if reserve.Token != token {
		log.Printf("WARN: handleUpload: Token mismatch for UploadId %s. Request token %s... != Reserved token %s...",
			uploadId, truncateToken(token), truncateToken(reserve.Token))
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(errorResponse{Message: "Forbidden: Token does not match reservation owner"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusForbidden)
		return
	}
	log.Printf("DEBUG: handleUpload: Token ownership verified for UploadId %s", uploadId)

	// Create directory for upload parts if it doesn't exist
	dir := filepath.Join(s.storageDir, uploadId)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("ERROR: handleUpload: Failed to create directory %s: %v", dir, err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(errorResponse{Message: "Internal server error: failed to create storage directory"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
		return
	}
	partPath := filepath.Join(dir, fmt.Sprintf("%d.part", idx))
	log.Printf("DEBUG: handleUpload: Preparing to write to part file: %s", partPath)

	f, err := os.Create(partPath)
	if err != nil {
		log.Printf("ERROR: handleUpload: Failed to create part file %s: %v", partPath, err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(errorResponse{Message: "Internal server error: failed to create part file"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
		return
	}
	defer f.Close() // Ensure file is closed

	hash := md5.New() // Calculate MD5 hash for ETag
	writer := io.MultiWriter(f, hash)

	log.Printf("DEBUG: handleUpload: Copying request body to %s and calculating MD5...", partPath)
	startTime := time.Now()
	bytesWritten, err := io.Copy(writer, r.Body)
	duration := time.Since(startTime)

	if err != nil {
		log.Printf("ERROR: handleUpload: Failed to copy request body to %s: %v", partPath, err)
		// Attempt to remove partially written file
		f.Close() // Close it first
		_ = os.Remove(partPath)
		log.Printf("DEBUG: handleUpload: Attempted cleanup of partial file %s", partPath)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(errorResponse{Message: "Internal server error: failed to write part file"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
		return
	}

	etag := hex.EncodeToString(hash.Sum(nil))
	log.Printf("INFO: handleUpload: Successfully wrote %d bytes to %s in %v. ETag: %s", bytesWritten, partPath, duration, etag)

	w.Header().Set("ETag", "\""+etag+"\"") // Standard ETag format includes quotes
	w.WriteHeader(http.StatusOK)
	log.Printf("DEBUG: handleUpload: Responded OK for UploadId %s Part %d", uploadId, idx)
	log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusOK)
}

// handleCommit finalizes upload, assembles the archive, and updates cache state.
func (s *server) handleCommit(w http.ResponseWriter, r *http.Request) {
	log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
	if r.Method != "POST" {
		log.Printf("WARN: handleCommit: Method %s not allowed, expected POST for %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusMethodNotAllowed)
		return
	}

	// Authenticate request
	token, ok := s.requireAuth(w, r)
	if !ok {
		log.Printf("DEBUG: handleCommit: Authentication failed.")
		log.Printf("DEBUG: << Response END: %s %s Status: %d (Unauthorized)", r.Method, r.URL.Path, http.StatusUnauthorized)
		return
	}
	log.Printf("DEBUG: handleCommit: Authentication successful for token prefix %s...", truncateToken(token))

	var req commitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("WARN: handleCommit: Failed to decode request body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorResponse{Message: "invalid request: " + err.Error()})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusBadRequest)
		return
	}
	log.Printf("DEBUG: handleCommit: Decoded request: UploadId=%s, Size=%d, EtagsCount=%d", req.UploadId, req.Size, len(req.Etags))
	// Maybe log first few ETags if needed: log.Printf("DEBUG: Etags (first few): %v", req.Etags[:min(len(req.Etags), 5)])

	// --- Quota Check (again) and Reservation Lookup ---
	s.mu.Lock()
	log.Printf("DEBUG: handleCommit: Acquired lock for quota/reserve check (UploadId: %s)", req.UploadId)
	user, userFound := s.tokens[token]
	reserve, reserveFound := s.reserves[req.UploadId]
	s.mu.Unlock() // Release lock after reading data
	log.Printf("DEBUG: handleCommit: Released lock after quota/reserve check (UploadId: %s)", req.UploadId)

	if !userFound {
		// Should not happen
		log.Printf("ERROR: handleCommit: Token %s... passed auth but not found during commit!", truncateToken(token))
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(errorResponse{Message: "Internal server error: user token inconsistency"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
		return
	}
	if !reserveFound {
		log.Printf("WARN: handleCommit: Reservation not found for UploadId %s", req.UploadId)
		// Check if it was already committed?
		s.mu.Lock()
		committedEntry, alreadyCommitted := s.byUpload[req.UploadId]
		s.mu.Unlock()
		if alreadyCommitted {
			log.Printf("INFO: handleCommit: UploadId %s was already committed (CacheId: %d). Responding with existing CacheId.", req.UploadId, committedEntry.CacheId)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(struct{ CacheId int64 `json:"cacheId"`}{committedEntry.CacheId})
			log.Printf("DEBUG: << Response END: %s %s Status: %d (Already Committed)", r.Method, r.URL.Path, http.StatusOK)
			return
		}
		// If not committed and reserve not found, it's an error
		w.WriteHeader(http.StatusNotFound) // Or BadRequest? Client might be sending invalid/old ID.
		json.NewEncoder(w).Encode(errorResponse{Message: "Upload ID not found or expired"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusNotFound)
		return
	}
	log.Printf("DEBUG: handleCommit: Found reservation for UploadId %s", req.UploadId)

	// Check ownership again
	if reserve.Token != token {
		log.Printf("WARN: handleCommit: Token mismatch for UploadId %s. Request token %s... != Reserved token %s...",
			req.UploadId, truncateToken(token), truncateToken(reserve.Token))
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(errorResponse{Message: "Forbidden: Token does not match reservation owner"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusForbidden)
		return
	}
	log.Printf("DEBUG: handleCommit: Token ownership verified for UploadId %s", req.UploadId)

	// --- Quota check based on *committed* size ---
	log.Printf("DEBUG: handleCommit: Checking final quota for token %s... (Quota: %d B, Used: %d B, Commit Size: %d B)", truncateToken(token), user.Quota, user.Used, req.Size)
	// Per-file size limit check (using committed size)
	if req.Size > user.Quota {
		msg := fmt.Sprintf(
			"Committed cache size of %d B (~%d MB) exceeds user quota of %d B (~%d MB)",
			req.Size, req.Size/(1024*1024), user.Quota, user.Quota/(1024*1024))
		log.Printf("WARN: handleCommit: Quota exceeded (final file size): %s", msg)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorResponse{Message: msg})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusBadRequest)
		// Consider deleting uploaded parts if commit fails due to quota? Requires cleanup logic.
		return
	}
	// Cumulative usage limit check (using committed size)
	if user.Used+req.Size > user.Quota {
		msg := fmt.Sprintf(
			"Quota exceeded: current usage %d B (~%d MB) + committed %d B (~%d MB) > quota %d B (~%d MB)",
			user.Used, user.Used/(1024*1024), req.Size, req.Size/(1024*1024), user.Quota, user.Quota/(1024*1024))
		log.Printf("WARN: handleCommit: Quota exceeded (final cumulative): %s", msg)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorResponse{Message: msg})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusBadRequest)
		// Consider deleting uploaded parts
		return
	}
	log.Printf("DEBUG: handleCommit: Final quota check passed for token %s...", truncateToken(token))

	// --- Assemble the final archive ---
	partsDir := filepath.Join(s.storageDir, req.UploadId)
	// Use UploadId in filename to avoid collisions if keys/versions are reused quickly
	// Determine file extension based on stored compression type
	fileExtension := ".tar.gz"
	if reserve.Compression == "zstd" {
		fileExtension = ".tar.zst"
	}
	finalFilename := fmt.Sprintf("%s-%s-%s%s", sanitizeFilename(reserve.Key), sanitizeFilename(reserve.Version), req.UploadId, fileExtension)
	finalPath := filepath.Join(s.storageDir, finalFilename)
	log.Printf("DEBUG: handleCommit: Assembling final archive at: %s (Compression: %s)", finalPath, reserve.Compression)

	out, err := os.Create(finalPath)
	if err != nil {
		log.Printf("ERROR: handleCommit: Failed to create final archive file %s: %v", finalPath, err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(errorResponse{Message: "Internal server error: failed to create archive file"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
		return
	}
	defer out.Close() // Ensure output file is closed

	var totalBytesWritten int64 = 0
	expectedParts := len(req.Etags) // Assuming number of Etags matches number of uploaded parts
	log.Printf("DEBUG: handleCommit: Starting assembly of %d parts from %s", expectedParts, partsDir)
	startTime := time.Now()

	for i := 0; i < expectedParts; i++ {
		partFile := filepath.Join(partsDir, fmt.Sprintf("%d.part", i))
		// log.Printf("DEBUG: handleCommit: Appending part %d: %s", i, partFile) // Can be very verbose
		in, err := os.Open(partFile)
		if err != nil {
			log.Printf("ERROR: handleCommit: Failed to open part file %s: %v", partFile, err)
			// Attempt cleanup of final file
			out.Close()
			_ = os.Remove(finalPath)
			log.Printf("DEBUG: handleCommit: Attempted cleanup of partial archive %s", finalPath)
			w.WriteHeader(http.StatusInternalServerError) // Indicate failure during assembly
			json.NewEncoder(w).Encode(errorResponse{Message: "Internal server error: failed to read cache part"})
			log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
			return
		}

		bytesCopied, err := io.Copy(out, in)
		in.Close() // Close input part file immediately after copy

		if err != nil {
			log.Printf("ERROR: handleCommit: Failed to copy part file %s to archive: %v", partFile, err)
			// Attempt cleanup
			out.Close()
			_ = os.Remove(finalPath)
			log.Printf("DEBUG: handleCommit: Attempted cleanup of partial archive %s", finalPath)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(errorResponse{Message: "Internal server error: failed to assemble archive"})
			log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
			return
		}
		totalBytesWritten += bytesCopied
		// log.Printf("DEBUG: handleCommit: Copied %d bytes from part %d", bytesCopied, i) // Verbose
	}
	duration := time.Since(startTime)
	log.Printf("INFO: handleCommit: Successfully assembled archive %s (%d bytes written) in %v", finalPath, totalBytesWritten, duration)

	// Optional: Verify final size matches committed size
	if totalBytesWritten != req.Size {
		log.Printf("WARN: handleCommit: Final assembled size %d bytes does not match committed size %d bytes for UploadId %s", totalBytesWritten, req.Size, req.UploadId)
		// Decide how to handle this - maybe proceed but log, or fail? Let's proceed but warn.
	}

	// --- Update Cache State ---
	origin := baseURL(r)
	downloadURL := fmt.Sprintf("%s/download/%s", origin, req.UploadId) // Use UploadId for download lookup
	entry := &cacheEntry{
		Key:             reserve.Key,
		Version:         reserve.Version,
		RunId:           reserve.RunId,
		CacheId:         reserve.CacheId,
		ArchivePath:     finalPath, // Store the actual file path
		ArchiveLocation: downloadURL, // Store the URL to serve
		CreationTime:    time.Now().UTC(),
		Compression:     reserve.Compression,
		Size:            req.Size,
	}
	compositeKey := reserve.Key + "|" + reserve.Version

	s.mu.Lock()
	log.Printf("DEBUG: handleCommit: Acquired lock to commit cache entry and update state")
	// Add to main cache map (key|version -> entry)
	s.caches[compositeKey] = entry
	// Add to lookup map (uploadId -> entry) for downloads
	s.byUpload[req.UploadId] = entry
	// Remove the reservation record
	delete(s.reserves, req.UploadId)
	// Update token usage (use req.Size as reported by client)
	user = s.tokens[token] // Re-fetch user within lock
	user.Used += req.Size
	log.Printf("DEBUG: handleCommit: Updated token %s... usage. New Used: %d B", truncateToken(token), user.Used)
	s.mu.Unlock()
	log.Printf("DEBUG: handleCommit: Released lock after committing state")

	log.Printf("INFO: handleCommit: Committed cache: CacheId=%d, Key='%s', Version='%s', Size=%d, Path=%s, Compression='%s'",
		entry.CacheId, entry.Key, entry.Version, req.Size, entry.ArchivePath, entry.Compression)

	// --- Cleanup Uploaded Parts ---
	log.Printf("DEBUG: handleCommit: Attempting to remove parts directory: %s", partsDir)
	err = os.RemoveAll(partsDir)
	if err != nil {
		log.Printf("WARN: handleCommit: Failed to remove parts directory %s after commit: %v", partsDir, err)
	} else {
		log.Printf("DEBUG: handleCommit: Successfully removed parts directory %s", partsDir)
	}

	// Respond with CacheId
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct{ CacheId int64 `json:"cacheId"`}{entry.CacheId})
	log.Printf("DEBUG: handleCommit: Responded with CacheId %d", entry.CacheId)
	log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusOK)
}

// handleGetCache implements GET /cache?keys=...&version=... (cache lookup)
func (s *server) handleGetCache(w http.ResponseWriter, r *http.Request) {
	log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
	if r.Method != "GET" {
		log.Printf("WARN: handleGetCache: Method %s not allowed, expected GET for %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusMethodNotAllowed)
		return
	}

	// Authenticate request
	token, ok := s.requireAuth(w, r)
	if !ok {
		log.Printf("DEBUG: handleGetCache: Authentication failed.")
		log.Printf("DEBUG: << Response END: %s %s Status: %d (Unauthorized)", r.Method, r.URL.Path, http.StatusUnauthorized)
		return
	}
	log.Printf("DEBUG: handleGetCache: Authentication successful for token prefix %s...", truncateToken(token))

	keysParam := r.URL.Query().Get("keys")
	version := r.URL.Query().Get("version")
	if keysParam == "" || version == "" {
		log.Printf("WARN: handleGetCache: Missing required query parameters 'keys' and/or 'version'. Keys: '%s', Version: '%s'", keysParam, version)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorResponse{Message: "Missing required query parameters: keys, version"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusBadRequest)
		return
	}

	keys := strings.Split(keysParam, ",")
	log.Printf("DEBUG: handleGetCache: Looking for cache with Keys=%v, Version=%s", keys, version)

	s.mu.Lock()
	log.Printf("DEBUG: handleGetCache: Acquired lock for cache lookup")
	defer s.mu.Unlock() // Use defer for safety within the loop/return
	log.Printf("DEBUG: handleGetCache: Checking %d potential keys against %d cache entries", len(keys), len(s.caches))

	for _, k := range keys {
		trimmedKey := strings.TrimSpace(k)
		if trimmedKey == "" {
			continue
		}
		comp := trimmedKey + "|" + version
		log.Printf("DEBUG: handleGetCache: Checking composite key: %s", comp)
		if e, found := s.caches[comp]; found {
			log.Printf("INFO: handleGetCache: Cache HIT for Key='%s', Version='%s'. Composite: %s. CacheId: %d, Location: %s",
				trimmedKey, version, comp, e.CacheId, e.ArchiveLocation)
			resp := struct {
				CacheId         int64  `json:"cacheId"`
				ArchiveLocation string `json:"archiveLocation"`
				CacheKey        string `json:"cacheKey"`
				CacheVersion    string `json:"cacheVersion"`
				Scope           string `json:"scope"`
				CreationTime    string `json:"creationTime"`
				Compression     string `json:"compression"`
				Size            int64  `json:"size,omitempty"`
			}{
				CacheId:         e.CacheId,
				ArchiveLocation: e.ArchiveLocation,
				CacheKey:        e.Key,
				CacheVersion:    e.Version,
				Scope:           "",
				CreationTime:    e.CreationTime.Format(time.RFC3339),
				Compression:     e.Compression,
				Size:            e.Size,
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			log.Printf("DEBUG: handleGetCache: Responded with cache hit details.")
			log.Printf("DEBUG: << Response END: %s %s Status: %d (Cache Hit)", r.Method, r.URL.Path, http.StatusOK)
			return // Found a match, return immediately
		} else {
			log.Printf("DEBUG: handleGetCache: Cache MISS for composite key: %s", comp)
		}
	}

	// If loop completes without finding a match
	log.Printf("INFO: handleGetCache: Cache MISS for all requested Keys=%v, Version=%s", keys, version)
	w.WriteHeader(http.StatusNoContent) // 204 No Content indicates cache miss
	log.Printf("DEBUG: << Response END: %s %s Status: %d (Cache Miss)", r.Method, r.URL.Path, http.StatusNoContent)
}

// handleDownload serves the combined archive at /download/{uploadId}.
func (s *server) handleDownload(w http.ResponseWriter, r *http.Request) {
	log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
	if r.Method != "GET" {
		log.Printf("WARN: handleDownload: Method %s not allowed, expected GET for %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusMethodNotAllowed)
		return
	}

	// Auth check is currently skipped here by requireAuth, but could be added if needed.

	// Parse path: /download/{uploadId}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 2 || parts[0] != "download" {
		log.Printf("WARN: handleDownload: Invalid URL path format: %s", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusNotFound)
		return
	}
	uploadId := parts[1]
	log.Printf("DEBUG: handleDownload: Request to download archive for UploadId=%s", uploadId)

	s.mu.Lock()
	log.Printf("DEBUG: handleDownload: Acquired lock to look up archive path for UploadId %s", uploadId)
	entry, ok := s.byUpload[uploadId]
	s.mu.Unlock()
	log.Printf("DEBUG: handleDownload: Released lock after looking up UploadId %s", uploadId)

	if !ok {
		log.Printf("WARN: handleDownload: No committed cache entry found for UploadId %s", uploadId)
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(errorResponse{Message: "Cache artifact not found"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusNotFound)
		return
	}

	log.Printf("INFO: handleDownload: Found archive for UploadId %s at path: %s. Serving file.", uploadId, entry.ArchivePath)
	// Use http.ServeFile for efficient serving with range requests, content type detection etc.
	// ServeFile handles setting headers and writing the response body.
	// Note: Any logging *after* ServeFile might not execute if ServeFile handles the full response lifecycle.
	http.ServeFile(w, r, entry.ArchivePath)
	// It's hard to reliably log the *end* status here as ServeFile takes over.
	// We can assume success if no error occurs before ServeFile.
	log.Printf("DEBUG: handleDownload: Handed off response to http.ServeFile for %s", entry.ArchivePath)
	// Log entry might appear before file transfer finishes completely.
}

// handleTokenCreation generates a new token.
// This endpoint is unauthenticated by design in the original code.
func (s *server) handleTokenCreation(w http.ResponseWriter, r *http.Request) {
	log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
	if r.Method != "POST" {
		log.Printf("WARN: handleTokenCreation: Method %s not allowed, expected POST for %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusMethodNotAllowed)
		return
	}

	// Parse optional quota in GB from JSON body {"quotaGB": <int>}
	var req struct{ QuotaGB int64 `json:"quotaGB"` }
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil && err != io.EOF { // Allow empty body (EOF)
		log.Printf("WARN: handleTokenCreation: Failed to decode JSON body (ignoring, using default quota): %v", err)
		// Don't fail, just use default quota
	} else if err == nil {
		log.Printf("DEBUG: handleTokenCreation: Decoded request body, found QuotaGB: %d", req.QuotaGB)
	} else {
		log.Printf("DEBUG: handleTokenCreation: No request body or empty body, using default quota.")
	}

	// Initialize quota to default, override if valid value provided
	var quota int64 = defaultQuota
	if req.QuotaGB > 0 {
		quota = req.QuotaGB * 1024 * 1024 * 1024 // Convert GB to Bytes
		log.Printf("DEBUG: handleTokenCreation: Using requested quota: %d GB (%d B)", req.QuotaGB, quota)
	} else {
		log.Printf("DEBUG: handleTokenCreation: Using default quota: %d B (~%d GB)", defaultQuota, defaultQuota/(1024*1024*1024))
	}

	// Generate token string (using the same random func as uploadId)
	tokenID, err := newUploadId()
	if err != nil {
		log.Printf("ERROR: handleTokenCreation: Failed to generate token ID: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(errorResponse{Message: "Internal server error: failed to generate token"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
		return
	}
	log.Printf("DEBUG: handleTokenCreation: Generated new Token ID prefix: %s...", truncateToken(tokenID))

	// Store token info
	s.mu.Lock()
	log.Printf("DEBUG: handleTokenCreation: Acquired lock to store new token %s...", truncateToken(tokenID))
	s.tokens[tokenID] = &tokenInfo{Quota: quota, Used: 0}
	s.mu.Unlock()
	log.Printf("DEBUG: handleTokenCreation: Released lock after storing token")
	log.Printf("INFO: handleTokenCreation: Created new token prefix %s... with Quota %d B (~%d GB)", truncateToken(tokenID), quota, quota/(1024*1024*1024))

	// Return token and quota
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated) // 201 Created is suitable for resource creation
	resp := struct {
		Token string `json:"token"`
		Quota int64  `json:"quota"` // Return quota in Bytes
	}{Token: tokenID, Quota: quota}
	json.NewEncoder(w).Encode(resp)
	log.Printf("DEBUG: handleTokenCreation: Responded with new token details.")
	log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusCreated)
}

//go:embed swagger.json
var swaggerSpec []byte

// newUploadId generates a random 16-byte hex string.
func newUploadId() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		log.Printf("ERROR: newUploadId: Failed to read random bytes: %v", err)
		return "", fmt.Errorf("failed to read random bytes: %w", err)
	}
	id := hex.EncodeToString(b)
	// log.Printf("DEBUG: newUploadId: Generated ID: %s", id) // Maybe too verbose
	return id, nil
}

// sanitizeFilename replaces potentially problematic characters in keys/versions for use in filenames.
func sanitizeFilename(input string) string {
	// Replace common problematic chars like '/', '\', ':', '*', '?', '"', '<', '>', '|'
	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "-", // Replace pipe specifically as it's used as our internal separator
	)
	return replacer.Replace(input)
}

// truncateToken returns a shortened version for logging (prefix/suffix).
func truncateToken(token string) string {
	if len(token) > 8 {
		return token[:4] + "..." + token[len(token)-4:]
	}
	return token // Return as is if too short
}

// writeTwirpError is a helper to write JSON errors consistent with Twirp.
func writeTwirpError(w http.ResponseWriter, code, msg string, httpStatus int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus) // Set appropriate HTTP status
	json.NewEncoder(w).Encode(errorResponse{
		Code:    code, // Twirp error code (e.g., "internal", "invalid_argument")
		Message: msg,
	})
}

// handleTwirpCreateCacheEntry implements the reservation logic for the Twirp endpoint.
func (s *server) handleTwirpCreateCacheEntry(w http.ResponseWriter, r *http.Request) {
	log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
	log.Printf("DEBUG: Handling Twirp request for CreateCacheEntry")
	if r.Method != "POST" {
		log.Printf("ERROR: Twirp CreateCacheEntry: Method %s not allowed, expected POST", r.Method)
		writeTwirpError(w, "malformed", "Method not allowed, expected POST", http.StatusMethodNotAllowed)
		return
	}

	token, ok := s.requireAuth(w, r)
	if !ok {
		log.Printf("DEBUG: Twirp CreateCacheEntry: Authentication failed.")
		// requireAuth already wrote the error response
		return
	}
	log.Printf("DEBUG: Twirp CreateCacheEntry: Authentication successful for token prefix %s...", truncateToken(token))

	// Decode Twirp JSON body for CreateCacheEntry
	// Based on Actions Cache v2 protocol observation, seems simple: {key, version}
	var req struct {
		Key     string `json:"key"`
		Version string `json:"version"`
		// cacheSize is often unknown at reservation time in Actions Cache v2
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("ERROR: Twirp CreateCacheEntry: Malformed request body: %v", err)
		writeTwirpError(w, "malformed", "Malformed request: "+err.Error(), http.StatusBadRequest)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusBadRequest)
		return
	}
	if req.Key == "" || req.Version == "" {
		log.Printf("ERROR: Twirp CreateCacheEntry: Request must contain 'key' and 'version'")
		writeTwirpError(w, "invalid_argument", "Request must contain 'key' and 'version'", http.StatusBadRequest)
		return
	}
	log.Printf("DEBUG: Twirp CreateCacheEntry: Decoded request: Key='%s', Version='%s'", req.Key, req.Version)

	// --- Quota Check (Harder without CacheSize upfront) ---
	// Actions Cache v2 often doesn't know size until commit.
	// We *could* enforce a max reserve size or skip the upfront size check here
	// and rely solely on the check during commit. Let's skip upfront size check for Twirp path.
	log.Printf("DEBUG: Twirp CreateCacheEntry: Skipping upfront cache size quota check (size often unknown). Will check during commit.")

	// Check token validity, needed for ownership checks later.
	s.mu.Lock()
	_, userFound := s.tokens[token]
	s.mu.Unlock() // Release lock after reading user info

	if !userFound {
		log.Printf("ERROR: Twirp CreateCacheEntry: Token %s... passed auth but not found!", truncateToken(token))
		writeTwirpError(w, "internal", "Internal server error: user token inconsistency", http.StatusInternalServerError)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
		return
	}

	// --- Check if cache entry already exists ---
	// Actions cache protocol: If exact match exists, return it immediately (no new reservation needed).
	compositeKey := req.Key + "|" + req.Version
	s.mu.Lock()
	log.Printf("DEBUG: Twirp CreateCacheEntry: Acquired lock check existing cache")
	existingEntry, found := s.caches[compositeKey]
	s.mu.Unlock()
	log.Printf("DEBUG: Twirp CreateCacheEntry: Released lock after check existing cache")

	if found {
		log.Printf("INFO: Twirp CreateCacheEntry: Cache HIT for Key='%s', Version='%s'. Returning existing CacheId: %d", req.Key, req.Version, existingEntry.CacheId)
		// Respond similar to commit, indicating the cache ID
		resp := struct {
			CacheId int64 `json:"cacheId"` // Actions Cache expects cacheId
		}{CacheId: existingEntry.CacheId}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		log.Printf("DEBUG: << Response END: %s %s Status: %d (Cache Hit)", r.Method, r.URL.Path, http.StatusOK) // 200 OK for existing cache
		return
	}
	log.Printf("DEBUG: Twirp CreateCacheEntry: No existing cache found for Key='%s', Version='%s'. Proceeding with reservation.", req.Key, req.Version)

	// --- Generate Upload ID and Reserve ---
	// We still need an uploadId internally to manage the upload process.
	uploadId, err := newUploadId()
	if err != nil {
		log.Printf("ERROR: Twirp CreateCacheEntry: Failed to generate upload ID: %v", err)
		writeTwirpError(w, "internal", "Internal server error: failed to generate upload ID", http.StatusInternalServerError)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
		return
	}
	log.Printf("DEBUG: Twirp CreateCacheEntry: Generated internal UploadId: %s", uploadId)

	// --- Determine Compression Method ---
	// Check headers like X-Actions-Cache-Compression-Method
	compressionMethod := "gzip" // Default
	clientCompression := r.Header.Get("X-Actions-Cache-Compression-Method")
	if clientCompression == "zstd" {
		compressionMethod = "zstd"
		log.Printf("DEBUG: Twirp CreateCacheEntry: Detected client requesting zstd compression via header.")
	} else if clientCompression != "" && clientCompression != "gzip" {
		log.Printf("WARN: Twirp CreateCacheEntry: Received unsupported X-Actions-Cache-Compression-Method: %s. Defaulting to gzip.", clientCompression)
	} else {
		log.Printf("DEBUG: Twirp CreateCacheEntry: No specific compression requested or gzip requested. Using gzip.")
	}

	s.mu.Lock()
	log.Printf("DEBUG: Twirp CreateCacheEntry: Acquired lock to record reservation")
	cacheId := s.nextCacheID
	s.nextCacheID++
	record := &reserveRecord{
		Key:         req.Key,
		Version:     req.Version,
		CacheId:     cacheId,
		Token:       token,
		Size:        -1,
		Compression: compressionMethod,
	}
	// Use the *numeric cacheId* as the key for reserves in the Twirp context?
	// No, let's stick to uploadId for internal tracking of uploads, but return cacheId to the client.
	s.reserves[uploadId] = record
	s.mu.Unlock()
	log.Printf("DEBUG: Twirp CreateCacheEntry: Released lock after recording reservation")
	log.Printf("INFO: Twirp CreateCacheEntry: Reserved cache: CacheId=%d (Internal UploadId=%s), Key='%s', Version='%s', Compression='%s', Token=%s...",
		cacheId, uploadId, req.Key, req.Version, compressionMethod, truncateToken(token))

	// --- Build Twirp Response ---
	// Actions Cache CreateCacheEntry returns the cacheId
	resp := struct {
		CacheId int64 `json:"cacheId"`
	}{CacheId: cacheId}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // 200 OK seems standard for successful reservation in Twirp
	json.NewEncoder(w).Encode(resp)
	log.Printf("DEBUG: Twirp CreateCacheEntry: Responded with CacheId %d", cacheId)
	log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusOK)
}

// Helper for logging ETags array cleanly (optional)
// func min(a, b int) int {
// 	if a < b {
// 		return a
// 	}
// 	return b
// }