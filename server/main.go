package main

import (
	_ "embed"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url" // Keep for joining paths
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
	// --- Hardcoded Base URL ---
	fixedBaseURL = "http://tenkicloud-cache-server.default.svc.cluster.local"
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
	ArchiveLocation string // Will store the *full* URL now
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

// reserveRecord holds metadata from reserve phase, including which user token reserved it.
type reserveRecord struct {
	Key         string
	Version     string
	RunId       string
	CacheId     int64
	Token       string
	Size        int64 // Expected size (might be -1 if unknown at reserve)
	Compression string
}

// server holds in-memory storage and state.
type server struct {
	storageDir        string
	mu                sync.Mutex // Protects all maps and nextCacheID
	nextCacheID       int64
	reserves          map[string]*reserveRecord // Key: uploadId
	caches            map[string]*cacheEntry    // Key: key|version (composite key)
	byUpload          map[string]*cacheEntry    // Key: uploadId (for download lookup after commit)
	tokens            map[string]*tokenInfo     // Key: token string
	cacheIdToUploadId map[int64]string          // Key: cacheId, Value: uploadId (maps numeric ID to hex upload ID)
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

	// --- Initialize server state ---
	srv := &server{
		storageDir:        storageDir,
		nextCacheID:       1, // Start cache IDs from 1
		reserves:          make(map[string]*reserveRecord),
		caches:            make(map[string]*cacheEntry),
		byUpload:          make(map[string]*cacheEntry),
		tokens:            make(map[string]*tokenInfo),
		cacheIdToUploadId: make(map[int64]string),
	}

	log.Printf("INFO: Registering HTTP handlers...")
	// Token creation endpoint (no auth)
	http.HandleFunc("/token", srv.handleTokenCreation)

	// REST API endpoints (subset, potentially conflicting with Twirp/Actions paths)
	http.HandleFunc("/cache", srv.handleGetCache) // GET for lookup
	http.HandleFunc("/caches", srv.handleCaches)  // POST for reserve, GET for list

	// Twirp RPC Emulation Endpoints
	http.HandleFunc(
		"/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL",
		srv.handleTwirpGetCacheEntryDownloadURL, // Separate handler for Twirp GET
	)
	http.HandleFunc(
		"/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
		srv.handleTwirpCreateCacheEntry, // Separate handler for Twirp POST (Reserve)
	)

	// Actions Artifact Cache API Endpoints (subset) - Preferred for Upload/Commit
	http.HandleFunc("/artifactcache/", srv.handleArtifactCache) // Routes PATCH /artifactcache/{cacheId} and POST /artifactcache/{cacheId}/commit

	// Download endpoint (uses UploadId from cache entry's ArchiveLocation)
	http.HandleFunc("/download/", srv.handleDownload)

	// --- Swagger UI ---
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
	// --- End Swagger UI ---

	log.Printf("INFO: TenkiCloud cache server starting to listen on :%s", port)
	log.Printf("INFO: Fixed Base URL for responses: %s", fixedBaseURL) // Log the fixed URL
	log.Fatal(http.ListenAndServe(":"+port, nil))                   // Blocks, logs fatal error on exit
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
		w.Header().Set("Content-Type", "application/json") // Ensure JSON error response
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(errorResponse{Message: "Unauthorized: Invalid or missing Bearer token"})
		return "", false
	}

	token := strings.TrimPrefix(auth, "Bearer ")
	if token == "" {
		log.Printf("WARN: requireAuth: Unauthorized - Empty token provided for %s", r.URL.Path)
		w.Header().Set("Content-Type", "application/json") // Ensure JSON error response
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
		w.Header().Set("Content-Type", "application/json") // Ensure JSON error response
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(errorResponse{Message: "Unauthorized: Invalid token"})
		return "", false
	}

	log.Printf("DEBUG: requireAuth: Authentication successful for token %s... on path %s", truncatedToken, r.URL.Path)
	return token, true
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
		s.handleReserveCache(w, r, token) // Uses REST reserve logic
	default:
		log.Printf("WARN: handleCaches: Method %s not allowed for %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusMethodNotAllowed)
	}
}

func (s *server) handleListCaches(w http.ResponseWriter, r *http.Request, token string) {
	log.Printf("DEBUG: handleListCaches: Processing GET request")
	key := r.URL.Query().Get("key")
	// version := r.URL.Query().Get("version") // Not used in original listing logic
	log.Printf("DEBUG: handleListCaches: Listing caches for Key='%s'", key)
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

// handleReserveCache implements the *REST* reservation logic (POST /caches)
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
		w.WriteHeader(http.StatusBadRequest)
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
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorResponse{Message: msg})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusBadRequest)
		return
	}
	log.Printf("DEBUG: handleReserveCache: Quota check passed for token %s...", truncateToken(token))

	// Check if already exists (REST API might conflict, Twirp handles this differently)
	compositeKey := req.Key + "|" + req.Version
	s.mu.Lock()
	if _, found := s.caches[compositeKey]; found {
		s.mu.Unlock()
		log.Printf("WARN: handleReserveCache (REST): Cache HIT for Key='%s', Version='%s'. Conflict.", req.Key, req.Version)
		w.WriteHeader(http.StatusConflict) // 409 Conflict is appropriate for REST
		json.NewEncoder(w).Encode(errorResponse{Message: "Cache entry already exists"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d (Conflict)", r.Method, r.URL.Path, http.StatusConflict)
		return
	}
	// No existing entry, proceed with reservation... (Still holding lock)

	// Generate upload ID
	uploadId, err := newUploadId()
	if err != nil {
		s.mu.Unlock() // Need to unlock before returning
		log.Printf("ERROR: handleReserveCache: Failed to generate upload ID: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(errorResponse{Message: "Internal server error: failed to generate upload ID"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
		return
	}
	log.Printf("DEBUG: handleReserveCache: Generated UploadId: %s", uploadId)

	cacheId := s.nextCacheID
	s.nextCacheID++
	// Record reserve
	record := &reserveRecord{
		Key:         req.Key,
		Version:     req.Version,
		RunId:       req.RunId,
		CacheId:     cacheId,
		Token:       token,
		Size:        req.CacheSize, // Store expected size
		Compression: "gzip",       // Assume gzip for REST reserve? Or check header? Let's default.
	}
	s.reserves[uploadId] = record
	s.cacheIdToUploadId[cacheId] = uploadId // Store mapping
	s.mu.Unlock() // RELEASE LOCK after state update
	log.Printf("DEBUG: handleReserveCache: Released lock after recording reservation")
	log.Printf("INFO: handleReserveCache: Reserved cache: UploadId=%s, CacheId=%d, Key='%s', Version='%s', Size=%d, Token=%s...",
		uploadId, cacheId, req.Key, req.Version, req.CacheSize, truncateToken(token))

	// Calculate number of parts and presign URLs using FIXED base URL
	parts := int((req.CacheSize + defaultChunkSize - 1) / defaultChunkSize)
	urls := make([]string, parts)
	log.Printf("DEBUG: handleReserveCache: Calculated %d upload parts for size %d B with chunk size %d B", parts, req.CacheSize, defaultChunkSize)
	// --- Use fixedBaseURL ---
	base, _ := url.Parse(fixedBaseURL) // Parse the fixed URL to join paths correctly
	for i := 0; i < parts; i++ {
		// Use the /artifactcache/ endpoint structure preferred by Actions v2/v3
		relativeUploadPath := fmt.Sprintf("/artifactcache/%d", cacheId) // Points to the PATCH endpoint
		// We might need to return the *same* URL for all parts, as the PATCH handles appending.
		// Let's return the base patch URL.
		uploadURL := base.JoinPath(relativeUploadPath)
		urls[i] = uploadURL.String() // Use the full URL
	}
	// --- End fixedBaseURL usage ---
	log.Printf("DEBUG: handleReserveCache: Generated %d upload URLs using fixed base URL (example: %s)", len(urls), urls[0])

	response := reserveResponse{Result: reserveResult{
		UploadId:      uploadId, // Return internal hex ID
		PresignedUrls: urls,     // Return URLs for PATCH endpoint
		ChunkSize:     defaultChunkSize,
	}}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated) // 201 Created
	json.NewEncoder(w).Encode(response)
	log.Printf("DEBUG: handleReserveCache: Responded with reservation details for UploadId %s", uploadId)
	log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusCreated)
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

	// Auth check is currently skipped here by requireAuth logic for this path.

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
	entry, ok := s.byUpload[uploadId] // Use the byUpload map populated during commit
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
	// Use http.ServeFile for efficient serving
	http.ServeFile(w, r, entry.ArchivePath)
	log.Printf("DEBUG: handleDownload: Handed off response to http.ServeFile for %s", entry.ArchivePath)
}

// handleTokenCreation generates a new token.
func (s *server) handleTokenCreation(w http.ResponseWriter, r *http.Request) {
	log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
	if r.Method != "POST" {
		log.Printf("WARN: handleTokenCreation: Method %s not allowed, expected POST for %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusMethodNotAllowed)
		return
	}

	var req struct{ QuotaGB int64 `json:"quotaGB"` }
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil && err != io.EOF {
		log.Printf("WARN: handleTokenCreation: Failed to decode JSON body (ignoring, using default quota): %v", err)
	} else if err == nil {
		log.Printf("DEBUG: handleTokenCreation: Decoded request body, found QuotaGB: %d", req.QuotaGB)
	} else {
		log.Printf("DEBUG: handleTokenCreation: No request body or empty body, using default quota.")
	}

	var quota int64 = defaultQuota
	if req.QuotaGB > 0 {
		quota = req.QuotaGB * 1024 * 1024 * 1024
		log.Printf("DEBUG: handleTokenCreation: Using requested quota: %d GB (%d B)", req.QuotaGB, quota)
	} else {
		log.Printf("DEBUG: handleTokenCreation: Using default quota: %d B (~%d GB)", defaultQuota, defaultQuota/(1024*1024*1024))
	}

	tokenID, err := newUploadId() // Use same random ID generator
	if err != nil {
		log.Printf("ERROR: handleTokenCreation: Failed to generate token ID: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(errorResponse{Message: "Internal server error: failed to generate token"})
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
		return
	}
	log.Printf("DEBUG: handleTokenCreation: Generated new Token ID prefix: %s...", truncateToken(tokenID))

	s.mu.Lock()
	log.Printf("DEBUG: handleTokenCreation: Acquired lock to store new token %s...", truncateToken(tokenID))
	s.tokens[tokenID] = &tokenInfo{Quota: quota, Used: 0}
	s.mu.Unlock()
	log.Printf("DEBUG: handleTokenCreation: Released lock after storing token")
	log.Printf("INFO: handleTokenCreation: Created new token prefix %s... with Quota %d B (~%d GB)", truncateToken(tokenID), quota, quota/(1024*1024*1024))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	resp := struct {
		Token string `json:"token"`
		Quota int64  `json:"quota"`
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
	return id, nil
}

// sanitizeFilename replaces potentially problematic characters.
func sanitizeFilename(input string) string {
	replacer := strings.NewReplacer(
		"/", "_", "\\", "_", ":", "_", "*", "_", "?", "_",
		"\"", "_", "<", "_", ">", "_", "|", "-", // Replace pipe used as separator
	)
	return replacer.Replace(input)
}

// truncateToken returns a shortened version for logging.
func truncateToken(token string) string {
	if len(token) > 8 {
		return token[:4] + "..." + token[len(token)-4:]
	}
	return token
}

// writeTwirpError is a helper to write JSON errors consistent with Twirp.
func writeTwirpError(w http.ResponseWriter, code, msg string, httpStatus int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(errorResponse{
		Code:    code,
		Message: msg,
	})
}

// --- Twirp Handler Implementations ---

// handleTwirpGetCacheEntryDownloadURL implements the lookup logic for the Twirp endpoint.
func (s *server) handleTwirpGetCacheEntryDownloadURL(w http.ResponseWriter, r *http.Request) {
	log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
	log.Printf("DEBUG: Handling Twirp request for GetCacheEntryDownloadURL")
	if r.Method != "POST" {
		log.Printf("ERROR: Twirp Get...: Method %s not allowed, expected POST", r.Method)
		writeTwirpError(w, "malformed", "Method not allowed, expected POST", http.StatusMethodNotAllowed)
		return
	}
	token, ok := s.requireAuth(w, r)
	if !ok {
		log.Printf("DEBUG: Twirp Get...: Authentication failed.")
		return // requireAuth handled response
	}
	log.Printf("DEBUG: Twirp Get...: Authentication successful for token prefix %s...", truncateToken(token))

	var req struct {
		Key         string   `json:"key"`
		RestoreKeys []string `json:"restoreKeys"`
		Version     string   `json:"version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("ERROR: Twirp Get...: Malformed request body: %v", err)
		writeTwirpError(w, "malformed", "Malformed request: "+err.Error(), http.StatusBadRequest)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusBadRequest)
		return
	}
	log.Printf("DEBUG: Twirp Get...: Decoded request: Key=%s, RestoreKeys=%v, Version=%s", req.Key, req.RestoreKeys, req.Version)

	s.mu.Lock()
	log.Printf("DEBUG: Twirp Get...: Acquired lock for cache lookup")
	var matchedKey string
	var matchedEntry *cacheEntry // Store the whole entry
	allKeys := append([]string{req.Key}, req.RestoreKeys...)
	log.Printf("DEBUG: Twirp Get...: Searching for keys: %v with version: %s", allKeys, req.Version)
	for _, k := range allKeys {
		trimmedKey := strings.TrimSpace(k)
		if trimmedKey == "" {
			continue
		}
		comp := trimmedKey + "|" + req.Version // Use pipe separator
		log.Printf("DEBUG: Twirp Get...: Checking composite key: %s", comp)
		if e, found := s.caches[comp]; found {
			log.Printf("DEBUG: Twirp Get...: Cache HIT for composite key: %s", comp)
			matchedKey = e.Key
			matchedEntry = e // Store the entry on hit
			log.Printf("DEBUG: Twirp Get...: Found entry: MatchedKey=%s, Location=%s", matchedKey, matchedEntry.ArchiveLocation)
			break // Stop on first match
		} else {
			log.Printf("DEBUG: Twirp Get...: Cache MISS for composite key: %s", comp)
		}
	}
	s.mu.Unlock()
	log.Printf("DEBUG: Twirp Get...: Released lock after cache lookup")

	// Build Twirp response
	resp := struct {
		Ok    bool        `json:"ok"`
		Entry interface{} `json:"entry,omitempty"` // Use interface{} to allow different structures
	}{}

	if matchedEntry == nil { // Check if we found an entry
		log.Printf("DEBUG: Twirp Get...: No matching cache entry found. Responding with ok=false")
		resp.Ok = false
	} else {
		log.Printf("DEBUG: Twirp Get...: Matching cache entry found. Responding with ok=true, MatchedKey=%s, URL=%s", matchedKey, matchedEntry.ArchiveLocation)
		resp.Ok = true
		// Construct the "entry" object expected by Actions Cache v2 client
		resp.Entry = map[string]interface{}{
			"cacheKey":        matchedEntry.Key,
			"scope":           "", // Assuming scope is not used
			"cacheVersion":    matchedEntry.Version,
			"archiveLocation": matchedEntry.ArchiveLocation, // Already the full URL
			"size":            matchedEntry.Size,
			"creationTime":    matchedEntry.CreationTime.Format(time.RFC3339),
			"compression":     matchedEntry.Compression,
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
	log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusOK)
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
		return // requireAuth handled response
	}
	log.Printf("DEBUG: Twirp CreateCacheEntry: Authentication successful for token prefix %s...", truncateToken(token))

	var req struct {
		Key     string `json:"key"`
		Version string `json:"version"`
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

	// Quota check skipped upfront for Twirp (size unknown)

	s.mu.Lock()
	_, userFound := s.tokens[token]
	s.mu.Unlock()
	if !userFound {
		log.Printf("ERROR: Twirp CreateCacheEntry: Token %s... passed auth but not found!", truncateToken(token))
		writeTwirpError(w, "internal", "Internal server error: user token inconsistency", http.StatusInternalServerError)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
		return
	}

	compositeKey := req.Key + "|" + req.Version
	s.mu.Lock()
	log.Printf("DEBUG: Twirp CreateCacheEntry: Acquired lock check existing cache")
	existingEntry, found := s.caches[compositeKey]
	s.mu.Unlock()
	log.Printf("DEBUG: Twirp CreateCacheEntry: Released lock after check existing cache")

	if found {
		log.Printf("INFO: Twirp CreateCacheEntry: Cache HIT for Key='%s', Version='%s'. Returning existing CacheId: %d", req.Key, req.Version, existingEntry.CacheId)
		resp := struct {
			CacheId int64 `json:"cacheId"`
		}{CacheId: existingEntry.CacheId}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		log.Printf("DEBUG: << Response END: %s %s Status: %d (Cache Hit)", r.Method, r.URL.Path, http.StatusOK)
		return
	}
	log.Printf("DEBUG: Twirp CreateCacheEntry: No existing cache found. Proceeding with reservation.")

	uploadId, err := newUploadId()
	if err != nil {
		log.Printf("ERROR: Twirp CreateCacheEntry: Failed to generate upload ID: %v", err)
		writeTwirpError(w, "internal", "Internal server error: failed to generate upload ID", http.StatusInternalServerError)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusInternalServerError)
		return
	}
	log.Printf("DEBUG: Twirp CreateCacheEntry: Generated internal UploadId: %s", uploadId)

	compressionMethod := "gzip"
	clientCompression := r.Header.Get("X-Actions-Cache-Compression-Method")
	if clientCompression == "zstd" {
		compressionMethod = "zstd"
		log.Printf("DEBUG: Twirp CreateCacheEntry: Detected client requesting zstd compression via header.")
	} else if clientCompression != "" && clientCompression != "gzip" {
		log.Printf("WARN: Twirp CreateCacheEntry: Received unsupported compression: %s. Defaulting to gzip.", clientCompression)
	} else {
		log.Printf("DEBUG: Twirp CreateCacheEntry: Using default gzip compression.")
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
		Size:        -1, // Size unknown at this point
		Compression: compressionMethod,
	}
	s.reserves[uploadId] = record
	// --- FIX: Assign string uploadId, not int64 cacheId ---
	s.cacheIdToUploadId[cacheId] = uploadId // Map numeric ID to the hex UploadId string
	// --- End FIX ---
	s.mu.Unlock()
	log.Printf("DEBUG: Twirp CreateCacheEntry: Released lock after recording reservation")
	log.Printf("INFO: Twirp CreateCacheEntry: Reserved cache: CacheId=%d (Internal UploadId=%s), Key='%s', Version='%s', Compression='%s', Token=%s...",
		cacheId, uploadId, req.Key, req.Version, compressionMethod, truncateToken(token))

	// --- Build Twirp Response using FIXED base URL ---
	base, _ := url.Parse(fixedBaseURL)
	relativeUploadPath := fmt.Sprintf("/artifactcache/%d", cacheId) // Points to the PATCH endpoint
	uploadURL := base.JoinPath(relativeUploadPath)
	urls := []string{uploadURL.String()} // Single URL for PATCH

	// Use the REST response structure as a HACK for @actions/cache compatibility
	response := reserveResponse{Result: reserveResult{
		UploadId:      uploadId,
		PresignedUrls: urls,
		ChunkSize:     defaultChunkSize,
	}}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // Use 200 OK for reservation response
	json.NewEncoder(w).Encode(response)
	log.Printf("DEBUG: Twirp CreateCacheEntry: Responded with REST-like structure (UploadId: %s, URL: %s)", uploadId, uploadURL.String())
	log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusOK)
}

// --- Handlers for Actions Artifact Cache API (/artifactcache/) ---

// handleArtifactCache routes PATCH and POST requests for /artifactcache/{cacheId}/*
func (s *server) handleArtifactCache(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(strings.TrimPrefix(r.URL.Path, "/artifactcache/"), "/")
	if len(pathParts) < 1 || pathParts[0] == "" {
		log.Printf("WARN: handleArtifactCache: Invalid path format: %s", r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorResponse{Message: "Invalid request path, expected /artifactcache/{cacheId}/..."})
		return
	}

	cacheIdStr := pathParts[0]
	cacheId, err := strconv.ParseInt(cacheIdStr, 10, 64)
	if err != nil {
		log.Printf("WARN: handleArtifactCache: Invalid cacheId '%s' in path: %v", cacheIdStr, err)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorResponse{Message: "Invalid cacheId in request path"})
		return
	}

	if r.Method == "PATCH" && len(pathParts) == 1 {
		s.handleArtifactUploadChunk(w, r, cacheId)
	} else if r.Method == "POST" && len(pathParts) == 2 && pathParts[1] == "commit" {
		s.handleArtifactCommit(w, r, cacheId)
	} else {
		log.Printf("WARN: handleArtifactCache: Unsupported method/path combination: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}
}

// handleArtifactUploadChunk handles PATCH /artifactcache/{cacheId} for uploading chunks
func (s *server) handleArtifactUploadChunk(w http.ResponseWriter, r *http.Request, cacheId int64) {
	log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
	token, ok := s.requireAuth(w, r)
	if !ok {
		log.Printf("DEBUG: handleArtifactUploadChunk: Authentication failed.")
		return
	}

	s.mu.Lock()
	uploadId, idFound := s.cacheIdToUploadId[cacheId]
	reserve, reserveFound := s.reserves[uploadId] // Check reserve using uploadId *after* finding it
	tokenMatches := idFound && reserveFound && reserve.Token == token
	s.mu.Unlock() // Unlock after reading state needed for checks

	if !idFound || !reserveFound {
		log.Printf("WARN: handleArtifactUploadChunk: Upload session not found or reserve missing for CacheId %d (UploadId found: %t, Reserve found: %t)", cacheId, idFound, reserveFound)
		writeTwirpError(w, "not_found", "Upload session not found or already completed", http.StatusNotFound)
		return
	}
	if !tokenMatches {
		log.Printf("WARN: handleArtifactUploadChunk: Token mismatch for CacheId %d (UploadId %s). Request token %s... != Reserved token %s...",
			cacheId, uploadId, truncateToken(token), truncateToken(reserve.Token))
		writeTwirpError(w, "permission_denied", "Forbidden: Token does not match reservation owner", http.StatusForbidden)
		return
	}

	// Proceed with upload
	log.Printf("DEBUG: handleArtifactUploadChunk: Handling chunk for CacheId=%d (UploadId=%s)", cacheId, uploadId)
	partsDir := filepath.Join(s.storageDir, uploadId) // Use uploadId for parts directory
	if err := os.MkdirAll(partsDir, 0755); err != nil {
		log.Printf("ERROR: handleArtifactUploadChunk: Failed to create directory %s: %v", partsDir, err)
		writeTwirpError(w, "internal", "Internal server error: failed to prepare storage", http.StatusInternalServerError)
		return
	}
	partPath := filepath.Join(partsDir, "data.part") // Append all chunks to one file
	f, err := os.OpenFile(partPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("ERROR: handleArtifactUploadChunk: Failed to open/create part file %s for append: %v", partPath, err)
		writeTwirpError(w, "internal", "Internal server error: failed to open part file", http.StatusInternalServerError)
		return
	}
	defer f.Close()

	log.Printf("DEBUG: handleArtifactUploadChunk: Appending request body to %s...", partPath)
	startTime := time.Now()
	contentRange := r.Header.Get("Content-Range")
	log.Printf("DEBUG: handleArtifactUploadChunk: Content-Range: %s", contentRange)

	bytesWritten, err := io.Copy(f, r.Body)
	duration := time.Since(startTime)

	if err != nil {
		log.Printf("ERROR: handleArtifactUploadChunk: Failed to copy request body to %s: %v", partPath, err)
		writeTwirpError(w, "internal", "Internal server error: failed to write part file", http.StatusInternalServerError)
		return
	}
	log.Printf("INFO: handleArtifactUploadChunk: Appended %d bytes to %s in %v (CacheId=%d)", bytesWritten, partPath, duration, cacheId)
	w.WriteHeader(http.StatusOK)
	log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusOK)
}

// artifactCommitRequest defines JSON body for the artifact commit API.
type artifactCommitRequest struct {
	Size int64 `json:"size"`
}

// handleArtifactCommit handles POST /artifactcache/{cacheId}/commit to finalize the cache
func (s *server) handleArtifactCommit(w http.ResponseWriter, r *http.Request, cacheId int64) {
	log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
	token, ok := s.requireAuth(w, r)
	if !ok {
		log.Printf("DEBUG: handleArtifactCommit: Authentication failed.")
		return
	}

	var req artifactCommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("WARN: handleArtifactCommit: Failed to decode commit request body for CacheId %d: %v", cacheId, err)
		writeTwirpError(w, "invalid_argument", "Invalid commit request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Size <= 0 {
		log.Printf("WARN: handleArtifactCommit: Invalid size %d in commit request for CacheId %d", req.Size, cacheId)
		writeTwirpError(w, "invalid_argument", "Invalid size in commit request", http.StatusBadRequest)
		return
	}
	log.Printf("DEBUG: handleArtifactCommit: Decoded commit request for CacheId=%d: Size=%d", cacheId, req.Size)

	// Reservation Lookup & Ownership Check
	s.mu.Lock() // Lock needed for consistent read/update/delete
	uploadId, idFound := s.cacheIdToUploadId[cacheId]
	if !idFound {
		s.mu.Unlock()
		log.Printf("WARN: handleArtifactCommit: No upload session found mapping for CacheId %d", cacheId)
		writeTwirpError(w, "not_found", "Upload session not found", http.StatusNotFound)
		return
	}

	reserve, reserveFound := s.reserves[uploadId]
	if !reserveFound {
		// Check if already committed before declaring reserve missing
		_, alreadyCommitted := s.byUpload[uploadId]
		s.mu.Unlock() // Unlock before potentially returning
		if alreadyCommitted {
			log.Printf("INFO: handleArtifactCommit: UploadId %s (CacheId %d) was already committed. Responding OK.", uploadId, cacheId)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]int64{"cacheId": cacheId})
			log.Printf("DEBUG: << Response END: %s %s Status: %d (Already Committed)", r.Method, r.URL.Path, http.StatusOK)
			return
		}
		log.Printf("WARN: handleArtifactCommit: Found mapping for CacheId %d to UploadId %s, but reservation record is missing and not committed.", cacheId, uploadId)
		writeTwirpError(w, "not_found", "Upload session not found or expired", http.StatusNotFound)
		return
	}

	if reserve.Token != token {
		s.mu.Unlock()
		log.Printf("WARN: handleArtifactCommit: Token mismatch for CacheId %d (UploadId %s). Request token %s... != Reserved token %s...",
			cacheId, uploadId, truncateToken(token), truncateToken(reserve.Token))
		writeTwirpError(w, "permission_denied", "Forbidden: Token does not match reservation owner", http.StatusForbidden)
		return
	}

	user, userFound := s.tokens[token]
	if !userFound {
		s.mu.Unlock()
		log.Printf("ERROR: handleArtifactCommit: Token %s... passed auth but not found!", truncateToken(token))
		writeTwirpError(w, "internal", "Internal server error: user token inconsistency", http.StatusInternalServerError)
		return
	}
	log.Printf("DEBUG: handleArtifactCommit: Token ownership verified for CacheId %d (UploadId %s)", cacheId, uploadId)

	// --- Quota check based on committed size ---
	log.Printf("DEBUG: handleArtifactCommit: Checking final quota for token %s... (Quota: %d B, Used: %d B, Commit Size: %d B)", truncateToken(token), user.Quota, user.Used, req.Size)
	if req.Size > user.Quota || user.Used+req.Size > user.Quota {
		exceededType := "per-file"
		if req.Size <= user.Quota {
			exceededType = "cumulative"
		}
		msg := fmt.Sprintf("Quota exceeded (%s): %d B requested, %d B used, %d B quota",
			exceededType, req.Size, user.Used, user.Quota)
		log.Printf("WARN: handleArtifactCommit: Quota exceeded (%s) for token %s...", exceededType, truncateToken(token))

		// Start cleanup *before* unlocking and sending error response
		partsDir := filepath.Join(s.storageDir, uploadId)
		cleanupErr := os.RemoveAll(partsDir)

		// Remove reservation record (still holding lock)
		delete(s.reserves, uploadId)
		delete(s.cacheIdToUploadId, cacheId)
		s.mu.Unlock() // Unlock *after* state cleanup

		// Log cleanup result
		if cleanupErr == nil {
			log.Printf("DEBUG: handleArtifactCommit: Cleaned up upload directory %s due to quota failure.", partsDir)
		} else {
			log.Printf("WARN: handleArtifactCommit: Failed to cleanup upload directory %s after quota failure: %v", partsDir, cleanupErr)
		}
		// Send error response
		writeTwirpError(w, "resource_exhausted", msg, http.StatusBadRequest)
		return
	}
	log.Printf("DEBUG: handleArtifactCommit: Final quota check passed for token %s...", truncateToken(token))

	// --- Finalize the archive (Rename temp part file) --- (Still holding lock)
	partsDir := filepath.Join(s.storageDir, uploadId)
	tempPartFile := filepath.Join(partsDir, "data.part") // This variable *is* used now

	fileExtension := ".tar.gz"
	if reserve.Compression == "zstd" {
		fileExtension = ".tar.zst"
		log.Printf("DEBUG: handleArtifactCommit: Using .tar.zst extension based on reserved compression method.")
	} else {
		log.Printf("DEBUG: handleArtifactCommit: Using default .tar.gz extension.")
	}
	finalFilename := fmt.Sprintf("%s-%s-%s%s", sanitizeFilename(reserve.Key), sanitizeFilename(reserve.Version), uploadId, fileExtension)
	finalPath := filepath.Join(s.storageDir, finalFilename)
	log.Printf("DEBUG: handleArtifactCommit: Finalizing archive for CacheId=%d. Renaming %s to %s", cacheId, tempPartFile, finalPath)

	if err := os.Rename(tempPartFile, finalPath); err != nil {
		s.mu.Unlock()
		log.Printf("ERROR: handleArtifactCommit: Failed to rename temp part file %s to %s: %v", tempPartFile, finalPath, err)
		_ = os.Remove(tempPartFile) // Attempt cleanup
		writeTwirpError(w, "internal", "Internal server error: failed to finalize archive file", http.StatusInternalServerError)
		return
	}

	// Verify final size
	fileInfo, err := os.Stat(finalPath)
	actualSize := int64(0)
	if err != nil {
		s.mu.Unlock()
		log.Printf("ERROR: handleArtifactCommit: Failed to stat final archive file %s after rename: %v", finalPath, err)
		_ = os.Remove(finalPath) // Attempt cleanup
		writeTwirpError(w, "internal", "Internal server error: failed to verify archive file size", http.StatusInternalServerError)
		return
	}
	actualSize = fileInfo.Size()
	if actualSize != req.Size {
		log.Printf("CRITICAL WARN: handleArtifactCommit: Final archive size %d bytes DOES NOT MATCH committed size %d bytes for CacheId %d (UploadId %s). Using committed size for metadata.", actualSize, req.Size, cacheId, uploadId)
	} else {
		log.Printf("DEBUG: handleArtifactCommit: Verified final archive size %d matches committed size.", actualSize)
	}

	log.Printf("INFO: handleArtifactCommit: Successfully finalized archive %s (Reported Size: %d B) for CacheId %d", finalPath, req.Size, cacheId)

	// --- Update Cache State --- (Still holding lock)
	base, _ := url.Parse(fixedBaseURL)
	relativeDownloadPath := fmt.Sprintf("/download/%s", uploadId)
	downloadURL := base.JoinPath(relativeDownloadPath)

	entry := &cacheEntry{
		Key:             reserve.Key,
		Version:         reserve.Version,
		RunId:           reserve.RunId,
		CacheId:         reserve.CacheId,
		ArchivePath:     finalPath,
		ArchiveLocation: downloadURL.String(), // Store the *full* URL
		CreationTime:    time.Now().UTC(),
		Compression:     reserve.Compression,
		Size:            req.Size, // Use client-reported size
	}
	compositeKey := reserve.Key + "|" + reserve.Version

	s.caches[compositeKey] = entry
	s.byUpload[uploadId] = entry // Map uploadId -> entry for downloads
	user.Used += req.Size        // Update quota usage
	delete(s.reserves, uploadId) // Remove reservation record
	delete(s.cacheIdToUploadId, cacheId) // Remove mapping

	log.Printf("DEBUG: handleArtifactCommit: Updated token %s... usage. New Used: %d B", truncateToken(token), user.Used)
	s.mu.Unlock() // RELEASE LOCK after all state updates
	log.Printf("DEBUG: handleArtifactCommit: Released lock after committing state")

	log.Printf("INFO: handleArtifactCommit: Committed cache: CacheId=%d, Key='%s', Version='%s', Size=%d, Path=%s, Compression='%s'",
		entry.CacheId, entry.Key, entry.Version, req.Size, entry.ArchivePath, entry.Compression)

	// --- Cleanup Upload Parts Directory ---
	log.Printf("DEBUG: handleArtifactCommit: Attempting to remove parts directory: %s", partsDir)
	if err := os.RemoveAll(partsDir); err != nil {
		log.Printf("WARN: handleArtifactCommit: Failed to remove parts directory %s after commit: %v", partsDir, err)
	} else {
		log.Printf("DEBUG: handleArtifactCommit: Successfully removed parts directory %s", partsDir)
	}

	// --- Respond OK with cacheId ---
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]int64{"cacheId": cacheId}) // Respond with numeric cacheId
	log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusOK)
}

// handleGetCache implements GET /cache?keys=...&version=... (REST cache lookup)
func (s *server) handleGetCache(w http.ResponseWriter, r *http.Request) {
	log.Printf("DEBUG: >> Request START: %s %s (Remote: %s)", r.Method, r.URL.Path, r.RemoteAddr)
	if r.Method != "GET" {
		log.Printf("WARN: handleGetCache: Method %s not allowed, expected GET for %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Printf("DEBUG: << Response END: %s %s Status: %d", r.Method, r.URL.Path, http.StatusMethodNotAllowed)
		return
	}

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
	defer s.mu.Unlock()
	log.Printf("DEBUG: handleGetCache: Checking %d potential keys against %d cache entries", len(keys), len(s.caches))

	for _, k := range keys {
		trimmedKey := strings.TrimSpace(k)
		if trimmedKey == "" { continue }
		comp := trimmedKey + "|" + version
		log.Printf("DEBUG: handleGetCache: Checking composite key: %s", comp)
		if e, found := s.caches[comp]; found {
			log.Printf("INFO: handleGetCache: Cache HIT for Key='%s', Version='%s'. Composite: %s. CacheId: %d, Location: %s",
				trimmedKey, version, comp, e.CacheId, e.ArchiveLocation)
			resp := struct {
				CacheId         int64  `json:"cacheId"`
				ArchiveLocation string `json:"archiveLocation"` // This now contains the full URL
				CacheKey        string `json:"cacheKey"`
				CacheVersion    string `json:"cacheVersion"`
				Scope           string `json:"scope"`
				CreationTime    string `json:"creationTime"`
				Compression     string `json:"compression"`
				Size            int64  `json:"size,omitempty"`
			}{
				CacheId:         e.CacheId,
				ArchiveLocation: e.ArchiveLocation, // Return the full URL stored in the entry
				CacheKey:        e.Key,
				CacheVersion:    e.Version,
				Scope:           "",
				CreationTime:    e.CreationTime.Format(time.RFC3339),
				Compression:     e.Compression,
				Size:            e.Size,
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			log.Printf("DEBUG: handleGetCache: Responded with cache hit details (Full URL: %s).", e.ArchiveLocation)
			log.Printf("DEBUG: << Response END: %s %s Status: %d (Cache Hit)", r.Method, r.URL.Path, http.StatusOK)
			return
		} else {
			log.Printf("DEBUG: handleGetCache: Cache MISS for composite key: %s", comp)
		}
	}

	log.Printf("INFO: handleGetCache: Cache MISS for all requested Keys=%v, Version=%s", keys, version)
	w.WriteHeader(http.StatusNoContent) // 204 No Content indicates cache miss
	log.Printf("DEBUG: << Response END: %s %s Status: %d (Cache Miss)", r.Method, r.URL.Path, http.StatusNoContent)
}