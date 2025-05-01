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
}

func main() {
   // initialize server; tokens can be created via /token endpoint
   port := os.Getenv("PORT")
   if port == "" {
       port = "8080"
   }
   storageDir := filepath.Join(os.TempDir(), "tenki-cloud-cache")
   if err := os.MkdirAll(storageDir, 0755); err != nil {
       log.Fatalf("failed to create storage dir: %v", err)
   }
   srv := &server{
       storageDir:  storageDir,
       nextCacheID: 1,
       reserves:    make(map[string]*reserveRecord),
       caches:      make(map[string]*cacheEntry),
       byUpload:    make(map[string]*cacheEntry),
       tokens:      make(map[string]*tokenInfo),
   }
   // token creation endpoint (no auth)
   http.HandleFunc("/token", srv.handleTokenCreation)
   http.HandleFunc("/cache", srv.handleGetCache)
   http.HandleFunc("/caches/commit", srv.handleCommit)
   http.HandleFunc("/caches", srv.handleCaches)
   http.HandleFunc("/upload/", srv.handleUpload)
   http.HandleFunc("/download/", srv.handleDownload)
   // serve Swagger spec and UI
   http.HandleFunc("/swagger.json", func(w http.ResponseWriter, r *http.Request) {
       w.Header().Set("Content-Type", "application/json")
       w.Write(swaggerSpec)
   })
   http.HandleFunc("/docs", func(w http.ResponseWriter, r *http.Request) {
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
   })
   http.HandleFunc("/docs/", func(w http.ResponseWriter, r *http.Request) {
       // redirect /docs/ to /docs
       http.Redirect(w, r, "/docs", http.StatusMovedPermanently)
   })
   log.Printf("TenkiCloud cache server listening on :%s", port)
   log.Fatal(http.ListenAndServe(":"+port, nil))
}

// reserveRecord holds metadata from reserve phase, including which user token reserved it.
type reserveRecord struct {
   Key     string
   Version string
   RunId   string
   CacheId int64
   Token   string
}

// server holds in-memory storage and state.
type server struct {
   storageDir  string
   mu          sync.Mutex
   nextCacheID int64
   reserves    map[string]*reserveRecord
   caches      map[string]*cacheEntry
   byUpload    map[string]*cacheEntry
   tokens      map[string]*tokenInfo
}

// requireAuth validates the Bearer token (skipped for downloads).
// Returns the token string and whether auth succeeded.
func (s *server) requireAuth(w http.ResponseWriter, r *http.Request) (string, bool) {
   if strings.HasPrefix(r.URL.Path, "/download/") {
       return "", true
   }
   auth := r.Header.Get("Authorization")
   if !strings.HasPrefix(auth, "Bearer ") {
       w.WriteHeader(http.StatusUnauthorized)
       json.NewEncoder(w).Encode(errorResponse{Message: "Unauthorized"})
       return "", false
   }
   token := strings.TrimPrefix(auth, "Bearer ")
   s.mu.Lock()
   _, ok := s.tokens[token]
   s.mu.Unlock()
   if !ok {
       w.WriteHeader(http.StatusUnauthorized)
       json.NewEncoder(w).Encode(errorResponse{Message: "Unauthorized"})
       return "", false
   }
   return token, true
}

// baseURL constructs the request URL origin.
func baseURL(r *http.Request) string {
   scheme := "http"
   if r.TLS != nil {
       scheme = "https"
   }
   return fmt.Sprintf("%s://%s", scheme, r.Host)
}

// handleCaches supports POST (reserve) and GET (listing).
func (s *server) handleCaches(w http.ResponseWriter, r *http.Request) {
   // authenticate request
   token, ok := s.requireAuth(w, r)
   if !ok {
       return
   }
   switch r.Method {
   case "GET":
       key := r.URL.Query().Get("key")
       var list []artifactCache
       s.mu.Lock()
       for _, e := range s.caches {
           if e.Key == key {
               list = append(list, artifactCache{
                   CacheKey:     e.Key,
                   CacheVersion: e.Version,
                   Scope:        "",
                   CreationTime: e.CreationTime.Format(time.RFC3339),
               })
           }
       }
       s.mu.Unlock()
       w.Header().Set("Content-Type", "application/json")
       json.NewEncoder(w).Encode(listResponse{TotalCount: len(list), ArtifactCaches: list})
   case "POST":
       var req reserveRequest
       if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
           w.WriteHeader(http.StatusBadRequest)
           json.NewEncoder(w).Encode(errorResponse{Message: "invalid request"})
           return
       }
       // enforce per-user quota
       s.mu.Lock()
       user := s.tokens[token]
       s.mu.Unlock()
       // per-file limit
       if req.CacheSize > user.Quota {
           w.WriteHeader(http.StatusBadRequest)
           msg := fmt.Sprintf(
               "Cache size of ~%d MB (%d B) exceeds user quota of ~%d MB, not saving cache.",
               req.CacheSize/(1024*1024), req.CacheSize, user.Quota/(1024*1024))
           json.NewEncoder(w).Encode(errorResponse{Message: msg})
           return
       }
       // cumulative usage limit
       if user.Used+req.CacheSize > user.Quota {
           w.WriteHeader(http.StatusBadRequest)
           msg := fmt.Sprintf(
               "Quota exceeded: current usage ~%d MB + requested ~%d MB > quota ~%d MB.",
               user.Used/(1024*1024), req.CacheSize/(1024*1024), user.Quota/(1024*1024))
           json.NewEncoder(w).Encode(errorResponse{Message: msg})
           return
       }
       // generate upload ID
       uploadId, err := newUploadId()
       if err != nil {
           w.WriteHeader(http.StatusInternalServerError)
           return
       }
       s.mu.Lock()
       cacheId := s.nextCacheID
       s.nextCacheID++
       // record reserve with user token
       s.reserves[uploadId] = &reserveRecord{
           Key:     req.Key,
           Version: req.Version,
           RunId:   req.RunId,
           CacheId: cacheId,
           Token:   token,
       }
       s.mu.Unlock()
       // presign URLs
       parts := int((req.CacheSize + defaultChunkSize - 1) / defaultChunkSize)
       urls := make([]string, parts)
       origin := baseURL(r)
       for i := 0; i < parts; i++ {
           urls[i] = fmt.Sprintf("%s/upload/%s/%d", origin, uploadId, i)
       }
       w.Header().Set("Content-Type", "application/json")
       json.NewEncoder(w).Encode(reserveResponse{Result: reserveResult{
           UploadId:      uploadId,
           PresignedUrls: urls,
           ChunkSize:     defaultChunkSize,
       }})
   default:
       w.WriteHeader(http.StatusMethodNotAllowed)
   }
}

// handleUpload accepts PUTs to /upload/{uploadId}/{partIndex}.
func (s *server) handleUpload(w http.ResponseWriter, r *http.Request) {
   if r.Method != "PUT" {
       w.WriteHeader(http.StatusMethodNotAllowed)
       return
   }
   // authenticate request
   token, ok := s.requireAuth(w, r)
   if !ok {
       return
   }
   parts := strings.Split(r.URL.Path, "/")
   if len(parts) != 4 {
       w.WriteHeader(http.StatusNotFound)
       return
   }
   uploadId := parts[2]
   idx, err := strconv.Atoi(parts[3])
   if err != nil {
       w.WriteHeader(http.StatusBadRequest)
       return
   }
   // verify reserve exists and belongs to this token
   s.mu.Lock()
   reserve, ok := s.reserves[uploadId]
   s.mu.Unlock()
   if !ok {
       w.WriteHeader(http.StatusNotFound)
       return
   }
   if reserve.Token != token {
       w.WriteHeader(http.StatusForbidden)
       return
   }
   dir := filepath.Join(s.storageDir, uploadId)
   os.MkdirAll(dir, 0755)
   partPath := filepath.Join(dir, fmt.Sprintf("%d.part", idx))
   f, err := os.Create(partPath)
   if err != nil {
       w.WriteHeader(http.StatusInternalServerError)
       return
   }
   defer f.Close()
   hash := md5.New()
   writer := io.MultiWriter(f, hash)
   if _, err := io.Copy(writer, r.Body); err != nil {
       w.WriteHeader(http.StatusInternalServerError)
       return
   }
   etag := hex.EncodeToString(hash.Sum(nil))
   w.Header().Set("ETag", etag)
   w.WriteHeader(http.StatusOK)
}

// handleCommit finalizes upload and assembles the archive.
func (s *server) handleCommit(w http.ResponseWriter, r *http.Request) {
   if r.Method != "POST" {
       w.WriteHeader(http.StatusMethodNotAllowed)
       return
   }
   // authenticate request
   token, ok := s.requireAuth(w, r)
   if !ok {
       return
   }
   var req commitRequest
   if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
       w.WriteHeader(http.StatusBadRequest)
       json.NewEncoder(w).Encode(errorResponse{Message: "invalid request"})
       return
   }
   // enforce per-user quota
   s.mu.Lock()
   user := s.tokens[token]
   s.mu.Unlock()
   if req.Size > user.Quota {
       w.WriteHeader(http.StatusBadRequest)
       msg := fmt.Sprintf(
           "Cache size of ~%d MB (%d B) exceeds user quota of ~%d MB, not saving cache.",
           req.Size/(1024*1024), req.Size, user.Quota/(1024*1024))
       json.NewEncoder(w).Encode(errorResponse{Message: msg})
       return
   }
   if user.Used+req.Size > user.Quota {
       w.WriteHeader(http.StatusBadRequest)
       msg := fmt.Sprintf(
           "Quota exceeded: current usage ~%d MB + requested ~%d MB > quota ~%d MB.",
           user.Used/(1024*1024), req.Size/(1024*1024), user.Quota/(1024*1024))
       json.NewEncoder(w).Encode(errorResponse{Message: msg})
       return
   }
   // locate reserve and check ownership
   s.mu.Lock()
   reserve, ok := s.reserves[req.UploadId]
   s.mu.Unlock()
   if !ok {
       w.WriteHeader(http.StatusNotFound)
       json.NewEncoder(w).Encode(errorResponse{Message: "uploadId not found"})
       return
   }
   if reserve.Token != token {
       w.WriteHeader(http.StatusForbidden)
       json.NewEncoder(w).Encode(errorResponse{Message: "uploadId not authorized for this token"})
       return
   }
   parts := len(req.Etags)
   dir := filepath.Join(s.storageDir, req.UploadId)
   finalPath := filepath.Join(s.storageDir, req.UploadId+".tar.gz")
   out, err := os.Create(finalPath)
   if err != nil {
       w.WriteHeader(http.StatusInternalServerError)
       return
   }
   defer out.Close()
   for i := 0; i < parts; i++ {
       partFile := filepath.Join(dir, fmt.Sprintf("%d.part", i))
       in, err := os.Open(partFile)
       if err != nil {
           w.WriteHeader(http.StatusInternalServerError)
           return
       }
       io.Copy(out, in)
       in.Close()
   }
   origin := baseURL(r)
   entry := &cacheEntry{
       Key:             reserve.Key,
       Version:         reserve.Version,
       RunId:           reserve.RunId,
       CacheId:         reserve.CacheId,
       ArchivePath:     finalPath,
       ArchiveLocation: fmt.Sprintf("%s/download/%s", origin, req.UploadId),
       CreationTime:    time.Now().UTC(),
   }
   composite := reserve.Key + "|" + reserve.Version
   // commit cache entry and update usage
   s.mu.Lock()
   s.caches[composite] = entry
   s.byUpload[req.UploadId] = entry
   delete(s.reserves, req.UploadId)
   // track usage for this token
   user = s.tokens[token]
   user.Used += req.Size
   s.mu.Unlock()
   w.Header().Set("Content-Type", "application/json")
   json.NewEncoder(w).Encode(struct{ CacheId int64 `json:"cacheId"`}{entry.CacheId})
}

// handleGetCache implements GET /cache?keys=...&version=...
func (s *server) handleGetCache(w http.ResponseWriter, r *http.Request) {
   if r.Method != "GET" {
       w.WriteHeader(http.StatusMethodNotAllowed)
       return
   }
   // authenticate request
   if _, ok := s.requireAuth(w, r); !ok {
       return
   }
   keysParam := r.URL.Query().Get("keys")
   version := r.URL.Query().Get("version")
   keys := strings.Split(keysParam, ",")
   s.mu.Lock()
   defer s.mu.Unlock()
   for _, k := range keys {
       comp := k + "|" + version
       if e, ok := s.caches[comp]; ok {
           resp := struct {
               CacheId         int64  `json:"cacheId"`
               ArchiveLocation string `json:"archiveLocation"`
               CacheKey        string `json:"cacheKey"`
               CacheVersion    string `json:"cacheVersion"`
               Scope           string `json:"scope"`
               CreationTime    string `json:"creationTime"`
           }{
               CacheId:         e.CacheId,
               ArchiveLocation: e.ArchiveLocation,
               CacheKey:        e.Key,
               CacheVersion:    e.Version,
               Scope:           "",
               CreationTime:    e.CreationTime.Format(time.RFC3339),
           }
           w.Header().Set("Content-Type", "application/json")
           json.NewEncoder(w).Encode(resp)
           return
       }
   }
   w.WriteHeader(http.StatusNoContent)
}

// handleDownload serves the combined archive at /download/{uploadId}.
func (s *server) handleDownload(w http.ResponseWriter, r *http.Request) {
   if r.Method != "GET" {
       w.WriteHeader(http.StatusMethodNotAllowed)
       return
   }
   parts := strings.Split(r.URL.Path, "/")
   if len(parts) != 3 {
       w.WriteHeader(http.StatusNotFound)
       return
   }
   uploadId := parts[2]
   s.mu.Lock()
   entry, ok := s.byUpload[uploadId]
   s.mu.Unlock()
   if !ok {
       w.WriteHeader(http.StatusNotFound)
       return
   }
   http.ServeFile(w, r, entry.ArchivePath)
}
// handleTokenCreation generates a new token with an optional quota (bytes) in JSON body {"quotaGB":<int>}.
// This endpoint is unauthenticated.
func (s *server) handleTokenCreation(w http.ResponseWriter, r *http.Request) {
   if r.Method != "POST" {
       w.WriteHeader(http.StatusMethodNotAllowed)
       return
   }
   // parse optional quota in GB
   var req struct{ QuotaGB int64 `json:"quotaGB"` }
   _ = json.NewDecoder(r.Body).Decode(&req)
   // initialize quota to default (int64)
   var quota int64 = defaultQuota
   if req.QuotaGB > 0 {
       quota = req.QuotaGB * 1024 * 1024 * 1024
   }
   // generate token string
   tokenID, err := newUploadId()
   if err != nil {
       w.WriteHeader(http.StatusInternalServerError)
       return
   }
   // store token info
   s.mu.Lock()
   s.tokens[tokenID] = &tokenInfo{Quota: quota, Used: 0}
   s.mu.Unlock()
   // return token and quota
   w.Header().Set("Content-Type", "application/json")
   json.NewEncoder(w).Encode(struct {
       Token string `json:"token"`
       Quota int64  `json:"quota"`
   }{Token: tokenID, Quota: quota})
}

//go:embed swagger.json
var swaggerSpec []byte

// handle docs and spec is registered in main()
// newUploadId generates a random 16-byte hex string.
func newUploadId() (string, error) {
   b := make([]byte, 16)
   if _, err := rand.Read(b); err != nil {
       return "", err
   }
   return hex.EncodeToString(b), nil
}