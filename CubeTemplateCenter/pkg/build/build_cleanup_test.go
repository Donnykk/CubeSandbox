// SPDX-License-Identifier: Apache-2.0

package build

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/db/models"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/log"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/templatecenter"
	"github.com/tencentcloud/CubeSandbox/CubeTemplateCenter/pkg/image"
	"github.com/tencentcloud/CubeSandbox/CubeTemplateCenter/pkg/s3store"
	"github.com/tencentcloud/CubeSandbox/CubeTemplateCenter/pkg/tcconfig"
)

func TestBuildUploadCleanup(t *testing.T) {
	for _, tt := range []struct {
		name         string
		backend      string
		uploadFails  bool
		reportFails  bool
		cleanupFails bool
		reuse        bool
	}{
		{name: "s3-success", backend: "s3"},
		{name: "s3-upload-failure", backend: "s3", uploadFails: true},
		{name: "s3-callback-failure", backend: "s3", reportFails: true},
		{name: "s3-cleanup-failure", backend: "s3", cleanupFails: true},
		{name: "local-only"},
		{name: "fs-store", backend: "fs"},
		{name: "reuse-s3", backend: "s3", reuse: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			const artifactID = "rfs-build-cleanup"
			root := t.TempDir()
			t.Setenv("CUBEMASTER_ROOTFS_ARTIFACT_DIR", filepath.Join(root, "work"))
			t.Setenv("CUBEMASTER_ROOTFS_ARTIFACT_STORE_DIR", filepath.Join(root, "configured"))
			// Deliberately differ from the configured root, as a fallback build can.
			storeDir := filepath.Join(root, "actual", artifactID)
			workDir := filepath.Join(image.ArtifactWorkRootDir(), artifactID)
			for _, dir := range []string{storeDir, workDir} {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			data := []byte("built rootfs")
			result := image.BuildResult{
				Ext4Path:  filepath.Join(storeDir, artifactID+".ext4"),
				SHA256:    fmt.Sprintf("%x", sha256.Sum256(data)),
				SizeBytes: int64(len(data)),
			}
			if err := os.WriteFile(result.Ext4Path, data, 0o644); err != nil {
				t.Fatal(err)
			}
			neighbor := filepath.Join(filepath.Dir(storeDir), "rfs-other.ext4")
			if err := os.WriteFile(neighbor, data, 0o644); err != nil {
				t.Fatal(err)
			}

			patches := gomonkey.ApplyFunc(image.BuildExt4, func(context.Context, *image.PreparedSource, image.BuildOptions) (image.BuildResult, error) {
				return result, nil
			})
			defer patches.Reset()
			if tt.cleanupFails {
				patches.ApplyFuncReturn(cleanupBuildOutput, os.ErrPermission)
			}

			var mu sync.Mutex
			objectExists := tt.reuse
			puts := 0
			s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.URL.Path != "/artifacts/templates/"+artifactID+".ext4" {
					t.Errorf("unexpected object path: %s", r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				switch r.Method {
				case http.MethodPut:
					puts++
					if _, err := os.Stat(result.Ext4Path); err != nil {
						t.Errorf("local file removed before upload completed: %v", err)
					}
					_, _ = io.Copy(io.Discard, r.Body)
					if tt.uploadFails {
						w.WriteHeader(http.StatusForbidden)
						_, _ = io.WriteString(w, "<Error><Code>AccessDenied</Code><Message>upload denied</Message></Error>")
						return
					}
					objectExists = true
					w.Header().Set("ETag", `"test-etag"`)
				case http.MethodHead:
					if !objectExists {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					w.Header().Set("Content-Length", fmt.Sprint(len(data)))
					w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
					w.Header().Set("ETag", `"test-etag"`)
				default:
					t.Errorf("unexpected object operation: %s", r.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer s3.Close()

			var client *s3store.Client
			fsRoot := filepath.Join(root, "objects")
			if tt.backend != "" {
				var err error
				client, err = s3store.NewClient(s3store.Config{
					Driver: tt.backend, Endpoint: s3.URL, Bucket: "artifacts",
					AccessKey: "test-key", SecretKey: "test-secret", UsePathStyle: true,
					ArtifactPrefix: "templates", FSRoot: fsRoot,
				})
				if err != nil {
					t.Fatal(err)
				}
			}

			wantRemoved := tt.backend == "s3" && !tt.reuse && !tt.cleanupFails
			var reports []map[string]any
			master := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload map[string]any
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Errorf("decode callback: %v", err)
				}
				mu.Lock()
				reports = append(reports, payload)
				mu.Unlock()
				if status := payload["status"]; status == "BUILT" || status == "FAILED" {
					for _, path := range []string{storeDir, workDir} {
						_, err := os.Stat(path)
						if wantRemoved && !os.IsNotExist(err) {
							t.Errorf("directory still present at terminal callback: %s (%v)", path, err)
						} else if !wantRemoved && err != nil {
							t.Errorf("retained directory missing: %s (%v)", path, err)
						}
					}
				}
				if tt.reportFails && payload["status"] == "BUILT" {
					w.WriteHeader(http.StatusServiceUnavailable)
				}
			}))
			defer master.Close()
			reporter := newTestReporter(t, master)
			source := &image.PreparedSource{Digest: "sha256:source", MasterNodeIP: master.URL, ConfigJSON: "{}"}
			var err error
			if tt.reuse {
				err = reportExistingArtifact(ctx, "job", &models.RootfsArtifact{
					ArtifactID: artifactID, Ext4Path: result.Ext4Path,
					Ext4SHA256: result.SHA256, Ext4SizeBytes: result.SizeBytes,
					StorageBackend: "s3", ObjectKey: "templates/" + artifactID + ".ext4",
				}, "fingerprint", source, reporter, nil, "", true, client, log.G(ctx))
			} else {
				err = runBuildLocked(ctx, "job", &types.CreateTemplateFromImageReq{}, artifactID,
					"fingerprint", source, reporter, nil, "", nil, nil, true,
					client != nil, client, log.G(ctx))
			}
			wantError := tt.uploadFails || tt.reportFails
			if (err != nil) != wantError {
				t.Fatalf("build error = %v, want error = %v", err, wantError)
			}
			if tt.uploadFails && !strings.Contains(err.Error(), "upload artifact to s3 store") {
				t.Fatalf("upload error lost: %v", err)
			}
			if _, err := os.Stat(neighbor); err != nil {
				t.Fatalf("another artifact was removed: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if tt.backend == "s3" {
				wantPuts := 1
				if tt.reuse {
					wantPuts = 0
				}
				if puts != wantPuts || objectExists == tt.uploadFails {
					t.Errorf("puts=%d objectExists=%v", puts, objectExists)
				}
			}
			wantStatus := "BUILT"
			if tt.uploadFails {
				wantStatus = "FAILED"
			}
			terminal := 0
			for _, payload := range reports {
				status := payload["status"]
				if status != "BUILT" && status != "FAILED" {
					continue
				}
				terminal++
				if status != wantStatus {
					t.Errorf("terminal status = %v, want %s", status, wantStatus)
				}
				if status == "BUILT" {
					if payload["ext4_path"] != result.Ext4Path || payload["ext4_sha256"] != result.SHA256 || payload["ext4_size_bytes"] != float64(result.SizeBytes) {
						t.Errorf("artifact metadata changed: %+v", payload)
					}
					if tt.backend != "" && (payload["storage_backend"] != tt.backend || payload["object_key"] != "templates/"+artifactID+".ext4") {
						t.Errorf("object locator missing: %+v", payload)
					}
				}
			}
			if terminal == 0 {
				t.Fatal("no terminal callback")
			}
			if tt.backend == "fs" {
				if exists, err := client.Stat(ctx, artifactID); err != nil || !exists {
					t.Fatalf("fs object missing: exists=%v err=%v", exists, err)
				}
			}
		})
	}
}

func TestBuildRejectsInvalidS3Client(t *testing.T) {
	resetSharedS3(t)
	t.Cleanup(func() { resetSharedS3(t) })
	t.Setenv(tcconfig.EnvArtifactStoreBackend, "s3")
	setS3Env(t, "http://localhost/bucket", "artifacts", "test-key", "test-secret")
	var failed atomic.Bool
	master := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode callback: %v", err)
		}
		if payload["status"] == templatecenter.JobStatusFailed {
			failed.Store(true)
		}
	}))
	defer master.Close()
	t.Setenv("CUBE_MASTER_ADDR", master.URL)
	err := Build(context.Background(), "job", &types.CreateTemplateFromImageReq{}, master.URL, "", nil)
	if err == nil || !strings.Contains(err.Error(), "artifact store failed to open") || !failed.Load() {
		t.Fatalf("invalid S3 client did not fail the build: err=%v failed=%v", err, failed.Load())
	}
}
