// Package s3 implements a fake s3 server for rclone
package s3

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"

	"github.com/Mikubill/gofakes3"
	"github.com/Mikubill/gofakes3/signature"
	"github.com/go-chi/chi/v5"
	"github.com/rclone/rclone/cmd/serve/proxy"
	"github.com/rclone/rclone/cmd/serve/proxy/proxyflags"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	httplib "github.com/rclone/rclone/lib/http"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfsflags"
)

type ctxKey int

const (
	ctxKeyId ctxKey = iota
)

// Options contains options for the http Server
type Options struct {
	//TODO add more options
	pathBucketMode bool
	hashName       string
	hashType       hash.Type
	authPair       []string
	noCleanup      bool
	Auth           httplib.AuthConfig
	HTTP           httplib.Config
	proxyMode      bool
}

// Server is a s3.FileSystem interface
type Server struct {
	*httplib.Server
	opt     Options
	f       fs.Fs
	_vfs    *vfs.VFS // don't use directly, use getVFS
	faker   *gofakes3.GoFakeS3
	handler http.Handler
	proxy   *proxy.Proxy
	ctx     context.Context // for global config
}

// Make a new S3 Server to serve the remote
func newServer(ctx context.Context, f fs.Fs, opt *Options) (s *Server, err error) {
	w := &Server{
		f:   f,
		ctx: ctx,
		opt: *opt,
	}

	var newLogger logger
	w.faker = gofakes3.New(
		newBackend(w, opt),
		gofakes3.WithHostBucket(!opt.pathBucketMode),
		gofakes3.WithLogger(newLogger),
		gofakes3.WithRequestID(rand.Uint64()),
		gofakes3.WithoutVersioning(),
		gofakes3.WithV4Auth(authlistResolver(opt.authPair)),
		gofakes3.WithIntegrityCheck(true), // Check Content-MD5 if supplied
	)

	w.handler = http.NewServeMux()
	w.handler = w.faker.Server()

	if proxyflags.Opt.AuthProxy != "" {
		fmt.Println("proxy mode...")
		w.proxy = proxy.New(ctx, &proxyflags.Opt)
		// override auth
		// w.opt.Auth.CustomAuthFn = w.auth
		w.handler = middlewareAuthCustom(w.handler, w)
	} else {
		w._vfs = vfs.New(f, &vfsflags.Opt)
	}

	if len(opt.authPair) == 0 {
		fs.Logf("serve s3", "No auth provided so allowing anonymous access")
	}

	w.Server, err = httplib.NewServer(ctx,
		httplib.WithConfig(w.opt.HTTP),
		httplib.WithAuth(w.opt.Auth),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to init server: %w", err)
	}


	// if opt.proxyMode {
	// 	w.handler = proxyMiddleware(w.handler, w)
	// } else {
	// 	w._vfs = vfs.New(f, &vfsflags.Opt)
	// }

	return w, nil
}

// func (w *Server) getVFS(ctx context.Context) (VFS *vfs.VFS, err error) {
// 	if w._vfs != nil {
// 		return w._vfs, nil
// 	}
// 	value := ctx.Value(ctxKeyId)
// 	if value == nil {
// 		return nil, errors.New("no VFS found in context")
// 	}
// 	VFS, ok := value.(*vfs.VFS)
// 	if !ok {
// 		return nil, fmt.Errorf("context value is not VFS: %#v", value)
// 	}
// 	return VFS, nil
// }

// Gets the VFS in use for this request
func (w *Server) getVFS(ctx context.Context) (VFS *vfs.VFS, err error) {
	if w._vfs != nil {
		return w._vfs, nil
	}
	value := httplib.CtxGetAuth(ctx)
	if value == nil {
		return nil, errors.New("no VFS found in context")
	}
	VFS, ok := value.(*vfs.VFS)
	if !ok {
		return nil, fmt.Errorf("context value is not VFS: %#v", value)
	}
	return VFS, nil
}


// auth does proxy authorization
func (w *Server) auth(accessKeyId string) (value interface{}, err error) {
	fmt.Println("auth called...")
	VFS, _, err := w.proxy.Call(accessKeyId, "", false)
	if err != nil {
		return nil, err
	}
	return VFS, err
}

// Bind register the handler to http.Router
func (w *Server) Bind(router chi.Router) {
	router.Handle("/*", w.handler)
}

func (w *Server) serve() error {
	w.Serve()
	fs.Logf(w.f, "Starting s3 server on %s", w.URLs())
	return nil
}

func middlewareAuthCustom(next http.Handler, ws *Server) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		accessKey, _ := parseAuthToken(r)
		value, err := ws.auth(accessKey)
		if err != nil {
			fs.Infof(r.URL.Path, "%s: Auth failed: %v", r.RemoteAddr, err)
		}

		if value != nil {
			r = r.WithContext(context.WithValue(r.Context(), ctxKeyId, value))
		}
		// info, name, remote, config, _ := fs.ConfigFs(ws.f.Name() + ":")
		// newFs, _ := info.NewFs(r.Context(), name+stringToMd5Hash(accessKey), remote, config)
		// _vfs := vfs.New(newFs, &vfsflags.Opt)
		// _vfs.Fs().(*webdav.Fs).SetBearerToken(accessKey)

		// ctx := context.WithValue(r.Context(), ctxKeyId, _vfs)
		next.ServeHTTP(w, r)
	})
}

func parseAuthToken(r *http.Request) (accessKey string, error signature.ErrorCode) {
	v4Auth := r.Header.Get("Authorization")
	req, err := signature.ParseSignV4(v4Auth)
	if err != signature.ErrNone {
		return "", err
	}

	return req.Credential.GetAccessKey(), signature.ErrNone
}

// func stringToMd5Hash(s string) string {
// 	hasher := md5.New()
// 	hasher.Write([]byte(s))
// 	return hex.EncodeToString(hasher.Sum(nil))
// }
