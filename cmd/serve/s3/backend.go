// Package s3 implements an s3 server for rclone
package s3

import (
	"context"
	"encoding/hex"
	"io"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/JankariTech/gofakes3"
	"github.com/ncw/swift/v2"
	"github.com/rclone/rclone/backend/webdav"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfsflags"
)

var (
	emptyPrefix = &gofakes3.Prefix{}
	timeFormat  = "Mon, 2 Jan 2006 15:04:05.999999999 GMT"
)

// s3Backend implements the gofacess3.Backend interface to make an S3
// backend for gofakes3
type s3Backend struct {
	opt  *Options
	meta *sync.Map
	w    *Server
}

// newBackend creates a new SimpleBucketBackend.
func newBackend(opt *Options, w *Server) gofakes3.Backend {
	return &s3Backend{
		opt:  opt,
		w:    w,
		meta: new(sync.Map),
	}
}

func (b *s3Backend) setAuthForWebDAV(accessKey string) *vfs.VFS {
	// new VFS
	if _, ok := b.w.f.(*webdav.Fs); ok {
		info, name, remote, config, _ := fs.ConfigFs(b.w.f.Name() + ":")
		f, _ := info.NewFs(context.Background(), name+accessKey, remote, config)
		vf := vfs.New(f, &vfsflags.Opt)
		vf.Fs().(*webdav.Fs).SetBearerToken(accessKey)
		return vf
	}
	return vfs.New(b.w.f, &vfsflags.Opt)
}

// ListBuckets always returns the default bucket.
func (b *s3Backend) ListBuckets(accessKey string) ([]gofakes3.BucketInfo, error) {
	vf := b.setAuthForWebDAV(accessKey)

	dirEntries, err := getDirEntries("/", vf)
	if err != nil {
		return nil, err
	}
	var response []gofakes3.BucketInfo
	for _, entry := range dirEntries {
		if entry.IsDir() {
			response = append(response, gofakes3.BucketInfo{
				Name:         gofakes3.URLEncode(entry.Name()),
				CreationDate: gofakes3.NewContentTime(entry.ModTime()),
			})
		}
		// FIXME: handle files in root dir
	}

	return response, nil
}

// ListBucket lists the objects in the given bucket.
func (b *s3Backend) ListBucket(accessKey string, bucket string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	vf := b.setAuthForWebDAV(accessKey)

	_, err := vf.Stat(bucket)
	if err != nil {
		return nil, gofakes3.BucketNotFound(bucket)
	}
	if prefix == nil {
		prefix = emptyPrefix
	}

	// workaround
	if strings.TrimSpace(prefix.Prefix) == "" {
		prefix.HasPrefix = false
	}
	if strings.TrimSpace(prefix.Delimiter) == "" {
		prefix.HasDelimiter = false
	}

	response := gofakes3.NewObjectList()
	path, remaining := prefixParser(prefix)

	err = b.entryListR(vf, bucket, path, remaining, prefix.HasDelimiter, response)
	if err == gofakes3.ErrNoSuchKey {
		// AWS just returns an empty list
		response = gofakes3.NewObjectList()
	} else if err != nil {
		return nil, err
	}

	return b.pager(response, page)
}

// HeadObject returns the fileinfo for the given object name.
//
// Note that the metadata is not supported yet.
func (b *s3Backend) HeadObject(accessKey string, bucketName, objectName string) (*gofakes3.Object, error) {
	vf := b.setAuthForWebDAV(accessKey)

	_, err := vf.Stat(bucketName)
	if err != nil {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	fp := path.Join(bucketName, objectName)
	node, err := vf.Stat(fp)
	if err != nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	if !node.IsFile() {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	entry := node.DirEntry()
	if entry == nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	fobj := entry.(fs.Object)
	size := node.Size()
	hash := getFileHashByte(fobj)

	meta := map[string]string{
		"Last-Modified": node.ModTime().Format(timeFormat),
		"Content-Type":  fs.MimeType(context.Background(), fobj),
	}

	if val, ok := b.meta.Load(fp); ok {
		metaMap := val.(map[string]string)
		for k, v := range metaMap {
			meta[k] = v
		}
	}

	return &gofakes3.Object{
		Name:     objectName,
		Hash:     hash,
		Metadata: meta,
		Size:     size,
		Contents: noOpReadCloser{},
	}, nil
}

// GetObject fetchs the object from the filesystem.
func (b *s3Backend) GetObject(accessKey string, bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (obj *gofakes3.Object, err error) {
	vf := b.setAuthForWebDAV(accessKey)

	_, err = vf.Stat(bucketName)
	if err != nil {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	fp := path.Join(bucketName, objectName)
	node, err := vf.Stat(fp)
	if err != nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	if !node.IsFile() {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	entry := node.DirEntry()
	if entry == nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	fobj := entry.(fs.Object)
	file := node.(*vfs.File)

	size := node.Size()
	hash := getFileHashByte(fobj)

	in, err := file.Open(os.O_RDONLY)
	if err != nil {
		return nil, gofakes3.ErrInternal
	}
	defer func() {
		// If an error occurs, the caller may not have access to Object.Body in order to close it:
		if err != nil {
			_ = in.Close()
		}
	}()

	var rdr io.ReadCloser = in
	rnge, err := rangeRequest.Range(size)
	if err != nil {
		return nil, err
	}

	if rnge != nil {
		if _, err := in.Seek(rnge.Start, io.SeekStart); err != nil {
			return nil, err
		}
		rdr = limitReadCloser(rdr, in.Close, rnge.Length)
	}

	meta := map[string]string{
		"Last-Modified": node.ModTime().Format(timeFormat),
		"Content-Type":  fs.MimeType(context.Background(), fobj),
	}

	if val, ok := b.meta.Load(fp); ok {
		metaMap := val.(map[string]string)
		for k, v := range metaMap {
			meta[k] = v
		}
	}

	return &gofakes3.Object{
		Name:     gofakes3.URLEncode(objectName),
		Hash:     hash,
		Metadata: meta,
		Size:     size,
		Range:    rnge,
		Contents: rdr,
	}, nil
}

// TouchObject creates or updates meta on specified object.
func (b *s3Backend) TouchObject(accessKey string, fp string, meta map[string]string) (result gofakes3.PutObjectResult, err error) {
	vf := b.setAuthForWebDAV(accessKey)

	_, err = vf.Stat(fp)
	if err == vfs.ENOENT {
		f, err := vf.Create(fp)
		if err != nil {
			return result, err
		}
		_ = f.Close()
		return b.TouchObject(accessKey, fp, meta)
	} else if err != nil {
		return result, err
	}

	_, err = vf.Stat(fp)
	if err != nil {
		return result, err
	}

	b.meta.Store(fp, meta)

	if val, ok := meta["X-Amz-Meta-Mtime"]; ok {
		ti, err := swift.FloatStringToTime(val)
		if err == nil {
			return result, vf.Chtimes(fp, ti, ti)
		}
		// ignore error since the file is successfully created
	}

	if val, ok := meta["mtime"]; ok {
		ti, err := swift.FloatStringToTime(val)
		if err == nil {
			return result, vf.Chtimes(fp, ti, ti)
		}
		// ignore error since the file is successfully created
	}

	return result, nil
}

// PutObject creates or overwrites the object with the given name.
func (b *s3Backend) PutObject(
	accessKey string,
	bucketName, objectName string,
	meta map[string]string,
	input io.Reader, size int64,
) (result gofakes3.PutObjectResult, err error) {
	vf := b.setAuthForWebDAV(accessKey)

	_, err = vf.Stat(bucketName)
	if err != nil {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	fp := path.Join(bucketName, objectName)
	objectDir := path.Dir(fp)
	// _, err = vf.Stat(objectDir)
	// if err == vfs.ENOENT {
	// 	fs.Errorf(objectDir, "PutObject failed: path not found")
	// 	return result, gofakes3.KeyNotFound(objectName)
	// }

	if objectDir != "." {
		if err := mkdirRecursive(objectDir, vf); err != nil {
			return result, err
		}
	}

	f, err := vf.Create(fp)
	if err != nil {
		return result, err
	}

	if _, err := io.Copy(f, input); err != nil {
		// remove file when i/o error occurred (FsPutErr)
		_ = f.Close()
		_ = vf.Remove(fp)
		return result, err
	}

	if err := f.Close(); err != nil {
		// remove file when close error occurred (FsPutErr)
		_ = vf.Remove(fp)
		return result, err
	}

	_, err = vf.Stat(fp)
	if err != nil {
		return result, err
	}

	b.meta.Store(fp, meta)

	if val, ok := meta["X-Amz-Meta-Mtime"]; ok {
		ti, err := swift.FloatStringToTime(val)
		if err == nil {
			return result, vf.Chtimes(fp, ti, ti)
		}
		// ignore error since the file is successfully created
	}

	if val, ok := meta["mtime"]; ok {
		ti, err := swift.FloatStringToTime(val)
		if err == nil {
			return result, vf.Chtimes(fp, ti, ti)
		}
		// ignore error since the file is successfully created
	}

	return result, nil
}

// DeleteMulti deletes multiple objects in a single request.
func (b *s3Backend) DeleteMulti(accessKey string, bucketName string, objects ...string) (result gofakes3.MultiDeleteResult, rerr error) {
	vf := b.setAuthForWebDAV(accessKey)

	for _, object := range objects {
		if err := b.deleteObject(vf, bucketName, object); err != nil {
			fs.Errorf("serve s3", "delete object failed: %v", err)
			result.Error = append(result.Error, gofakes3.ErrorResult{
				Code:    gofakes3.ErrInternal,
				Message: gofakes3.ErrInternal.Message(),
				Key:     object,
			})
		} else {
			result.Deleted = append(result.Deleted, gofakes3.ObjectID{
				Key: object,
			})
		}
	}

	return result, nil
}

// DeleteObject deletes the object with the given name.
func (b *s3Backend) DeleteObject(accessKey string, bucketName, objectName string) (result gofakes3.ObjectDeleteResult, rerr error) {
	vf := b.setAuthForWebDAV(accessKey)

	return result, b.deleteObject(vf, bucketName, objectName)
}

// deleteObject deletes the object from the filesystem.
func (b *s3Backend) deleteObject(vf *vfs.VFS, bucketName, objectName string) error {
	_, err := vf.Stat(bucketName)
	if err != nil {
		return gofakes3.BucketNotFound(bucketName)
	}

	fp := path.Join(bucketName, objectName)
	// S3 does not report an error when attemping to delete a key that does not exist, so
	// we need to skip IsNotExist errors.
	if err := vf.Remove(fp); err != nil && !os.IsNotExist(err) {
		return err
	}

	// FIXME: unsafe operation
	rmdirRecursive(fp, vf)
	return nil
}

// CreateBucket creates a new bucket.
func (b *s3Backend) CreateBucket(accessKey string, name string) error {
	vf := b.setAuthForWebDAV(accessKey)

	_, err := vf.Stat(name)
	if err != nil && err != vfs.ENOENT {
		return gofakes3.ErrInternal
	}

	if err == nil {
		return gofakes3.ErrBucketAlreadyExists
	}

	if err := vf.Mkdir(name, 0755); err != nil {
		return gofakes3.ErrInternal
	}
	return nil
}

// DeleteBucket deletes the bucket with the given name.
func (b *s3Backend) DeleteBucket(accessKey string, name string) error {
	vf := b.setAuthForWebDAV(accessKey)

	_, err := vf.Stat(name)
	if err != nil {
		return gofakes3.BucketNotFound(name)
	}

	if err := vf.Remove(name); err != nil {
		return gofakes3.ErrBucketNotEmpty
	}

	return nil
}

// BucketExists checks if the bucket exists.
func (b *s3Backend) BucketExists(accessKey string, name string) (exists bool, err error) {
	vf := b.setAuthForWebDAV(accessKey)

	_, err = vf.Stat(name)
	if err != nil {
		return false, nil
	}

	return true, nil
}

// CopyObject copy specified object from srcKey to dstKey.
func (b *s3Backend) CopyObject(accessKey string, srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (result gofakes3.CopyObjectResult, err error) {
	vf := b.setAuthForWebDAV(accessKey)

	fp := path.Join(srcBucket, srcKey)
	if srcBucket == dstBucket && srcKey == dstKey {
		b.meta.Store(fp, meta)

		val, ok := meta["X-Amz-Meta-Mtime"]
		if !ok {
			if val, ok = meta["mtime"]; !ok {
				return
			}
		}
		// update modtime
		ti, err := swift.FloatStringToTime(val)
		if err != nil {
			return result, nil
		}

		return result, vf.Chtimes(fp, ti, ti)
	}

	cStat, err := vf.Stat(fp)
	if err != nil {
		return
	}

	c, err := b.GetObject(accessKey, srcBucket, srcKey, nil)
	if err != nil {
		return
	}
	defer func() {
		_ = c.Contents.Close()
	}()

	for k, v := range c.Metadata {
		if _, found := meta[k]; !found && k != "X-Amz-Acl" {
			meta[k] = v
		}
	}
	if _, ok := meta["mtime"]; !ok {
		meta["mtime"] = swift.TimeToFloatString(cStat.ModTime())
	}

	_, err = b.PutObject(accessKey, dstBucket, dstKey, meta, c.Contents, c.Size)
	if err != nil {
		return
	}

	return gofakes3.CopyObjectResult{
		ETag:         `"` + hex.EncodeToString(c.Hash) + `"`,
		LastModified: gofakes3.NewContentTime(cStat.ModTime()),
	}, nil
}
