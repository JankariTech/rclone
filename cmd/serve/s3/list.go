package s3

import (
	"path"
	"strings"

	"github.com/JankariTech/gofakes3"
	"github.com/rclone/rclone/vfs"
)

func (b *s3Backend) entryListR(vf *vfs.VFS, bucket, fdPath, name string, addPrefix bool, response *gofakes3.ObjectList) error {
	fp := path.Join(bucket, fdPath)

	dirEntries, err := getDirEntries(fp, vf)
	if err != nil {
		return err
	}

	for _, entry := range dirEntries {
		object := entry.Name()

		// workround for control-chars detect
		objectPath := path.Join(fdPath, object)

		if !strings.HasPrefix(object, name) {
			continue
		}

		if entry.IsDir() {
			if addPrefix {
				response.AddPrefix(gofakes3.URLEncode(objectPath))
				continue
			}
			err := b.entryListR(vf, bucket, path.Join(fdPath, object), "", false, response)
			if err != nil {
				return err
			}
		} else {
			item := &gofakes3.Content{
				Key:          gofakes3.URLEncode(objectPath),
				LastModified: gofakes3.NewContentTime(entry.ModTime()),
				ETag:         getFileHash(entry),
				Size:         entry.Size(),
				StorageClass: gofakes3.StorageStandard,
			}
			response.Add(item)
		}
	}
	return nil
}
