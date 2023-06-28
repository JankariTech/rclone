# S3 to webDAV Proxy

**This project is still under development**

## Purpose
Allow accessing WebDAV backends through the S3 protocol.

As a developer of an app that is able to talk to S3 backends

I want to access WebDAV servers through the S3 protocol

So that I don't have to implement and maintain two protocols in my code

## How it works

This project is based on [rclone](https://github.com/rclone/rclone) (and the plan is to bring the changes back to rclone).
Rclone is a powerful tool to manage files on cloud storages. [One feature is to serve a particular storage over a given protocol](https://rclone.org/commands/rclone_serve/).

[One of the protocols rclone can serve is S3](https://github.com/rclone/rclone/pull/7062) (the feature did not make it yet into the release).
The shortcoming of that feature is that only one WebDAV authentication can match one S3 authentication. E.g. a WebDAV username + password combination can be set and all S3 requests will be forwarded to that specific WebDAV user.

For our use-case we need to access data of different WebDAV users. The solution is to use the S3 access key and forward it as a Bearer token to WebDAV.
1. The application that wants to talk to WebDAV using the S3 protocol has to obtain oauth tokens (or any other valid bearer tokens) for every user from the WebDAV server.
2. It puts the acquired token as the S3 access key into the S3 request.
3. rclone (acting as the S3 to WebDAV proxy) takes the S3 access key of every request and uses it as Bearer token to authenticate against the WebDAV server. (The S3 secrets will be ignored in the current solution.)

Through this solution the application at the very end only has to implement the S3 protocol, but can also access data that is stored on WebDAV servers on a user to user basis.

## How to use it

### 1. Start a WebDAV server
So far we have tested it with [ownCloud](https://github.com/owncloud/core/) and [Nextcloud](https://github.com/nextcloud/server).

### 2. Run the S3 to WebDAV proxy

```bash
docker run --rm \
-p 8080:8080 \
-e REMOTE_NAME=nc \
-e REMOTE_URL="https://<domain>/remote.php/webdav/" \
-e REMOTE_VENDOR=nextcloud \
--name s3-webdav-proxy ghcr.io/jankaritech/s3-webdav-proxy
```

If you want to use a WebDAV server running on `localhost` add `--network=host`

#### Configuration for Remote Server (WebDAV)

- **REMOTE_NAME**: Identifier for the remote server.
- **REMOTE_URL**: WebDAV URL of the remote server. Example: `https://<domain>/remote.php/webdav/`.
- **REMOTE_VENDOR**: Vendor of the remote server. Tested vendors are `nextcloud` and `owncloud`.

#### Configuration for the S3 to WebDAV proxy

- **PROXY_ARGS**: A space separated list of arguments to pass to rclone.

  Example: `-e PROXY_ARGS="--vfs-cache-mode=full --vfs-read-chunk-size=100M"`.

  Some useful options are:

  - `--vfs-cache-max-age`: Max time since last access of objects in the cache _(default 1h0m0s)_.
  - `--vfs-cache-max-size`: Max total size of objects in the cache _(default off)_.
  - `--vfs-cache-mode` Cache mode off|minimal|writes|full _(default off)_.
  - `--vfs-cache-poll-interval`: Interval to poll the cache for stale objects _(default 1m0s)_.
  - `--vfs-case-insensitive`: If a file name not found, find a case insensitive match.
  - `--vfs-disk-space-total-size`: Specify the total space of disk _(default off)_.
  - `--vfs-fast-fingerprint`: Use fast (less accurate) fingerprints for change detection.
  - `--vfs-read-ahead`: Extra read ahead over `--buffer-size` when using cache-mode full.
  - `--vfs-read-chunk-size`: Read the source objects in chunks _(default 128Mi)_.
  - `--vfs-read-chunk-size-limit`: If greater than `--vfs-read-chunk-size`, double the chunk size after each chunk read, until the limit is reached ('off' is unlimited) _(default off)_.
  - `--vfs-read-wait`: Time to wait for in-sequence read before seeking _(default 20ms)_.
  - `--vfs-used-is-size`: rclone size Use the rclone size algorithm for Used size.
  - `--vfs-write-back`: Time to writeback files after last use when using cache _(default 5s)_.
  - `--vfs-write-wait`: Time to wait for in-sequence write before giving error _(default 1s)_.

### 3. Obtain a Bearer token from the WebDAV server
#### using oauth2 tokens
- install the oauth2 app in ownCloud/Nextcloud
- in ownCloud/Nextcloud create a new oauth2 client with the redirect URL `http://localhost:9876/`
- create a json file called `oauth.json` and this content
```
   {
  "installed": {
    "client_id": "<client-id-copied-from-oauth2-app>",
    "project_id": "focus-surfer-382910",
    "auth_uri": "<owncloud-server-root>/index.php/apps/oauth2/authorize",
    "token_uri": "<owncloud-server-root>/index.php/apps/oauth2/api/v1/token",
    "client_secret": "<client-secret-copied-from-oauth2-app>",
    "redirect_uris": [
      "http://localhost:9876"
    ]
  }
}
```
- download and install `oauth2l` https://github.com/google/oauth2l#pre-compiled-binaries
- get an oauth2 token `./oauth2l fetch --credentials ./oauth.json --scope all --refresh --output_format bare`

#### using app-passwords
In Nextcloud one can also use the app-passwords as Bearer tokens. For that navigate in the WebUI to Personal Settings -> Security and generate a new app password. This app password is simply your token. (This does not work with OwnCloud)

### 4. access WebDAV using S3:
every root folder becomes a bucket, every file below it is a key

#### curl
- list all buckets (root folders): `curl --location 'http://localhost:8080/' -H'Authorization: AWS4-HMAC-SHA256 Credential=<oauth-access-token-or-app-password>/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-date,Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024'`
  replace `<oauth-access-token-or-app-password>` with your token, that you got with `oauth2l` or the app password, the rest of the header has to exist, but the content doesn't matter currently
- list keys in a bucket (any file below the root folder): `curl --location 'http://localhost:8080/<bucket>?list-type=2' -H'Authorization: AWS4-HMAC-SHA256 Credential=<oauth-access-token-or-app-password>/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-date,Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024'`

#### MinIO client
- install [mc](https://min.io/docs/minio/linux/reference/minio-mc.html)
- add the proxy as alias: `mc alias set mys3proxy http://localhost:8080 <oauth-access-token> anysecretkeyitdoesnotmatter`
  replace `<oauth-access-token-or-app-password>` with your token, that you got with `oauth2l`
- list buckets: `mc ls mys3proxy`
- list items in a bucket `mc ls mys3proxy/folder`
- upload a file: `mc cp <localfile> mys3proxy/<bucket (root folder)>/<keyname>`
- find files `mc find proxy/<bucket>/ --name "*.txt"`
- download all content of a bucket: `mc cp --recursive mys3proxy/<bucket> /tmp/dst/` (items that have a space in their name seem to have an issue)

#### MinIO Go Client SDK
```
minioClient, err := minio.New(testURL.Host, &minio.Options{
    Creds:  credentials.NewStaticV4(
        <oauth-access-token-or-app-password>,
        "does-not-matter-will-be-ignored-by-server",
        "",
    ),
    Secure: false,
})
assert.NoError(t, err)
buckets, err := minioClient.ListBuckets(context.Background())
```

#### Any other S3 library or tool
They all should "just" work :pray:
