# file-storage

Key/value storage interfaces for server-side [`File` objects](https://developer.mozilla.org/en-US/docs/Web/API/File). `file-storage` gives Remix apps one consistent API across local disk and cloud object storage backends.

## Features

- **Simple API** - Intuitive key/value API (like [Web Storage](https://developer.mozilla.org/en-US/docs/Web/API/Web_Storage_API), but for `File`s instead of strings)
- **Multiple Backends** - Built-in filesystem, memory, and S3-compatible object storage backends
- **Streaming Support** - Stream file content to and from storage
- **Metadata Preservation** - Preserves all `File` metadata including `file.name`, `file.type`, and `file.lastModified`

## Installation

```sh
npm i remix
```

## Usage

### File System

```ts
import { createFsFileStorage } from 'remix/file-storage/fs'

let storage = createFsFileStorage('./user/files')

let file = new File(['hello world'], 'hello.txt', { type: 'text/plain' })
let key = 'hello-key'

// Put the file in storage.
await storage.set(key, file)

// Then, sometime later...
let fileFromStorage = await storage.get(key)
// All of the original file's metadata is intact
fileFromStorage.name // 'hello.txt'
fileFromStorage.type // 'text/plain'

// To remove from storage
await storage.remove(key)
```

### S3

```ts
import { createS3FileStorage } from 'remix/file-storage/s3'

let storage = createS3FileStorage({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  bucket: 'my-app-uploads',
  region: 'us-east-1',
})

let file = new File(['hello world'], 'hello.txt', { type: 'text/plain' })
await storage.set('uploads/hello.txt', file)

let fileFromStorage = await storage.get('uploads/hello.txt')
await storage.remove('uploads/hello.txt')
```

For S3-compatible providers such as MinIO and LocalStack, set `endpoint` and `forcePathStyle: true`.

## Related Packages

- [`form-data-parser`](https://github.com/remix-run/remix/tree/main/packages/form-data-parser) - Pairs well with this library for storing `FileUpload` objects received in `multipart/form-data` requests
- [`lazy-file`](https://github.com/remix-run/remix/tree/main/packages/lazy-file) - The streaming `File` implementation used internally to stream files from storage

## License

See [LICENSE](https://github.com/remix-run/remix/blob/main/LICENSE)
