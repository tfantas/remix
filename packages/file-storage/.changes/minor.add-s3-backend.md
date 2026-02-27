Add an S3-backed file storage implementation with `createS3FileStorage` in `@remix-run/file-storage/s3`.

This backend uses `aws4fetch` for SigV4 request signing, supports AWS S3 and S3-compatible APIs,
and preserves file metadata (`name`, `type`, and `lastModified`) across storage operations.
