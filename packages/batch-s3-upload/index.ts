import "dotenv/config";
import { HeadBucketCommand, HeadObjectCommand, S3 } from "@aws-sdk/client-s3";
import path from "path";
import { promises as fs, createReadStream } from "fs";

const [inputDirectory, bucketName, bucketPath] = process.argv.slice(2);
if (!inputDirectory || !bucketName || !bucketPath) {
  console.error("usage:   cmd <inputDirectory> <bucketName> <bucketPath>");
  console.error("example: cmd /path/to/files bucket /foo");
  process.exit(1);
}

const readDir = async (dir: string): Promise<string[]> => {
  const files = await fs.readdir(dir);

  const recursed = await Promise.all(
    files.map(async (file) => {
      const resolved = path.resolve(path.join(dir, file));
      const stats = await fs.stat(resolved);
      if (stats.isDirectory()) {
        return readDir(resolved);
      }

      return [resolved];
    })
  );

  return recursed.flat();
};

const headBucket = (client: S3, bucketName: string, log = false) =>
  client
    .send(new HeadBucketCommand({ Bucket: bucketName }))
    .then(() => true)
    .catch((error) => {
      log && console.error(error);
      return false;
    });

const headObject = (client: S3, bucketName: string, key: string, log = false) =>
  client
    .send(new HeadObjectCommand({ Bucket: bucketName, Key: key }))
    .then(() => true)
    .catch((error) => {
      log && console.error(error);
      return false;
    });

const run = async () => {
  const directory = path.resolve(inputDirectory);
  console.log("ingesting from path:", directory);

  const files = await readDir(directory);
  console.log("total found files:", files.length);

  console.log("pushing to bucket:", `${bucketName}:${bucketPath}`);

  const client = new S3({
    endpoint: process.env.AWS_S3_ENDPOINT,
    tls: false,
    forcePathStyle: true,
    credentials: {
      accessKeyId: process.env.AWS_S3_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_S3_SECRET_ACCESS_KEY,
    },
  });

  console.log(
    "head bucket:",
    (await headBucket(client, bucketName, true)) ? "success" : "false"
  );

  const progress = Math.ceil(files.length / 10);
  await files.reduce(async (prev, file, index) => {
    await prev;
    if (index % progress === 0) {
      console.log("progress:", `${Math.floor((index * 100) / files.length)}%`);
    }

    const key = path.join(bucketPath, path.basename(file));
    if (await headObject(client, bucketName, key)) {
      return;
    }

    await client.putObject({
      Bucket: bucketName,
      Key: key,
      Body: createReadStream(file),
    });
  }, Promise.resolve());

  console.log("progress: 100%");
};

run();
