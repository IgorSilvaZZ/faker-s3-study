const S3rver = require("s3rver");

const serverS3 = new S3rver({
  port: 4000,
  address: "localhost",
  directory: "./tmp",
  silent: true,
  resetOnClose: true,
});

serverS3.run((req, res) => {
  console.log(res);
});
