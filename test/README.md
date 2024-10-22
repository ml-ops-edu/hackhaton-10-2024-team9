# TEST SUITE

## Description

Python-based unit test suite for Trino deployment.

### Prerequisite:

- A running trino instance with S3 catalogs (see [local deployment](../containers/README.md))
- A bash-compatible shell
- A Docker engine

### Run the test:

- In the test folder: `cd ./test`
- Execute the test script: `./test.sh`, this will:
  - Create and activate a python virtual environment _./trino_venv_ with the required dependencies (trino, boto3) in the folder _trino_venv_
  - Create a _config.ini_ file from _config.ini.template_ and using the S3 credentials from _../containers/minio.env_
    - If set, the following environment variables take precedence over the _config.ini_:
      - _TRINO_HOST_
      - _TRINO_PORT_
      - _AWS_S3_ACCESS_KEY_ID_
      - _AWS_S3_SECRET_ACCESS_KEY_
      - _AWS_S3_ENDPOINT_
      - _AWS_S3_BUCKET_
      - Also multiple test environment are configured in config.ini.template (e.g. local and external), set _TRINO_TEST_ENV_ to specify a target environment other than _default_
  - Create the test user and bucket if using a local minio S3 and the user or bucket does not exists
  - Run all the test cases, unless you specify individual test files (_test\_*.py_ files) on the command line, e.g. `./test.sh test_s3.py test_iceberg.py`
  - Clean up the S3 objects created by the test after each test completes

Notes:
- You can also manually create the user and bucket and manually delete the objects in the _s3://{bucket}/trino/data/unittest/_ path.
- Unfortunately, a nix shell is required for _test.sh_, see comments at end of script to run it inside docker containers.
- For ref, see python unittest [command line](https://docs.python.org/3/library/unittest.html#command-line-interface) documentation.

