# SDSC Hackhaton 2024 - Team 9

**Processing Large Scale Multidimensional S3 Data on Distributed SQL Engines**

[Full description](https://sdsc-hackathons.ch/projectPage?projectRef=vUt8BfDJXaAs0UfOesXI|k7GpeFFCLbNxAv60mNra)

Starter Pack.

## Abstract

Traditional formats like HDF5 and NetCDF, which were designed for storing high-dimensional data such as earth science or genomic datasets, were not originally optimized for cloud storage environments. As a result, they often encounter significant performance issues when accessed in such settings. To address this challenge, we propose implementing methods to query highly dimensional data stored in Zarr or similar formats on S3. Furthermore, by leveraging TrinoDB’s distributed SQL engine, we enable scalable and efficient data processing. This initiative aims to enhance the accessibility, usability, and sustainability of high-dimensional research datasets using open-source tools and community-driven follow-up improvements. The project will focus on developing a flexible, high-performance Proof of Concept infrastructure that can serve as a blueprint for similar Research, Development, and Innovation (RDI) initiatives.

## What do you want to build?

Through this hackathon, we provide you with the platform to explore one of the following topics:

### 1. Zarr over NetCDF Data Format Support for Trino

Implement a TrinoDB connector along with a set of User Defined Functions (UDFs) to facilitate the querying of read-only multidimensional data stored in Zarr (NetCDF4, HDF5 backend) on S3 [1,2,3]. The connector can either embed an existing or your own library to handle the multidimensional data directly or act as a client to external frameworks capable of processing such data, such as Dask, Vaex, or Spark. Two types of operations are expected: (1) cube selection, which involves selecting a subset (slice) or the entire set of dimensions within a specified range from the large dataset stored on S3, and (2) slice arithmetic, which perform operations on the selected slice, such as calculating the differences between two slices or averaging values along a specific dimension of a slice. A successful Proof of Concept (PoC) should demonstrate one implementation of each operation type.

The following is an example of a fictional query language to be developed, provided for illustrative purposes only to demonstrate how the language can be extended to support the two types of operations mentioned above. In this example, a multidimensional data slice is retrieved from the "climateDb.SurfaceTemperature" table using the to-be-developed Zarr connector, and the slice (of VARBINARY type) is averaged along the time dimension using the zarr_average UDF:

```
WITH ClimateChunk AS (
    SELECT chunk FROM climate WHERE
        time        IN('2024-10-01', '2024-10-31' )
        AND lon  IN(6.022609, 10.442701)
        AND lat  IN(45.776947, 47.830827)
    FROM Zarr.ClimateDB.SurfaceTemperatures
)
SELECT zarr_average(chunk, 'time') FROM ClimateChunk;
```

### 2. Federated Queries

_This is a backup task, or if time permit_

Often, we need to analyze data stored across multiple locations and managed by different entities. To address this, we propose demonstrating a deployment that extends Trino with data access control methods and Single Sign-On (SSO) capabilities to operate across trust boundaries. This would enable a TrinoDB instance to securely query tables on other TrinoDB instances located in different administrative domains while ensuring consistent access control. Additionally, if feasible, we should leverage push-down optimizations to delegate portions of the queries to the remote servers [4,5].

## What data will you be working with?

We will be working primarily with Zarr v2 or v3 over NetCDF4 format. One such data is the _NOAA High-Resolution Rapid Refresh (HRRR) Model_
available from AWS open data registry at [https://registry.opendata.aws/noaa-hrrr-pds/](https://registry.opendata.aws/noaa-hrrr-pds/) (the Zarr format near-real time data archive managed by the University of Utah).

## Prerequesite

* Docker or docker-compatible container runtime
    - [Docker Engine](https://docs.docker.com/engine/install/)
    - [Colima](https://github.com/abiosoft/colima)
    - [Podman](https://podman.io/docs/installation) - with podman-compose
    - [Orbstack](https://orbstack.dev/download)
    - Others

## Useful links

### References

1. [Zarr specifications v3](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html)
2. [Zarr specifications v2](https://zarr-specs.readthedocs.io/en/latest/v2/v2.0.html)
3. [Trino catalog developer guide](https://trino.io/docs/current/develop.html)
4. [Trino on Trino](https://github.com/trinodb/trino/issues/21791)
5. [Trino Gateway](https://trinodb.github.io/trino-gateway/)


### Communication channels and help line
6. [Trino Forum (Slack)](https://trino.io/slack.html)
7. [Team's Discord channel](https://discord.com/channels/1290958531507257394/1293930869941338154)

### Useful links
8. [Catalog REST API (Iceberg OpenAPI)](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)

