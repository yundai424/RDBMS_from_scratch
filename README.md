# RDBMS_from_scratch

A full-functioning relational database management system that supports various types of queries, consisting of following components:
* A basic record-oriented file system including page-based and record(row)-based file manager that provides interfaces for CRUD operations.
* A relation manager that provides system catalogs and table management building on top of the file system.
* A B+ tree index manager that provides APIs for table indexing, including index creation, entry insertion/deletion/update, and scanning. Boosted performance by ~2X by implementing an in-memory LFU cache to reduce disk I/O.
* A query engine that provides APIs for query operations including filter, projection, group-by aggregation and join (including Block Nested Loop Join, Index Nested Loop Join, and Grace Hash Join).
