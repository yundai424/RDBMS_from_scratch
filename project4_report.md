## 1. Basic information
- Team #: 12
- Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222p-winter20-team-12
- Student 1 UCI NetID: yund9
- Student 1 Name: Yun Dai
- Student 2 UCI NetID (if applicable):
- Student 2 Name (if applicable):

## 2. Catalog information about Index

##### Show your catalog information about an index (tables, columns).

In the RM catalog, I add another boolean field called  `have_index` to indicate whether a column in an table has a B+ tree index (hence there's also another record in `Catalog.catalog` that describes this field). Each attribute has at most one index, so if an attribute has an index, then we can directly map the attribute to the pre-formatted index file name: `tableName_attrName.idx`.


## 3. Block Nested Loop Join (If you have implemented this feature)
##### Describe how your block nested loop join works (especially, how you manage the given buffers.)

For each BNLJoin, there are four major variables:

* a memory buffer `l_buffer` with size of `4096 * numPages`, which is used for loading and keeping `numPages` pages of left table's records
* a `std::unordered_map<Key, std::vector<char *>>` hash table that maps each different value to corresponding vector of pointers pointing to records in left table (i.e. pointer pointing to `l_buffer`) that have such value in their join attribute.
* a memory buffer `r_buffer` with size of one page, which is used for temporarily saving the current loaded record in right table.
* a boolean flag `same_key` (initially set to false) and an iterator `std::vector<char *>::iterator same_key_iter` used for handling the case where multiple records in left table match with one record in right table.

When the BNLJoin is initialized, it will firstly fill up the `l_buffer` by iterativly dumping records returned by `leftIn->getNextTuple` (size of each record is over-estimated but fixed according to the attribute's length).

Each time `getNextTuple` method is called, it will firstly check `same_key` to see if it's going to use the same right record stored in `r_buffer` and iterate the `same_key_iter` to generate next result. If not, then it will load the next record in `rightIn` to `r_buffer`, parse the key and fetch the in-memory hashmap, and return a matched result. If there're multiple matching records on left table, the corresponding boolean flat and iterator will be set.

When `rightIn-getNextTuple` returns a `QE_EOF`, indicating that the right table has been totally scanned, we will load the succeeding left table records into `l_buffer`, then continue the same steps as above. The iteration will end when the left table has been totally scanned, and return a `QE_EOF`.


## 4. Index Nested Loop Join (If you have implemented this feature)



## 5. Grace Hash Join (If you have implemented this feature)
##### Describe how your grace hash join works (especially, in-memory structure).

For GHJoin, the most important in-memory structures are:

* two vector `left_fhs`, `right_fhs` storing the file handler of each partition for each table (`std::vector<std::shared_ptr<FileHandle>>`)
* a hash map that maps each unique key to vector of RIDs of records in current left table **partition** that have such key in the join attribute (`std::unordered_map<Key, std::vector<RID>>`)
* an `RBF_ScanIterator` for scanning current right table partition, and a memory buffer `r_buffer` same as in BNLJoin.
* an int `curr_index` specifying the current partition (initially set to -1)

When the GHJoin is initialized, it will firstly create `numPartitions` files, bind each file to the `FileHandle` in corresponding posision in the `left_fhs`, load records from left input iterator, parse the key, write the record into corresponding patition files. Then it will do the same things to the right input iterator. To avoid conflict partition name, I name each partition as `left/right_attrName_partitionX`.

The logics of `getNextTuple` is quite similar of that in BNLJoin. It will still firstly consider the duplication case. Then it fetches the next record from the RMSI of current partition of right table, parse key, fetch in-memory hashmap, determine if there're multiple matching, and load a corresponding left table record from partition file using stored RID, then return a result. 

If the RMSI of right partition reaches the end (including the case that it's not initialized when the very first time the `getNextTuple` is called), the GHJoin will increment the `curr_index` by one, load record and get RID using `rbfm->getNextRecord` on corresponding file handler `left_fhs[curr_index]`, and store results in the hashmap. Then it recursively call `getNextTuple` again to go back and fetch the next matching record.

The iteration will terminate when`curr_index` reaches `numPartitions`.

When the GHJoin object is destructed, it will close and destroy all partition files in `left_fhs` and `right_fhs`.

## 6. Aggregation

##### Describe how your aggregation (basic, group-based hash) works.

**Basic**

For basic aggregation, I keep two in-memory float variables `val` and `cnt`, referring to the current "value" (which could be min, max and sum) and the current count of records fetched. Each time a new record is fetched, the value on aggregation attribute will be parsed, the `cnt` will be incremented by one, and the `val` will be updated using value according to the `AggregateOp`. Specifically, it will be added up for  `SUM` and `AVG` , and will be updated by `std::max` for `MAX` and `std::min` for `MIN`.  The `getNextTuple` will return resulting value calculated using `val` and `cnt` w.r.t the `AggregateOp` for only once (I keep an in-memory boolean flag to indicate whether this method has been called once). The next time it will return a `QE_EOF`.

**Group-Based**

Group-based hash aggregation is quite homologous to basic aggregation, except that instead of keeping only two in-memory float variables, I keep a hashmap that maps each Key to a pair of floats (i.e. `std::unordered_map<Key, std::pair<float, float>>`). Each time a new record is fetched, I will firstly parse the key on `groupAttr`, probe into the hashmap, then update the corresponding `val` & `cnt` pair following the same logics as in basic aggregation.

When returning value, I keep an iterator of the hashmap, eath time fetching the next key-value pair and return a calculated result, until the iterator reaches the end.

## 7. Implementation Detail

##### Have you added your own source file (.cc or .h)?

No

##### Have you implemented any optional features? Then, describe them here.

I implemented Grace Hash Join and group-based hash aggregation.

##### Other implementation details:

For convenience purpose, I abstract attribute value as `Key` object, which has `AttrType` indicating its type, and `int`, `float`, `std::string` fields that store its real value, and hash method that basically hash based on the string representation of its real value.


## 6. Other (optional)
##### Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)

Finally built the whole database system bottom-up, which I would definitely not imagine when I just enrolled in this course and made the bold decision to do the projects solo. Project 4 is definitely the easiest one (at least not that nerve-wracking) but still much more than interesting and exciting. Thanks the nice professor and TAs for all the helps. Really learned a lot from 222!