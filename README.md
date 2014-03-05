dish: disk-based hash
====

scala mutable hashmap[string,long], mmapped and stored on disk in many files (2GB per file). probably really only useful for SSD, because it's going to cause a lot of page faults.
