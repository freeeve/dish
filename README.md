dish: disk-based hash
====

Scala [currently fixed-size, configurable] mutable hashmap[string,long], mmapped and stored on disk in many files (2GB per file). probably really only useful for SSD, because it's going to cause a lot of page faults.

So far, testing on my macbook pro indicates that this hash table can get pretty fast speeds while it can fit in RAM (300k+ puts and gets/sec), and when it doesn't fit in RAM--when it's more than double RAM size--it slows significantly to something like 1k puts and gets/sec). I haven't tested only gets, which is my end use case, once it gets too big for memory.
