# Simple Database

This database was made exclusively for use
by [my full-text search engine](https://github.com/econaxis/search#new-improved-storage-engine). There are more details
on this database at the link.

This README only contains information on column-oriented data format and compression. Other information related to the
database can be found at the link above.

## Compressing data

Let's say we wanted to store millions of telemetry data points, in the form of
tuples `(int timestamp, float data1, float data2, float data3)`

The simplest way to store a bunch of fixed-size data points like this is as a packed array. We store the data
contiguously in memory as `(int, float, float, float)(int, float, float, float)(int, float, float, ...)`. Because the
elements are fixed size, accessing the "timestamp" of the nth tuple is easy.

If we imagine data1, data2, and data3 as values that are closely related, then we'd also expect that data1 fields of all
tuples will be "close together." If data1 represented air temperature in Celsius, then we'd expect most data1 fields to
be close to 20.0 to 30.0. These bunched up values mean we are not using the full 32 bit "information-carrying capacity,"
or "entropy" of a float. Basically, we're wasting bit space. This is great for compression, as a good compression
algorithm can exploit this fact, and represent the same data in less bits.

However, this is hard because of how the data is layed out. Instead of having values like `20.1, 20.0, 20.5, 25.1, ...`,
we actually
have `(timestamp, 20.1, data2, data3), (timestamp, 20.0, data2, data3), (timestamp, 20.5, data2, data3),... `.

The "bunched up" data1's are not close together. They're lying in between a bunch of other unrelated data. This worsens
the compression ratio.

The simplest solution would be "shuffling" the data into a columnar format. In other words, we take the "transpose" of
the table.

Instead of storing arrays of tuples as arrays of tuples, we
do `(timestamp, timestamp, timestamp, ...)(data1, data1, data1, ...)(data2, data2, data2, ...)(data3, data3, data3, ...)`
.

Thus, all timestamps are stored together, all data1's are stored together, and so on. If you're a C programmer, this
would be like

```c
struct Telemetry {
int timestamp;
float data1, data2, data3;
};
struct NormalFormat {
struct Telemetry data[];
};
struct ColumnarFormat {
int timestamps[];
float data1s[], data2s[], data3s[];
};
```

This enables extremely efficient compression, as the compression algorithm can work with blocks of very similar data.

In this database, however, I do it a bit differently. Instead of shuffling each element in the tuple, I shuffle each
byte. This is a more extreme version of the "transpose" done above. The first byte of all the structs are stored
contiguously, then the second byte, and so on. This result results in many long sequences of 0's, which can be
compressed easily.

All of what I've said is implemented in `src/compressor.rs`
