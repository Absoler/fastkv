#pragma once

#include "kvstore_api.h"

#include <string>
#include <vector>
#include <set>
#include <list>
#include <random>
#include <algorithm>

#define MAX_SKIPLIST_LEVEL 32
#define SKIPLIST_P 0.5
#define MAX_SSTABLE_SIZE (16 * 1024)
#define BLOOM_SIZE (8 * 1024)
#define u64 unsigned long int
#define u32 unsigned int
#define u16 unsigned short
#define u8 unsigned char

/* ******************** my utils ******************** */
u64 loadu(u8 *bytes, int n);
void storeu(u8 *bytes, u64 value, int n);

struct SSTableHead;
struct SSEntry;
bool compare_ssentry(const SSEntry &sent1, const SSEntry &sent2);
int readSSTheader(const std::string &filename, SSTableHead &head);
int getsstfiles(const std::string &dir, int level, std::vector<std::string> &sstfiles);
/* ******************** my utils ******************** */

struct SSEntry {
    u64 key;
    u64 offset;
    int vlen;       // == 0 means dead in sstable
};
class MemEntry {
public:
  SSEntry sent;
  bool dead;        // indicates wether dead in memory
  std::vector<MemEntry *> forward;
  MemEntry(u64 _key, u64 _offset, int _vlen, bool _dead = false, int maxLevel = MAX_SKIPLIST_LEVEL)
      : sent({_key, _offset, _vlen}), dead(_dead), forward(maxLevel, nullptr) {}
};

// implemented with skiplist
class MemTable {
public:
    friend class SSTable;
    std::mt19937 gen{std::random_device{}()};
    std::uniform_real_distribution<double> dis;
    MemEntry *head;
    int maxLevel;
    int size;

    MemTable();
    ~MemTable();
    MemEntry *search_less(u64 key);
    MemEntry *search (u64 key);
    MemEntry *search_live (u64 key);
    MemEntry *add(u64 key);
    int randomLv();
    bool erase(u64 key);
};

class VLog;

struct SSTableHead {
    u64 timestamp;
    u64 cnt;
    u64 minkey, maxkey;
};
class SSTable {
    void updateKey(u64 key);
    void loaddata();
    public:
    SSTableHead head;
    unsigned char bloom[1<<13];
    std::string vlog_name;
    std::string filename;

	bool useBloomFilter;
	int maxEntries;


    // this must be deleted in time
    // if you delete this, remember set it to `nullptr`
    u8 *data;

    SSTable(const std::string &filename);
    ~SSTable();

    void put(u64 key, const std::string &s);
    void scan(u64 key1, u64 key2, std::list<std::pair<u64, std::string>> &list);

    void convert(MemTable *memtable);
    SSEntry *find(u64 key);
};

class VEntry {
    public:
    u8  magic;
    u16 checksum;
    u64 key;
    int vlen;
    std::string value;

    VEntry(u64 _key, int _vlen, const std::string &s)
        : key(_key), vlen(_vlen), value(s){}
};
class VLog {
    public:
    friend class SSTable;
    const static u8 magic = 0xff;
    std::string filename;

    VLog(const std::string &name): filename(name) {}

    bool createfile();
    u64 append(u64 key, const std::string &value); // return the offset
    std::string get(u64 offset);    // get the value

};

class KVStore : public KVStoreAPI
{
	// You can add your implementation here
private:
public:
    MemTable *memtable;
    VLog *vlog;
    static u64 timestamp;
    std::string curdir;
    std::string vlog_name;

	KVStore(const std::string &dir, const std::string &vlog);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

    SSEntry *find_sstable(u64 key);
    void find_sstable_range(u64 key1, u64 key2, std::vector<SSEntry> &sents, std::set<u64> &viskey);
	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;
};
