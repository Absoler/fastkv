#pragma once

#include "kvstore_api.h"

#include <queue>
#include <map>
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
#define BLOCK_SIZE 4096

/* ******************** my utils ******************** */
extern bool useBloomFilter;
extern bool useCache;
extern size_t maxSSEntry;

enum KEY_STATE {LIVE, DEAD, NOT_FOUND};
u64 loadu(u8 *bytes, int n);
void storeu(u8 *bytes, u64 value, int n);

bool in_intervals(const std::vector<std::pair<u64, u64>> &intervals,  std::pair<u64, u64> range);
struct SSTableHead;
struct SSEntry;
bool compare_ssentry(const SSEntry &sent1, const SSEntry &sent2);
int readSSTheader(const std::string &filename, SSTableHead &head);
int getsstfiles(const std::string &dir, int level, std::list<std::string> &sstfiles);
bool existLevelDir(const std::string &dir, int level);
bool selectolds(std::list<std::string> &sstfiles, int level);
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
    MemEntry *search_noless(u64 key);
    KEY_STATE search (u64 key, MemEntry * &ment);
    MemEntry *add(u64 key);
    int randomLv();
    KEY_STATE erase(u64 key);
    bool full();
};

class VLog;

struct SSTableHead {
    u64 timestamp;
    u64 cnt;
    u64 minkey, maxkey;
};
class SSTable {
    SSEntry *curp;
    void updateKey(u64 key);
    void loadsents();
    public:
    SSTableHead head;
    unsigned char bloom[1<<13];
    std::string vlog_name;
    std::string filename;

    // this must be deleted in time
    // if you delete this, remember set it to `nullptr`
    u8 *data;
    u32 slen;
    int level;
    int setdata(std::vector<SSEntry> &sents, size_t start, size_t end);
    int save();
    SSEntry *get_content();
    SSEntry *get_next();
    bool full();

    SSTable(const std::string &filename, int level, bool loadsents);
    ~SSTable();

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

    VEntry(): key(-1), checksum(-1), value("") {}
    VEntry(u64 _key, int _vlen, const std::string &s)
        : key(_key), vlen(_vlen), value(s){}
    size_t size() const;
};
class VLog {
    public:
    friend class SSTable;
    const static u8 magic = 0xff;
    std::string filename;

    VLog(const std::string &name): filename(name) {}
    ~VLog();

    bool createfile();
    off_t get_start_off();
    u64 append(u64 key, const std::string &value); // return the offset
    int readvent(u64 offset, int fd, VEntry &vent);
    std::string get(u64 offset);    // get the value

};

class KVStore : public KVStoreAPI
{
	// You can add your implementation here
private:
public:
    MemTable *memtable;
    VLog *vlog;
    std::string vlog_name;

	KVStore(const std::string &dir, const std::string &vlog);

	~KVStore();

    int merge(std::list<SSTable *> &stabs, int tolevel);
    int compact();
    int check_compact();

	void put(uint64_t key, const std::string &s) override;

    SSEntry *find_sstable(u64 key);
    void find_sstable_range(u64 key1, u64 key2, std::vector<SSEntry> &sents, std::set<u64> &viskey);
    KEY_STATE get_sent(uint64_t key, SSEntry &sent);
	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;
};
